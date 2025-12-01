import logging
import os
import signal
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import List

from ..core.config import EncodingConfig
from ..core.ffmpeg import FFmpegService, SegmentInfo
from ..core.logging_config import setup_file_and_console_logger
from ..core.video_probe import VideoProbe
from ..core.workspace import make_workspace_from_path


def _worker_init():
    """ワーカープロセスの初期化: シグナルハンドラをデフォルトに戻す"""
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def _format_time(seconds: float) -> str:
    """秒数をhh:mm:ss形式にフォーマットする"""
    seconds = int(seconds)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class EncodingOrchestrator:
    def __init__(
        self,
        config: EncodingConfig,
        log_level: int = logging.INFO
    ):
        self.config = config
        self.start_time = datetime.now()
        self.workspace = make_workspace_from_path(config.workspace_dir)
        self.logger = setup_file_and_console_logger(
            "av1_encoder",
            self.workspace.log_file,
            level=log_level
        )
        self.ffmpeg = FFmpegService()
        self.video_probe = VideoProbe()
        self._main_pid = os.getpid()  # メインプロセスのPIDを記録

    def run(self) -> None:
        # シグナルハンドラを設定
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            self._encode_segments()
            self._generate_concat_file()
            self._print_completion()
        except KeyboardInterrupt:
            self.logger.error("処理が中断されました")
            sys.exit(130)  # SIGINT の標準終了コード
        except Exception:
            self.logger.exception("エラー")
            raise

    def _signal_handler(self, signum: int, frame) -> None:
        """シグナル受信時の処理（メインプロセスのみ）"""
        # メインプロセス以外では何もしない
        if os.getpid() != self._main_pid:
            return

        self.logger.warning("中断シグナルを受信しました。クリーンアップ中...")
        raise KeyboardInterrupt()

    def _print_completion(self) -> None:
        end_time = datetime.now()
        elapsed = end_time - self.start_time

        self.logger.info(f"終了 処理時間: {elapsed}")

    def _load_completed_segments(self) -> set[int]:
        """完了済みセグメント番号を読み込む"""
        completed_file = self.workspace.work_dir / "completed.txt"
        if not completed_file.exists():
            return set()
        with open(completed_file, 'r', encoding='utf-8') as f:
            return {int(line.strip()) for line in f if line.strip()}

    def _mark_segment_completed(self, segment_index: int) -> None:
        """セグメントを完了としてマーク"""
        completed_file = self.workspace.work_dir / "completed.txt"
        with open(completed_file, 'a', encoding='utf-8') as f:
            f.write(f"{segment_index}\n")

    def _encode_segments(self) -> None:
        # セグメント情報リストを作成
        segments = self._list_segments()

        # 完了済みセグメントを読み込み
        completed_indices = self._load_completed_segments()
        pending_segments = [seg for seg in segments if seg.index not in completed_indices]
        completed_count = len(completed_indices)

        if completed_count > 0:
            self.logger.info(f"スキップ: {completed_count}セグメント (処理済み)")

        if not pending_segments:
            self.logger.info("すべてのセグメントが処理済みです")
            return

        # 総フレーム数を取得
        total_frames = self.video_probe.get_total_frames(self.config.input_file)

        total_count = len(segments)
        pending_count = len(pending_segments)

        # エンコード開始前の情報表示
        self.logger.info("=== エンコード情報 ===")
        self.logger.info(f"入力: {self.config.input_file.name}")
        self.logger.info(f"総フレーム数: {total_frames}")
        self.logger.info(f"セグメント数: {total_count} (未処理: {pending_count})")
        self.logger.info(f"並列数: {self.config.parallel_jobs}")

        # セグメントあたりのフレーム数を計算
        frames_per_segment = total_frames / total_count

        count = completed_count  # 完了済みセグメント数から開始
        encode_start_time = time.time()

        executor = ProcessPoolExecutor(
            max_workers=self.config.parallel_jobs,
            initializer=_worker_init
        )

        try:
            # 未処理セグメントのみを投入
            futures = {
                executor.submit(
                    self.ffmpeg.encode_segment,
                    seg,
                    self.config.input_file,
                    self.config
                ): seg.index for seg in pending_segments
            }

            # 完了したものから処理
            for future in as_completed(futures):
                success = future.result()
                segment_idx = futures[future]

                count += 1

                if success:
                    # 完了をマーク
                    self._mark_segment_completed(segment_idx)

                    # 進捗情報を計算
                    elapsed = time.time() - encode_start_time
                    completed_in_session = count - completed_count
                    speed_fps = (completed_in_session * frames_per_segment) / elapsed if elapsed > 0 else 0
                    remaining_segments = total_count - count
                    remaining_seconds = (remaining_segments * frames_per_segment) / speed_fps if speed_fps > 0 else 0

                    self.logger.info(
                        f"完了: {segment_idx} ({count}/{total_count}) "
                        f"速度: {speed_fps:.1f}fps 残り: {_format_time(remaining_seconds)}"
                    )
                else:
                    raise RuntimeError(f"セグメント{segment_idx}のエンコードに失敗")

            self.logger.info("エンコード完了")
        except KeyboardInterrupt:
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            executor.shutdown(wait=True)

    def _generate_concat_file(self) -> None:
        self.logger.info("concat.txt生成開始")

        # セグメントファイルをリストアップ
        segment_files = sorted(self.workspace.work_dir.glob("segment_*.ivf"))

        # concat.txtを生成
        concat_txt_content = [f"file '{segment_file.resolve()}'\n" for segment_file in segment_files]
        with open(self.workspace.concat_file, 'w', encoding='utf-8') as f:
            f.writelines(concat_txt_content)

        self.logger.info(f"concat.txt生成完了: {self.workspace.concat_file}")

    def _list_segments(self) -> List[SegmentInfo]:
        num_segments = self._calc_num_segments()

        # フレームレートとGOPサイズを取得
        fps = self.ffmpeg.get_fps(self.config.input_file)
        gop_size = self.config.gop_size

        # GOP整列されたセグメント長を計算
        gop_duration = gop_size / fps
        num_gops = round(self.config.segment_length / gop_duration)
        segment_duration = num_gops * gop_duration

        segments: List[SegmentInfo] = []
        for i in range(num_segments):
            start_time = i * segment_duration
            is_final = (i == num_segments - 1)
            segments.append(SegmentInfo(
                index=i,
                start_time=start_time,
                duration=segment_duration,
                is_final=is_final,
                file=self.workspace.work_dir / f"segment_{i:04d}.ivf",
                log_file=self.workspace.work_dir / f"segment_{i:04d}.log"
            ))
        return segments

    def _calc_num_segments(self) -> int:
        duration = self.ffmpeg.get_duration(self.config.input_file)
        return int((duration + self.config.segment_length - 1) // self.config.segment_length)

