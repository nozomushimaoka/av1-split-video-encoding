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

    def _encode_segments(self) -> None:
        # セグメント情報リストを作成
        segments = self._list_segments()

        # 総フレーム数とFPSを取得
        total_frames = self.video_probe.get_total_frames(self.config.input_file)
        fps = self.video_probe.get_fps(self.config.input_file)

        total_count = len(segments)

        # エンコード開始前の情報表示
        self.logger.info("=== エンコード情報 ===")
        self.logger.info(f"入力: {self.config.input_file.name}")
        self.logger.info(f"総フレーム数: {total_frames}")
        self.logger.info(f"セグメント数: {total_count}")
        self.logger.info(f"並列数: {self.config.parallel_jobs}")

        # セグメントあたりのフレーム数を計算
        frames_per_segment = total_frames / total_count

        count = 0
        encode_start_time = time.time()

        executor = ProcessPoolExecutor(
            max_workers=self.config.parallel_jobs,
            initializer=_worker_init
        )

        try:
            # 全セグメントを投入
            futures = {
                executor.submit(
                    self.ffmpeg.encode_segment,
                    seg,
                    self.config.input_file,
                    self.config
                ): seg.index for seg in segments
            }

            # 完了したものから処理
            for future in as_completed(futures):
                success = future.result()
                segment_idx = futures[future]

                count += 1

                if success:
                    # 進捗情報を計算
                    elapsed = time.time() - encode_start_time
                    completed_frames = int(count * frames_per_segment)
                    speed_fps = completed_frames / elapsed if elapsed > 0 else 0
                    remaining_frames = total_frames - completed_frames
                    remaining_seconds = remaining_frames / speed_fps if speed_fps > 0 else 0

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

