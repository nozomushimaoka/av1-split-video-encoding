import logging
import os
import signal
import sys
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List
from datetime import datetime

from .config import EncodingConfig
from .workspace import make_workspace
from .ffmpeg import FFmpegService, SegmentInfo


def _worker_init():
    """ワーカープロセスの初期化: シグナルハンドラをデフォルトに戻す"""
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


class EncodingOrchestrator:
    def __init__(
        self,
        config: EncodingConfig
    ):
        self.config = config
        self.start_time = datetime.now()
        self.workspace = make_workspace(config.input_file, self.start_time)
        # TODO: 本当はinit内でファイルシステム操作をしたくない
        self.workspace.prepare_directory()
        self.logger = self._init_logger(self.workspace.log_file)
        self.ffmpeg = FFmpegService()
        self._main_pid = os.getpid()  # メインプロセスのPIDを記録

    def run(self) -> None:
        # シグナルハンドラを設定
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        try:
            self._print_header()
            self._encode_segments()
            self._concat_segments()
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

    def _print_header(self) -> None:
        self.logger.info(f"作業ディレクトリ: {self.workspace.work_dir}")
        self.logger.info(f"並列ジョブ数: {self.config.parallel_jobs}")
        if self.config.extra_args:
            self.logger.info(f"追加FFmpegオプション: {' '.join(self.config.extra_args)}")

    def _print_completion(self) -> None:
        end_time = datetime.now()
        elapsed = end_time - self.start_time

        self.logger.info("全処理完了")
        self.logger.info(f"処理時間: {elapsed}")

    def _encode_segments(self) -> None:
        self.logger.info("分割エンコードを開始")

        # セグメント情報リストを作成
        segments = self._list_segments()

        # 並列エンコード実行
        self.logger.debug(f"エンコード開始 並列数: {self.config.parallel_jobs}")

        total_count = len(segments)
        count = 0
        failed = 0

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
                    self.logger.info(f"完了: {segment_idx} ({count}/{total_count})")
                else:
                    failed += 1
                    self.logger.error(f"失敗: {segment_idx}")

            # 結果確認
            if failed > 0:
                raise RuntimeError(f"{failed}個のセグメントでエラーが発生")

            self.logger.info("セグメントエンコード完了")
        except KeyboardInterrupt:
            executor.shutdown(wait=False, cancel_futures=True)
            raise
        finally:
            executor.shutdown(wait=True)

    def _concat_segments(self) -> None:
        self.logger.info("セグメント結合開始")

        # セグメントファイルをリストアップ
        segment_files = sorted(self.workspace.segments_dir.glob("segment_*.mp4"))

        self.ffmpeg.concat_segments(
            segment_files,
            self.config.input_file,
            self.workspace.concat_file,
            self.workspace.output_file
        )

        self.logger.info("結合処理完了")

        # 結合後、segmentsディレクトリを削除
        self.logger.info("セグメントファイルを削除中")
        shutil.rmtree(self.workspace.segments_dir)

    def _list_segments(self) -> List[SegmentInfo]:
        num_segments = self._calc_num_segments()
        segments: List[SegmentInfo] = []
        for i in range(num_segments):
            start_time = i * self.config.segment_length
            is_final = (i == num_segments - 1)
            segments.append(SegmentInfo(
                index=i,
                start_time=start_time,
                duration=self.config.segment_length,
                is_final=is_final,
                file=self.workspace.segments_dir / f"segment_{i:04d}.mp4",
                log_file=self.workspace.logs_dir / f"segment_{i:04d}.log"
            ))
        return segments

    def _calc_num_segments(self) -> int:
        duration = self.ffmpeg.get_duration(self.config.input_file)
        return int((duration + self.config.segment_length - 1) // self.config.segment_length)

    def _init_logger(self, log_file: Path) -> logging.Logger:
        logger = logging.getLogger("av1_encoder")
        logger.setLevel(logging.INFO)

        # 既存のハンドラをクリア
        logger.handlers.clear()

        # ファイルハンドラ
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)

        # コンソールハンドラ
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # フォーマッター
        formatter = logging.Formatter(
            '[%(asctime)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

