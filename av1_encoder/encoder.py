import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List

from .config import EncodingConfig, SegmentInfo, WorkspaceConfig
from .ffmpeg import FFmpegService
from .storage import S3Service


class EncodingOrchestrator:
    def __init__(
        self,
        config: EncodingConfig,
        workspace_config: WorkspaceConfig,
        logger: logging.Logger
    ):
        self.config = config
        self.workspace = workspace_config
        self.logger = logger
        self.ffmpeg = FFmpegService()
        self.s3 = S3Service()

    def run(self) -> None:
        self.s3.download(
            self.config.s3_bucket,
            f"input/{self.config.input_filename}",
            self.workspace.local_input_file
        )
        self._encode_segments()
        self._concat_segments()
        self._upload_to_s3()

    def _encode_segments(self) -> None:
        self.logger.info("セグメント分割・エンコード開始")

        # 動画の長さを取得
        duration = self.ffmpeg.get_duration(self.workspace.local_input_file)

        # セグメント数を計算（切り上げ）
        num_segments = int((duration + self.config.segment_length - 1) // self.config.segment_length)
        self.logger.info(f"セグメント数: {num_segments}")

        # セグメント情報リストを作成
        segments: List[SegmentInfo] = []
        for i in range(num_segments):
            start_time = i * self.config.segment_length
            segments.append(SegmentInfo(
                index=i,
                start_time=start_time,
                duration=self.config.segment_length,
                total_duration=duration
            ))

        # 並列エンコード実行
        self.logger.info(f"エンコード開始 (並列数: {self.config.parallel_jobs})")

        failed_segments = []

        with ProcessPoolExecutor(max_workers=self.config.parallel_jobs) as executor:
            # 全セグメントを投入
            futures = {
                executor.submit(
                    self.ffmpeg.encode_segment,
                    seg,
                    self.workspace.local_input_file,
                    self.workspace.segments_dir,
                    self.workspace.logs_dir,
                    self.config
                ): seg.index for seg in segments
            }

            # 完了したものから処理
            for future in as_completed(futures):
                success = future.result()
                segment_idx = futures[future]

                if success:
                    self.logger.info(f"完了: セグメント {segment_idx:04d}")
                else:
                    self.logger.error(f"失敗: セグメント {segment_idx:04d}")
                    failed_segments.append(segment_idx)

        # 結果確認
        if failed_segments:
            raise RuntimeError(f"{len(failed_segments)}個のセグメントでエラーが発生")

        # セグメント結果チェック
        self._check_segment_results(num_segments)

        self.logger.info("セグメントエンコード完了")

    def _check_segment_results(self, num_segments: int) -> None:
        missing_segments = []
        for i in range(num_segments):
            segment_file = self.workspace.segments_dir / f"segment_{i:04d}.mp4"
            if not segment_file.exists():
                missing_segments.append(i)

        if missing_segments:
            raise RuntimeError(f"セグメントが作成されていません: {','.join(map(str, missing_segments))}")

        # 合計サイズを表示
        total_size = sum(
            f.stat().st_size
            for f in self.workspace.segments_dir.glob("segment_*.mp4")
        )
        self.logger.info(f"セグメント合計サイズ: {total_size / 1024 / 1024:.2f} MB")

    def _concat_segments(self) -> None:
        self.logger.info("セグメント結合開始")

        # セグメントファイルをリストアップ
        segment_files = sorted(self.workspace.segments_dir.glob("segment_*.mp4"))

        # ビデオのみを結合
        video_temp = self.workspace.work_dir / "video_only.mp4"

        self.ffmpeg.concat_segments(
            segment_files,
            self.workspace.concat_file,
            video_temp
        )

        # 音声処理
        self._merge_audio_video(video_temp)

        self.logger.info("ステップ2: 結合処理完了")

    def _merge_audio_video(self, video_temp: Path) -> None:
        audio_file = self.workspace.work_dir / "audio_extracted.m4a"

        # 音声を抽出
        self.ffmpeg.extract_audio(self.workspace.local_input_file, audio_file)

        # ビデオと音声を多重化
        self.ffmpeg.merge_video_audio(
            video_temp,
            audio_file,
            self.workspace.local_output_file
        )

        # 一時ファイル削除
        video_temp.unlink(missing_ok=True)
        audio_file.unlink(missing_ok=True)
        self.logger.info("音声トラック結合完了")

    def _upload_to_s3(self) -> None:
        self.logger.info("S3アップロード開始")

        output_key = f"output/{self.workspace.work_dir.name}"
        self.s3.upload_directory(
            self.workspace.work_dir,
            self.config.s3_bucket,
            output_key
        )

        self.logger.info("S3アップロード完了")
