"""エンコード処理オーケストレーター"""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List

from .config import EncodingConfig, SegmentInfo, WorkspaceConfig
from .ffmpeg import FFmpegService
from .storage import S3Service


class EncodingOrchestrator:
    """エンコード処理全体を統括するクラス"""

    def __init__(
        self,
        config: EncodingConfig,
        workspace_config: WorkspaceConfig,
        logger: logging.Logger
    ):
        """
        Args:
            config: エンコード設定
            workspace_config: ワークスペース設定
            logger: ロガーインスタンス
        """
        self.config = config
        self.workspace = workspace_config
        self.logger = logger
        self.ffmpeg = FFmpegService(logger)
        self.s3 = S3Service(logger)

    def run(self) -> None:
        """エンコード処理を実行"""
        # ステップ0: 入力ファイル取得
        s3_input_path = f"s3://{self.config.s3_bucket}/input/{self.config.input_filename}"
        self.s3.download(s3_input_path, self.workspace.local_input_file)

        # ステップ1: 分割・エンコード
        self._encode_segments()

        # ステップ2: 結合
        self._concat_segments()

        # ステップ3: S3アップロード
        self._upload_to_s3()

    def _encode_segments(self) -> None:
        """セグメント分割・エンコード"""
        self.logger.info("=" * 50)
        self.logger.info("ステップ1: セグメント分割・エンコード開始")
        self.logger.info("=" * 50)

        # 動画の長さを取得
        duration = self.ffmpeg.get_duration(self.workspace.local_input_file)

        # セグメント数を計算（切り上げ）
        num_segments = int((duration + self.config.segment_length - 1) // self.config.segment_length)
        self.logger.info(f"セグメント数: {num_segments} (各{self.config.segment_length}秒)")

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
                segment_idx, success, message = future.result()
                self.logger.info(message)

                if not success:
                    failed_segments.append(segment_idx)

        # 結果確認
        if failed_segments:
            self.logger.error(f"失敗したセグメント: {failed_segments}")
            raise RuntimeError(f"{len(failed_segments)}個のセグメントでエラーが発生")

        # セグメント結果チェック
        self._check_segment_results(num_segments)

        self.logger.info("ステップ1: セグメントエンコード完了")

    def _check_segment_results(self, num_segments: int) -> None:
        """セグメント結果を検証"""
        self.logger.info("セグメント結果確認中...")

        missing_segments = []
        for i in range(num_segments):
            segment_file = self.workspace.segments_dir / f"segment_{i:04d}.mp4"
            if not segment_file.exists():
                missing_segments.append(i)

        if missing_segments:
            self.logger.error(f"作成されていないセグメント: {missing_segments}")
            raise RuntimeError("一部のセグメントが作成されていません")

        # 合計サイズを表示
        total_size = sum(
            f.stat().st_size
            for f in self.workspace.segments_dir.glob("segment_*.mp4")
        )
        self.logger.info(f"セグメント合計サイズ: {total_size / 1024 / 1024:.2f} MB")

    def _concat_segments(self) -> None:
        """セグメント結合"""
        self.logger.info("=" * 50)
        self.logger.info("ステップ2: セグメント結合開始")
        self.logger.info("=" * 50)

        # セグメントファイルをリストアップ
        segment_files = sorted(self.workspace.segments_dir.glob("segment_*.mp4"))

        if not segment_files:
            self.logger.error("セグメントファイルが見つかりません")
            raise RuntimeError("セグメントファイルが見つかりません")

        self.logger.info(f"結合するセグメント数: {len(segment_files)}")

        # ビデオのみを結合
        video_temp = self.workspace.work_dir / "video_only.mp4"
        self.logger.info("ビデオストリーム結合中...")

        self.ffmpeg.concat_segments(
            segment_files,
            self.workspace.concat_file,
            video_temp
        )

        # 音声処理
        if self.ffmpeg.has_audio_stream(self.workspace.local_input_file):
            self.logger.info("音声トラック検出 - 抽出して結合します")
            self._merge_audio_video(video_temp)
        else:
            self.logger.info("音声トラックなし - ビデオのみを出力")
            video_temp.rename(self.workspace.local_output_file)

        self.logger.info("ステップ2: 結合処理完了")

    def _merge_audio_video(self, video_temp: Path) -> None:
        """音声とビデオを結合"""
        audio_file = self.workspace.work_dir / "audio_extracted.m4a"

        # 音声を抽出
        self.logger.info("音声トラック抽出中...")
        self.ffmpeg.extract_audio(self.workspace.local_input_file, audio_file)

        # ビデオと音声を多重化
        self.logger.info("ビデオと音声を多重化中...")
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
        """S3へアップロード"""
        self.logger.info("=" * 50)
        self.logger.info("ステップ3: S3アップロード開始")
        self.logger.info("=" * 50)

        s3_output_path = f"s3://{self.config.s3_bucket}/output/{self.workspace.work_dir.name}/"
        self.s3.sync(
            self.workspace.work_dir,
            s3_output_path,
            "作業ディレクトリ（ログ含む）"
        )

        self.logger.info("ステップ3: S3アップロード完了")
        self.logger.info(f"出力先: {s3_output_path}")
