"""S3オーケストレーションパイプライン"""

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ProgressCallback:
    """1GB毎に進捗をログ出力するコールバック"""

    def __init__(self, filename: str, total_size: int, update_interval: int = 1024 * 1024 * 1024):
        """
        Args:
            filename: ファイル名
            total_size: 総ファイルサイズ(バイト)
            update_interval: 更新間隔(バイト数)、デフォルトは1GB
        """
        self.filename = filename
        self.total_size = total_size
        self.update_interval = update_interval
        self.accumulated = 0
        self.transferred = 0

    def __call__(self, bytes_transferred: int) -> None:
        """転送されたバイト数を蓄積し、1GB毎にログ出力"""
        self.accumulated += bytes_transferred
        self.transferred += bytes_transferred

        if self.accumulated >= self.update_interval:
            # 1GB単位でログ出力
            progress_gb = self.transferred / (1024 * 1024 * 1024)
            total_gb = self.total_size / (1024 * 1024 * 1024)
            percentage = (self.transferred / self.total_size * 100) if self.total_size > 0 else 0
            logger.info(f"{self.filename}: {progress_gb:.2f}GB / {total_gb:.2f}GB ({percentage:.1f}%)")
            self.accumulated %= self.update_interval

    def flush(self) -> None:
        """最終的な進捗をログ出力(転送完了時に呼び出す)"""
        if self.transferred > 0:
            progress_gb = self.transferred / (1024 * 1024 * 1024)
            total_gb = self.total_size / (1024 * 1024 * 1024)
            percentage = (self.transferred / self.total_size * 100) if self.total_size > 0 else 0
            logger.info(f"{self.filename}: {progress_gb:.2f}GB / {total_gb:.2f}GB ({percentage:.1f}%)")


class S3Pipeline:
    """S3との連携を管理するクラス"""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.executor = ThreadPoolExecutor(max_workers=2)

    def download_file(
        self,
        filename: str,
        local_path: Path,
        show_progress: bool = True
    ) -> None:
        """S3からファイルをダウンロード"""
        if local_path.exists():
            logger.info(f"[DL] スキップ: {filename} は既に存在します")
            return

        s3_key = f"input/{filename}"
        logger.info(f"[DL] ダウンロード開始: {filename}")

        try:
            # ファイルサイズを取得
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=s3_key
            )
            file_size = response['ContentLength']

            # プログレスコールバック付きでダウンロード
            if show_progress:
                callback = ProgressCallback(f"[DL] {filename}", file_size)
                self.s3_client.download_file(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Filename=str(local_path),
                    Callback=callback
                )
                callback.flush()  # 残りの進捗をログ出力
            else:
                self.s3_client.download_file(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Filename=str(local_path)
                )

            logger.info(f"[DL] ダウンロード完了: {filename}")

        except ClientError as e:
            logger.error(f"ダウンロードに失敗: {filename} - {e}")
            raise

    def download_file_async(
        self,
        filename: str,
        local_path: Path
    ) -> Future[None]:
        """バックグラウンドでダウンロードを開始"""
        logger.info(f"[DL] 次ファイルのダウンロード開始: {filename}")
        return self.executor.submit(
            self.download_file,
            filename,
            local_path,
            show_progress=True
        )

    def upload_file(
        self,
        local_path: Path,
        base_name: str,
        show_progress: bool = True
    ) -> None:
        """S3へファイルをアップロード"""
        s3_key = f"output/{base_name}"
        logger.info(f"アップロード中: {base_name}")

        try:
            file_size = local_path.stat().st_size

            # プログレスコールバック付きでアップロード
            if show_progress:
                callback = ProgressCallback(f"[UP] {base_name}", file_size)
                self.s3_client.upload_file(
                    Filename=str(local_path),
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    Callback=callback
                )
                callback.flush()  # 残りの進捗をログ出力
            else:
                self.s3_client.upload_file(
                    Filename=str(local_path),
                    Bucket=self.bucket_name,
                    Key=s3_key
                )

            logger.info(f"アップロード完了: {base_name}")

        except ClientError as e:
            logger.error(f"アップロードに失敗: {base_name} - {e}")
            raise

    def upload_file_async(
        self,
        local_path: Path,
        base_name: str
    ) -> Future[None]:
        """バックグラウンドでアップロードを開始"""
        logger.info(f"バックグラウンドアップロード開始: {base_name}")
        return self.executor.submit(
            self.upload_file,
            local_path,
            base_name,
            show_progress=True
        )

    def shutdown(self) -> None:
        """リソースのクリーンアップ"""
        logger.info("[S3] ExecutorをShutdown中...")
        self.executor.shutdown(wait=True)
        logger.info("[S3] Shutdown完了")
