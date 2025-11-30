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
    """S3との連携を管理するクラス

    コンテキストマネージャとして使用することで、リソースの自動クリーンアップが保証される。

    使用例:
        with S3Pipeline() as s3:
            s3.download_file('my-bucket', 'input/file.mkv', Path('file.mkv'))
            s3.upload_file(Path('output.mkv'), 'my-bucket', 'output/file.mkv')
    """

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.executor = ThreadPoolExecutor(max_workers=2)

    def __enter__(self) -> 'S3Pipeline':
        """コンテキストマネージャのエントリ"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """コンテキストマネージャのクリーンアップ"""
        self.shutdown()

    def download_file(
        self,
        bucket: str,
        key: str,
        local_path: Path,
        show_progress: bool = True
    ) -> None:
        """S3からファイルをダウンロード

        Args:
            bucket: S3バケット名
            key: S3オブジェクトキー
            local_path: ローカル保存先パス
            show_progress: 進捗表示の有無
        """
        filename = Path(key).name

        if local_path.exists():
            logger.info(f"[DL] スキップ: {filename} は既に存在します")
            return

        logger.info(f"[DL] ダウンロード開始: {filename}")

        try:
            # ファイルサイズを取得
            response = self.s3_client.head_object(
                Bucket=bucket,
                Key=key
            )
            file_size = response['ContentLength']

            # プログレスコールバック付きでダウンロード
            if show_progress:
                callback = ProgressCallback(f"[DL] {filename}", file_size)
                self.s3_client.download_file(
                    Bucket=bucket,
                    Key=key,
                    Filename=str(local_path),
                    Callback=callback
                )
                callback.flush()  # 残りの進捗をログ出力
            else:
                self.s3_client.download_file(
                    Bucket=bucket,
                    Key=key,
                    Filename=str(local_path)
                )

            logger.info(f"[DL] ダウンロード完了: {filename}")

        except ClientError as e:
            logger.error(f"ダウンロードに失敗: {filename} - {e}")
            raise

    def download_file_async(
        self,
        bucket: str,
        key: str,
        local_path: Path
    ) -> Future[None]:
        """バックグラウンドでダウンロードを開始

        Args:
            bucket: S3バケット名
            key: S3オブジェクトキー
            local_path: ローカル保存先パス

        Returns:
            Future オブジェクト
        """
        filename = Path(key).name
        logger.info(f"[DL] 次ファイルのダウンロード開始: {filename}")
        return self.executor.submit(
            self.download_file,
            bucket,
            key,
            local_path,
            show_progress=True
        )

    def upload_file(
        self,
        local_path: Path,
        bucket: str,
        key: str,
        show_progress: bool = True
    ) -> None:
        """S3へファイルをアップロード

        Args:
            local_path: ローカルファイルパス
            bucket: S3バケット名
            key: S3オブジェクトキー
            show_progress: 進捗表示の有無
        """
        filename = Path(key).name
        logger.info(f"アップロード中: {filename}")

        try:
            file_size = local_path.stat().st_size

            # プログレスコールバック付きでアップロード
            if show_progress:
                callback = ProgressCallback(f"[UP] {filename}", file_size)
                self.s3_client.upload_file(
                    Filename=str(local_path),
                    Bucket=bucket,
                    Key=key,
                    Callback=callback
                )
                callback.flush()  # 残りの進捗をログ出力
            else:
                self.s3_client.upload_file(
                    Filename=str(local_path),
                    Bucket=bucket,
                    Key=key
                )

            logger.info(f"アップロード完了: {filename}")

        except ClientError as e:
            logger.error(f"アップロードに失敗: {filename} - {e}")
            raise

    def upload_file_async(
        self,
        local_path: Path,
        bucket: str,
        key: str
    ) -> Future[None]:
        """バックグラウンドでアップロードを開始

        Args:
            local_path: ローカルファイルパス
            bucket: S3バケット名
            key: S3オブジェクトキー

        Returns:
            Future オブジェクト
        """
        filename = Path(key).name
        logger.info(f"バックグラウンドアップロード開始: {filename}")
        return self.executor.submit(
            self.upload_file,
            local_path,
            bucket,
            key,
            show_progress=True
        )

    def shutdown(self) -> None:
        """リソースのクリーンアップ"""
        logger.info("[S3] ExecutorをShutdown中...")
        self.executor.shutdown(wait=True)
        logger.info("[S3] Shutdown完了")
