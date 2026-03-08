"""S3 orchestration pipeline"""

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class ProgressCallback:
    """Callback that logs transfer progress every 1 GB"""

    def __init__(self, filename: str, total_size: int, update_interval: int = 1024 * 1024 * 1024):
        """
        Args:
            filename: File name
            total_size: Total file size in bytes
            update_interval: Logging interval in bytes (default: 1 GB)
        """
        self.filename = filename
        self.total_size = total_size
        self.update_interval = update_interval
        self.accumulated = 0
        self.transferred = 0

    def __call__(self, bytes_transferred: int) -> None:
        """Accumulate transferred bytes and log every 1 GB"""
        self.accumulated += bytes_transferred
        self.transferred += bytes_transferred

        if self.accumulated >= self.update_interval:
            # Log in 1 GB increments
            progress_gb = self.transferred / (1024 * 1024 * 1024)
            total_gb = self.total_size / (1024 * 1024 * 1024)
            percentage = (self.transferred / self.total_size * 100) if self.total_size > 0 else 0
            logger.info(f"{self.filename}: {progress_gb:.2f}GB / {total_gb:.2f}GB ({percentage:.1f}%)")
            self.accumulated %= self.update_interval

    def flush(self) -> None:
        """Log final progress (call when transfer is complete)"""
        if self.transferred > 0:
            progress_gb = self.transferred / (1024 * 1024 * 1024)
            total_gb = self.total_size / (1024 * 1024 * 1024)
            percentage = (self.transferred / self.total_size * 100) if self.total_size > 0 else 0
            logger.info(f"{self.filename}: {progress_gb:.2f}GB / {total_gb:.2f}GB ({percentage:.1f}%)")


class S3Pipeline:
    """Manages S3 transfers

    Use as a context manager to ensure automatic resource cleanup.

    Example:
        with S3Pipeline() as s3:
            s3.download_file('my-bucket', 'input/file.mkv', Path('file.mkv'))
            s3.upload_file(Path('output.mkv'), 'my-bucket', 'output/file.mkv')
    """

    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.executor = ThreadPoolExecutor(max_workers=2)

    def __enter__(self) -> 'S3Pipeline':
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager cleanup"""
        self.shutdown()

    def download_file(
        self,
        bucket: str,
        key: str,
        local_path: Path,
        show_progress: bool = True
    ) -> None:
        """Download a file from S3

        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Local destination path
            show_progress: Whether to log progress
        """
        filename = Path(key).name

        if local_path.exists():
            logger.info(f"[DL] Skipping {filename}: already exists")
            return

        logger.info(f"[DL] Starting download: {filename}")

        try:
            # Get file size
            response = self.s3_client.head_object(
                Bucket=bucket,
                Key=key
            )
            file_size = response['ContentLength']

            # Download with progress callback
            if show_progress:
                callback = ProgressCallback(f"[DL] {filename}", file_size)
                self.s3_client.download_file(
                    Bucket=bucket,
                    Key=key,
                    Filename=str(local_path),
                    Callback=callback
                )
                callback.flush()  # Log remaining progress
            else:
                self.s3_client.download_file(
                    Bucket=bucket,
                    Key=key,
                    Filename=str(local_path)
                )

            logger.info(f"[DL] Download complete: {filename}")

        except ClientError as e:
            logger.error(f"Download failed: {filename} - {e}")
            raise

    def download_file_async(
        self,
        bucket: str,
        key: str,
        local_path: Path
    ) -> Future[None]:
        """Start a download in the background

        Args:
            bucket: S3 bucket name
            key: S3 object key
            local_path: Local destination path

        Returns:
            Future object
        """
        filename = Path(key).name
        logger.info(f"[DL] Starting background download: {filename}")
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
        """Upload a file to S3

        Args:
            local_path: Local file path
            bucket: S3 bucket name
            key: S3 object key
            show_progress: Whether to log progress
        """
        filename = Path(key).name
        logger.info(f"Uploading: {filename}")

        try:
            file_size = local_path.stat().st_size

            # Upload with progress callback
            if show_progress:
                callback = ProgressCallback(f"[UP] {filename}", file_size)
                self.s3_client.upload_file(
                    Filename=str(local_path),
                    Bucket=bucket,
                    Key=key,
                    Callback=callback
                )
                callback.flush()  # Log remaining progress
            else:
                self.s3_client.upload_file(
                    Filename=str(local_path),
                    Bucket=bucket,
                    Key=key
                )

            logger.info(f"Upload complete: {filename}")

        except ClientError as e:
            logger.error(f"Upload failed: {filename} - {e}")
            raise

    def upload_file_async(
        self,
        local_path: Path,
        bucket: str,
        key: str
    ) -> Future[None]:
        """Start an upload in the background

        Args:
            local_path: Local file path
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            Future object
        """
        filename = Path(key).name
        logger.info(f"Starting background upload: {filename}")
        return self.executor.submit(
            self.upload_file,
            local_path,
            bucket,
            key,
            show_progress=True
        )

    def shutdown(self) -> None:
        """Clean up resources"""
        logger.info("[S3] Shutting down executor...")
        self.executor.shutdown(wait=True)
        logger.info("[S3] Shutdown complete")
