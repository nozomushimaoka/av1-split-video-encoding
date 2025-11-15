"""S3オーケストレーションパイプライン"""

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from tqdm import tqdm

logger = logging.getLogger(__name__)


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

            # プログレスバー付きでダウンロード
            if show_progress:
                with tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"[DL] {filename}"
                ) as pbar:
                    self.s3_client.download_file(
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Filename=str(local_path),
                        Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
                    )
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
    ) -> Future:
        """バックグラウンドでダウンロードを開始"""
        logger.info(f"[DL] 次ファイルのダウンロード開始: {filename}")
        return self.executor.submit(
            self.download_file,
            filename,
            local_path,
            show_progress=False  # バックグラウンドではプログレスバーを表示しない
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

            # プログレスバー付きでアップロード
            if show_progress:
                with tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"[UP] {base_name}"
                ) as pbar:
                    self.s3_client.upload_file(
                        Filename=str(local_path),
                        Bucket=self.bucket_name,
                        Key=s3_key,
                        Callback=lambda bytes_transferred: pbar.update(bytes_transferred)
                    )
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
    ) -> Future:
        """バックグラウンドでアップロードを開始"""
        logger.info(f"バックグラウンドアップロード開始: {base_name}")
        return self.executor.submit(
            self.upload_file,
            local_path,
            base_name,
            show_progress=False  # バックグラウンドではプログレスバーを表示しない
        )

    def shutdown(self) -> None:
        """リソースのクリーンアップ"""
        self.executor.shutdown(wait=True)
