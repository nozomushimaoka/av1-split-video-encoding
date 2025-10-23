"""S3操作サービス"""

import logging
import subprocess
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


class S3Service:
    """S3操作を担当するクラス"""

    def __init__(self, logger: logging.Logger):
        """
        Args:
            logger: ロガーインスタンス
        """
        self.logger = logger
        self.s3_client = boto3.client('s3')

    def download(self, s3_path: str, local_file: Path) -> None:
        """
        S3からファイルをダウンロード

        Args:
            s3_path: S3パス (s3://bucket/key 形式)
            local_file: ローカル保存先パス

        Raises:
            RuntimeError: ダウンロード失敗時
        """
        if local_file.exists():
            self.logger.info(f"既存のファイルを再利用: {local_file}")
            return

        self.logger.info(f"S3ダウンロード開始: {s3_path}")

        # s3://bucket/key からバケットとキーを抽出
        parts = s3_path.replace('s3://', '').split('/', 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ''

        try:
            self.s3_client.download_file(bucket, key, str(local_file))
            self.logger.info("S3ダウンロード完了")
        except ClientError as e:
            self.logger.error(f"S3ダウンロード失敗: {e}")
            raise RuntimeError(f"S3ダウンロード失敗: {e}") from e

    def sync(
        self,
        source_dir: Path,
        s3_destination: str,
        description: str = "ディレクトリ"
    ) -> None:
        """
        ディレクトリをS3に同期

        Args:
            source_dir: 同期元ディレクトリ
            s3_destination: S3同期先パス
            description: 説明（ログ出力用）

        Raises:
            RuntimeError: 同期失敗時
        """
        self.logger.info(f"{description}を同期中: {s3_destination}")

        try:
            result = subprocess.run(
                ['aws', 's3', 'sync', str(source_dir), s3_destination],
                capture_output=True,
                text=True,
                check=True
            )
            self.logger.info(f"{description}同期成功: {s3_destination}")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"{description}同期失敗: {e.stderr}")
            raise RuntimeError(f"{description}同期失敗: {e.stderr}") from e
