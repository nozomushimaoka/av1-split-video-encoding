"""S3オーケストレーションパイプライン"""

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Set

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

    def list_input_files(self) -> Set[str]:
        """S3のinput/ディレクトリから.mkvファイル一覧を取得"""
        logger.info("input/内の.mkvファイルを取得中...")
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix='input/'
            )

            if 'Contents' not in response:
                logger.warning("input/ディレクトリにファイルがありません")
                return set()

            files = {
                obj['Key'].split('/')[-1]
                for obj in response['Contents']
                if obj['Key'].endswith('.mkv')
            }

            logger.info(f"{len(files)}個の.mkvファイルを発見")
            return files

        except ClientError as e:
            logger.error(f"S3からのファイル一覧取得に失敗: {e}")
            raise

    def list_output_files(self) -> Set[str]:
        """S3のoutput/ディレクトリから既存ファイル一覧を取得"""
        logger.info("output/内の既存ファイルを取得中...")
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix='output/'
            )

            if 'Contents' not in response:
                logger.info("output/ディレクトリにファイルがありません")
                return set()

            files = {
                obj['Key'].split('/')[-1]
                for obj in response['Contents']
            }

            logger.info(f"{len(files)}個のファイルが既に出力済み")
            return files

        except ClientError as e:
            logger.error(f"S3からのファイル一覧取得に失敗: {e}")
            raise

    def calculate_pending_files(self) -> list[str]:
        """処理が必要なファイルのリストを計算"""
        input_files = self.list_input_files()

        if not input_files:
            raise ValueError(f"s3://{self.bucket_name}/input/ に.mkvファイルが見つかりません")

        output_files = self.list_output_files()

        # ベース名の差分を計算
        input_base_names = {f.replace('.mkv', '') for f in input_files}
        output_base_names = {f.replace('.mkv', '') for f in output_files}

        pending_base_names = input_base_names - output_base_names

        # スキップされるファイルを表示
        skipped = input_base_names & output_base_names
        for base_name in sorted(skipped):
            logger.info(f"スキップ: {base_name}.mkv (既に処理済み)")

        # 処理対象を表示
        pending_files = sorted([f"{base_name}.mkv" for base_name in pending_base_names])
        for file in pending_files:
            logger.info(f"処理対象: {file}")

        logger.info(f"\n処理開始: {len(pending_files)}ファイル\n")

        return pending_files

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
