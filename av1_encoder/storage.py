from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3

class S3Service:
    NUM_PARALLEL_UPLOAD = 4
    
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def download(self, bucket: str, key: str, local_file: Path) -> bool:
        if local_file.exists():
            return True

        self.s3_client.download_file(bucket, key, str(local_file))
        return False

    def upload_directory(
        self,
        source_dir: Path,
        bucket: str,
        key_prefix: str,
    ) -> None:
        # アップロード対象ファイルを収集
        local_files = [f for f in source_dir.rglob('*') if f.is_file()]

        # 並列アップロード
        with ThreadPoolExecutor(max_workers=self.NUM_PARALLEL_UPLOAD) as executor:
            futures = [
                executor.submit(self._upload_single_file, local_file, bucket, source_dir, key_prefix)
                for local_file in local_files
            ]

            for future in as_completed(futures):
                future.result()

    def _upload_single_file(
        self,
        local_file: Path,
        bucket: str,
        base_dir: Path,
        key_prefix: str
    ) -> None:
        s3_key = self._build_s3_key(local_file, base_dir, key_prefix)
        self._upload_file(local_file, bucket, s3_key)

    def _build_s3_key(self, local_file: Path, base_dir: Path, key_prefix: str) -> str:
        """ローカルファイルパスからS3キーを構築"""
        relative_path = local_file.relative_to(base_dir)
        relative_key = str(relative_path).replace('\\', '/')
        return f"{key_prefix}/{relative_key}" if key_prefix else relative_key

    def _upload_file(self, local_file: Path, bucket: str, key: str) -> None:
        self.s3_client.upload_file(str(local_file), bucket, key)
