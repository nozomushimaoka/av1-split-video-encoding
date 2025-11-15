"""S3から未処理ファイル一覧を取得"""

from typing import Set


def list_objects(s3_client, bucket_name: str, prefix: str) -> Set[str]:
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    if 'Contents' not in response:
        return set()

    files = {
        obj['Key'].split('/')[-1]
        for obj in response['Contents']
    }

    return files


def calculate_pending_files(s3_client, bucket_name: str) -> list[str]:
    input_files = list_objects(s3_client, bucket_name, 'input/')
    output_files = list_objects(s3_client, bucket_name, 'output/')

    # ベース名の差分を計算
    input_base_names = {f.replace('.mkv', '') for f in input_files}
    output_base_names = {f.replace('.mkv', '') for f in output_files}

    pending_base_names = input_base_names - output_base_names

    # 処理対象をリストに変換してソート
    pending_files = sorted([f"{base_name}.mkv" for base_name in pending_base_names])

    return pending_files
