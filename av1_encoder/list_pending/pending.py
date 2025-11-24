"""S3から未処理ファイル一覧を取得"""

from typing import Set


def list_objects(s3_client, bucket_name: str, prefix: str) -> Set[str]:
    """
    S3バケットから指定されたprefixのオブジェクト一覧を取得

    Args:
        s3_client: S3クライアント
        bucket_name: バケット名
        prefix: プレフィックス（例: 'input/', 'output/'）

    Returns:
        prefixを除いた相対パス（例: 'subfolder/file.mkv'）のセット
    """
    all_files = set()
    continuation_token = None

    while True:
        # ページネーション対応
        kwargs = {
            'Bucket': bucket_name,
            'Prefix': prefix
        }
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token

        response = s3_client.list_objects_v2(**kwargs)

        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                # prefixを除いた相対パスを取得
                if key.startswith(prefix):
                    relative_path = key[len(prefix):]
                    # ディレクトリエントリ（末尾が/）は除外
                    if relative_path and not relative_path.endswith('/'):
                        all_files.add(relative_path)

        # 次のページがあるか確認
        if response.get('IsTruncated', False):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return all_files


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
