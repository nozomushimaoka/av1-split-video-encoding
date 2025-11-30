"""未処理ファイル一覧を取得

S3およびローカルファイルシステムから未処理ファイルを検出する。
"""

from pathlib import Path

from av1_encoder.core.path_utils import is_s3_path, parse_s3_uri


def list_s3_objects(s3_client, bucket_name: str, prefix: str) -> set[str]:
    """
    S3バケットから指定されたprefixのオブジェクト一覧を取得

    Args:
        s3_client: S3クライアント
        bucket_name: バケット名
        prefix: プレフィックス（例: 'input/', 'output/'）

    Returns:
        prefixを除いた相対パス（例: 'subfolder/file.mkv'）のセット
    """
    all_files: set[str] = set()
    continuation_token = None

    while True:
        # ページネーション対応
        kwargs: dict = {
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


def list_local_files(directory: Path) -> set[str]:
    """
    ローカルディレクトリ内のファイル一覧を取得

    Args:
        directory: 検索対象のディレクトリ

    Returns:
        ディレクトリからの相対パスのセット（例: 'subfolder/file.mkv'）
    """
    all_files: set[str] = set()

    if not directory.exists():
        return all_files

    for file_path in directory.rglob('*'):
        if file_path.is_file():
            relative_path = file_path.relative_to(directory)
            all_files.add(str(relative_path))

    return all_files


def _get_files_from_path(path: str, s3_client=None) -> tuple[set[str], str]:
    """
    パスからファイル一覧を取得

    Args:
        path: S3 URIまたはローカルパス
        s3_client: S3クライアント（S3パスの場合に必要）

    Returns:
        (ファイルセット, ベースパス) のタプル
        ベースパスは絶対パス構築に使用
    """
    if is_s3_path(path):
        bucket, prefix = parse_s3_uri(path)
        # prefixの末尾に/がない場合は追加
        if prefix and not prefix.endswith('/'):
            prefix = prefix + '/'
        files = list_s3_objects(s3_client, bucket, prefix)
        # ベースパスはS3 URI形式
        base_path = f"s3://{bucket}/{prefix}"
        return files, base_path
    else:
        directory = Path(path)
        files = list_local_files(directory)
        # ベースパスは絶対パス
        base_path = str(directory.resolve()) + '/'
        return files, base_path


def calculate_pending_files(input_dir: str, output_dir: str, s3_client=None) -> list[str]:
    """
    入力と出力の差分を計算し、未処理ファイルの絶対パスリストを返す

    Args:
        input_dir: 入力ディレクトリ（S3 URIまたはローカルパス）
        output_dir: 出力ディレクトリ（S3 URIまたはローカルパス）
        s3_client: S3クライアント（S3パスが含まれる場合に必要）

    Returns:
        未処理ファイルの絶対パス（S3 URIまたはローカル絶対パス）のリスト
    """
    input_files, input_base_path = _get_files_from_path(input_dir, s3_client)
    output_files, _ = _get_files_from_path(output_dir, s3_client)

    # ベース名の差分を計算
    input_base_names = {f.replace('.mkv', '') for f in input_files}
    output_base_names = {f.replace('.mkv', '') for f in output_files}

    pending_base_names = input_base_names - output_base_names

    # 絶対パスのリストを構築してソート
    pending_files = sorted([
        f"{input_base_path}{base_name}.mkv"
        for base_name in pending_base_names
    ])

    return pending_files
