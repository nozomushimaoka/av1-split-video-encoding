"""パスユーティリティモジュール

S3 URIとローカルパスを扱うためのヘルパー関数を提供する。
"""


def is_s3_path(path: str) -> bool:
    """S3 URIかどうかを判定

    Args:
        path: 判定するパス文字列

    Returns:
        S3 URIの場合True

    Examples:
        >>> is_s3_path('s3://bucket/key')
        True
        >>> is_s3_path('/home/user/file.mkv')
        False
    """
    return path.startswith('s3://')


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """S3 URIをバケットとキーに分解

    Args:
        uri: S3 URI（例: 's3://bucket/path/to/file.mkv'）

    Returns:
        (バケット名, キー) のタプル

    Raises:
        ValueError: S3 URIの形式が不正な場合

    Examples:
        >>> parse_s3_uri('s3://my-bucket/input/video.mkv')
        ('my-bucket', 'input/video.mkv')
        >>> parse_s3_uri('s3://bucket/file.mkv')
        ('bucket', 'file.mkv')
    """
    if not is_s3_path(uri):
        raise ValueError(f"S3 URIではありません: {uri}")

    # 's3://' を除去
    path = uri[5:]
    bucket, _, key = path.partition('/')

    if not bucket:
        raise ValueError(f"バケット名が空です: {uri}")

    return bucket, key
