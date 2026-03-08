"""Path utility module

Provides helper functions for handling S3 URIs and local paths.
"""


def is_s3_path(path: str) -> bool:
    """Determine whether the path is an S3 URI

    Args:
        path: Path string to check

    Returns:
        True if it is an S3 URI

    Examples:
        >>> is_s3_path('s3://bucket/key')
        True
        >>> is_s3_path('/home/user/file.mkv')
        False
    """
    return path.startswith('s3://')


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse an S3 URI into bucket and key

    Args:
        uri: S3 URI (e.g. 's3://bucket/path/to/file.mkv')

    Returns:
        Tuple of (bucket_name, key)

    Raises:
        ValueError: If the S3 URI format is invalid

    Examples:
        >>> parse_s3_uri('s3://my-bucket/input/video.mkv')
        ('my-bucket', 'input/video.mkv')
        >>> parse_s3_uri('s3://bucket/file.mkv')
        ('bucket', 'file.mkv')
    """
    if not is_s3_path(uri):
        raise ValueError(f"Not an S3 URI: {uri}")

    # Remove 's3://'
    path = uri[5:]
    bucket, _, key = path.partition('/')

    if not bucket:
        raise ValueError(f"Bucket name is empty: {uri}")

    return bucket, key
