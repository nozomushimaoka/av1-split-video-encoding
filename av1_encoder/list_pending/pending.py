"""Pending file detection

Detects unprocessed files from S3 and local file systems.
"""

from pathlib import Path

from av1_encoder.core.path_utils import is_s3_path, parse_s3_uri


def list_s3_objects(s3_client, bucket_name: str, prefix: str) -> set[str]:
    """
    List objects in an S3 bucket under the given prefix

    Args:
        s3_client: S3 client
        bucket_name: Bucket name
        prefix: Key prefix (e.g. 'input/', 'output/')

    Returns:
        Set of relative paths with the prefix stripped (e.g. 'subfolder/file.mkv')
    """
    all_files: set[str] = set()
    continuation_token = None

    while True:
        # Handle pagination
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
                # Get relative path by stripping the prefix
                if key.startswith(prefix):
                    relative_path = key[len(prefix):]
                    # Exclude directory entries (keys ending with /)
                    if relative_path and not relative_path.endswith('/'):
                        all_files.add(relative_path)

        # Check for more pages
        if response.get('IsTruncated', False):
            continuation_token = response.get('NextContinuationToken')
        else:
            break

    return all_files


def list_local_files(directory: Path) -> set[str]:
    """
    List files in a local directory

    Args:
        directory: Directory to search

    Returns:
        Set of relative paths from the directory root (e.g. 'subfolder/file.mkv')
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
    Get a file listing from a path (S3 or local)

    Args:
        path: S3 URI or local path
        s3_client: S3 client (required for S3 paths)

    Returns:
        Tuple of (file set, base path) where base path is used to build absolute paths
    """
    if is_s3_path(path):
        bucket, prefix = parse_s3_uri(path)
        # Ensure prefix ends with /
        if prefix and not prefix.endswith('/'):
            prefix = prefix + '/'
        files = list_s3_objects(s3_client, bucket, prefix)
        # Base path in S3 URI form
        base_path = f"s3://{bucket}/{prefix}"
        return files, base_path
    else:
        directory = Path(path)
        files = list_local_files(directory)
        # Base path as absolute local path
        base_path = str(directory.resolve()) + '/'
        return files, base_path


def calculate_pending_files(input_dir: str, output_dir: str, s3_client=None) -> list[str]:
    """
    Compute the diff between input and output, returning absolute paths of pending files

    Args:
        input_dir: Input directory (S3 URI or local path)
        output_dir: Output directory (S3 URI or local path)
        s3_client: S3 client (required when S3 paths are involved)

    Returns:
        Sorted list of absolute paths (S3 URIs or local) for files not yet processed
    """
    input_files, input_base_path = _get_files_from_path(input_dir, s3_client)
    output_files, _ = _get_files_from_path(output_dir, s3_client)

    # Compute diff by base name (strip .mkv extension)
    input_base_names = {f.replace('.mkv', '') for f in input_files}
    output_base_names = {f.replace('.mkv', '') for f in output_files}

    pending_base_names = input_base_names - output_base_names

    # Build and sort the absolute path list
    pending_files = sorted([
        f"{input_base_path}{base_name}.mkv"
        for base_name in pending_base_names
    ])

    return pending_files
