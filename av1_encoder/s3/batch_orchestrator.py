"""Batch orchestration module

Coordinates batch encoding of multiple files.
Supports both S3 and local files.
"""
import logging
from concurrent.futures import Future
from pathlib import Path
from typing import Optional

from av1_encoder.core.path_utils import is_s3_path, parse_s3_uri
from av1_encoder.s3.file_processor import process_single_file
from av1_encoder.s3.pipeline import S3Pipeline

logger = logging.getLogger(__name__)


def _load_pending_files(pending_files_path: Path) -> list[str] | None:
    """Load the list of files to process

    Args:
        pending_files_path: Path to the pending files list

    Returns:
        List of file paths, or None on error
    """
    logger.info(f"Loading pending files from: {pending_files_path}")
    try:
        with open(pending_files_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error(f"File not found: {pending_files_path}")
        return None
    except Exception as e:
        logger.error(f"Failed to read file: {e}")
        return None


def _has_s3_files(pending_files: list[str], output_dir: str) -> bool:
    """Check whether any S3 paths are involved"""
    if is_s3_path(output_dir):
        return True
    return any(is_s3_path(f) for f in pending_files)


def _process_files(
    pending_files: list[str],
    output_dir: str,
    workspace_base: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None,
    audio_args: list[str] | None,
    s3: Optional[S3Pipeline],
    hardware_decode: Optional[str] = None,
    hardware_decode_device: Optional[str] = None
) -> None:
    """Process files sequentially

    Args:
        pending_files: Files to process (S3 URIs or local absolute paths)
        output_dir: Output directory (S3 URI or local path)
        workspace_base: Workspace base directory
        parallel: Number of parallel encoding jobs
        gop_size: GOP size
        svtav1_args: SvtAv1EncApp arguments
        ffmpeg_args: FFmpeg arguments
        audio_args: Audio arguments
        s3: S3 pipeline (required when handling S3 files)
    """
    download_future: Optional[Future[None]] = None

    for i, input_file_path in enumerate(pending_files):
        # Resolve display name for logging
        if is_s3_path(input_file_path):
            _, key = parse_s3_uri(input_file_path)
            display_name = Path(key).name
        else:
            display_name = Path(input_file_path).name

        logger.info("=" * 50)
        logger.info(f"Processing ({i+1}/{len(pending_files)}): {display_name}")
        logger.info("=" * 50)

        # Start downloading the next file in the background (S3 only)
        next_download_future = None
        if i + 1 < len(pending_files):
            next_file_path = pending_files[i + 1]
            if is_s3_path(next_file_path) and s3 is not None:
                bucket, key = parse_s3_uri(next_file_path)
                next_local_path = workspace_base / Path(key).name
                next_download_future = s3.download_file_async(
                    bucket,
                    key,
                    next_local_path
                )

        # Process the current file
        process_single_file(
            input_file_path=input_file_path,
            output_dir=output_dir,
            workspace_base=workspace_base,
            parallel=parallel,
            gop_size=gop_size,
            svtav1_args=svtav1_args,
            ffmpeg_args=ffmpeg_args,
            audio_args=audio_args,
            s3=s3,
            download_future=download_future,
            hardware_decode=hardware_decode,
            hardware_decode_device=hardware_decode_device
        )

        # Save for the next iteration
        download_future = next_download_future

        logger.info(f"Done: {display_name}")


def run_batch_encoding(
    pending_files_path: Path,
    output_dir: str,
    workspace_base: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    audio_args: list[str] | None = None,
    hardware_decode: Optional[str] = None,
    hardware_decode_device: Optional[str] = None
) -> int:
    """Run batch encoding

    Args:
        pending_files_path: Path to the pending files list
        output_dir: Output directory (S3 URI or local path)
        workspace_base: Workspace base directory
        parallel: Number of parallel encoding jobs
        gop_size: GOP size
        svtav1_args: SvtAv1EncApp arguments
        ffmpeg_args: FFmpeg arguments
        audio_args: Audio arguments

    Returns:
        0 on success, 1 on error
    """
    # Load pending files
    pending_files = _load_pending_files(pending_files_path)
    if pending_files is None:
        return 1

    if not pending_files:
        logger.info("All files are already processed")
        return 0

    # Initialize S3 pipeline only when S3 paths are involved
    s3: Optional[S3Pipeline] = None
    if _has_s3_files(pending_files, output_dir):
        logger.info("Initializing S3 pipeline...")
        try:
            s3 = S3Pipeline()
        except Exception as e:
            logger.error(f"Failed to initialize S3 pipeline: {e}")
            return 1

    try:
        # Process files sequentially
        _process_files(
            pending_files=pending_files,
            output_dir=output_dir,
            workspace_base=workspace_base,
            parallel=parallel,
            gop_size=gop_size,
            svtav1_args=svtav1_args,
            ffmpeg_args=ffmpeg_args,
            audio_args=audio_args,
            s3=s3,
            hardware_decode=hardware_decode,
            hardware_decode_device=hardware_decode_device
        )

        logger.info("")
        logger.info("All files processed successfully")

        return 0

    except Exception as e:
        logger.error(f"Error during processing: {e}", exc_info=True)
        return 1

    finally:
        if s3 is not None:
            s3.shutdown()
