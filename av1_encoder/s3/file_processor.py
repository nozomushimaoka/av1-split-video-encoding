"""File processing module

Handles encoding of a single file and its interaction with S3 or local storage.
"""
import logging
import shutil
from concurrent.futures import Future
from pathlib import Path
from typing import Optional

from av1_encoder.core.path_utils import is_s3_path, parse_s3_uri
from av1_encoder.s3.pipeline import S3Pipeline
from av1_encoder.s3.video_merger import merge_video_with_audio

logger = logging.getLogger(__name__)


def encode_video(
    input_file: Path,
    workspace: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    audio_args: list[str] | None = None,
    hardware_decode: Optional[str] = None,
    hardware_decode_device: Optional[str] = None
) -> None:
    """Run the encoding pipeline"""
    from av1_encoder.core.config import EncodingConfig
    from av1_encoder.encoding.encoder import EncodingOrchestrator

    # Build config
    config = EncodingConfig(
        input_file=input_file,
        workspace_dir=workspace,
        parallel_jobs=parallel,
        gop_size=gop_size,
        segment_length=60,  # default value
        svtav1_args=svtav1_args,
        ffmpeg_args=ffmpeg_args or [],
        audio_args=audio_args or [],
        hardware_decode=hardware_decode,
        hardware_decode_device=hardware_decode_device
    )

    # Run encoding (logging is handled by EncodingOrchestrator)
    orchestrator = EncodingOrchestrator(config)
    orchestrator.run()


def _delete_segment_files(workspace: Path, output_file: Path) -> None:
    """Delete segment files only (keep log files and concat.txt)"""
    logger.info("Deleting segment files")
    for file in workspace.iterdir():
        if file.is_file() and file != output_file:
            # Only delete segment files (segment_*.ivf)
            if file.name.startswith("segment_") and file.suffix == ".ivf":
                file.unlink()
                logger.debug(f"Deleted: {file.name}")


def _handle_output(
    output_file: Path,
    output_dir: str,
    base_name: str,
    s3: Optional[S3Pipeline]
) -> None:
    """Save the output file to the appropriate location

    Args:
        output_file: Output file inside the workspace
        output_dir: Destination directory (S3 URI or local path)
        base_name: Base file name without extension
        s3: S3 pipeline (used for S3 output)
    """
    output_filename = f"{base_name}.mkv"

    if is_s3_path(output_dir):
        # Upload to S3
        bucket, prefix = parse_s3_uri(output_dir)
        # Ensure prefix ends with /
        if prefix and not prefix.endswith('/'):
            prefix = prefix + '/'
        key = f"{prefix}{output_filename}"

        logger.info("Starting upload to S3")
        if s3 is None:
            raise RuntimeError("S3 output specified but S3Pipeline is not initialized")
        upload_future = s3.upload_file_async(output_file, bucket, key)

        # Wait for upload to complete
        logger.info("Waiting for upload to complete...")
        upload_future.result()
        logger.info("Upload complete")

        # Remove output.mkv after successful upload
        if output_file.exists():
            logger.info(f"Removing output.mkv: {output_file}")
            output_file.unlink()
    else:
        # Copy to local directory
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        final_output = output_path / output_filename

        logger.info(f"Copying output file: {final_output}")
        shutil.copy2(output_file, final_output)

        # Remove output.mkv after copy
        if output_file.exists():
            logger.info(f"Removing output.mkv: {output_file}")
            output_file.unlink()


def process_single_file(
    input_file_path: str,
    output_dir: str,
    workspace_base: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    audio_args: list[str] | None = None,
    s3: Optional[S3Pipeline] = None,
    download_future: Optional[Future[None]] = None,
    hardware_decode: Optional[str] = None,
    hardware_decode_device: Optional[str] = None
) -> None:
    """Process a single file

    Args:
        input_file_path: Input file path (S3 URI or local absolute path)
        output_dir: Output directory (S3 URI or local path)
        workspace_base: Workspace base directory
        parallel: Number of parallel encoding jobs
        gop_size: GOP size
        svtav1_args: SvtAv1EncApp arguments
        ffmpeg_args: FFmpeg arguments
        audio_args: Audio arguments
        s3: S3 pipeline (required when handling S3 files)
        download_future: Future for a previous background download
    """
    # Wait for the previous download to finish
    if download_future is not None:
        logger.info("Waiting for previous download to complete...")
        download_future.result()  # Re-raises any exception from the download

    # Resolve the input file
    is_s3_input = is_s3_path(input_file_path)

    if is_s3_input:
        # S3 file: download it
        bucket, key = parse_s3_uri(input_file_path)
        filename = Path(key).name
        input_file = workspace_base / filename

        if not input_file.exists():
            if s3 is None:
                raise RuntimeError("S3 input specified but S3Pipeline is not initialized")
            s3.download_file(bucket, key, input_file)
    else:
        # Local file: use directly
        input_file = Path(input_file_path)
        filename = input_file.name

    base_name = input_file.stem

    # Create workspace
    workspace = workspace_base / f"encode_{base_name}"
    workspace.mkdir(parents=True, exist_ok=True)

    output_file = None
    try:
        # Encode
        encode_video(input_file, workspace, parallel, gop_size, svtav1_args, ffmpeg_args, audio_args,
                     hardware_decode, hardware_decode_device)

        # Merge
        output_file = workspace / "output.mkv"
        merge_video_with_audio(workspace, input_file, output_file, audio_args)

        # Delete downloaded input only if it came from S3
        if is_s3_input:
            input_file.unlink()
            logger.info(f"Removed downloaded input file: {input_file}")

        # Remove segment files
        _delete_segment_files(workspace, output_file)

        # Save output to the appropriate location
        _handle_output(output_file, output_dir, base_name, s3)

        logger.info("File processing complete")

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        # On error, clean up only files downloaded from S3
        if is_s3_input and input_file.exists():
            logger.info("Removing downloaded input file due to error")
            try:
                input_file.unlink()
                logger.info(f"Removed input file: {input_file}")
            except Exception as cleanup_error:
                logger.warning(f"Failed to remove input file: {cleanup_error}")
        raise
