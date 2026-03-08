"""Video and audio merge module

Merges encoded video segments with the original audio track.
"""
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def merge_video_with_audio(
    workspace: Path,
    input_file: Path,
    output_file: Path,
    audio_args: list[str] | None = None
) -> None:
    """Merge encoded video with the original audio track

    Args:
        workspace: Workspace directory
        input_file: Original input file (audio source)
        output_file: Output file path
        audio_args: Expanded audio arguments (e.g. ['-c:a', 'aac', '-b:a', '128k'])
                   Defaults to ['-c:a', 'copy'] if not specified
    """
    concat_file = workspace / "concat.txt"

    if not concat_file.exists():
        raise FileNotFoundError(f"concat.txt not found: {concat_file}")

    logger.info("Merging...")

    # Build base command
    cmd = [
        'ffmpeg',
        '-f', 'concat',
        '-safe', '0',
        '-i', str(concat_file),
        '-i', str(input_file),
        '-map', '0:v:0',
        '-map', '1:a?',
        '-c:v', 'copy',
    ]

    # Append audio arguments
    if audio_args:
        cmd.extend(audio_args)
    else:
        # Default: copy audio stream
        cmd.extend(['-c:a', 'copy'])

    # Append output file
    cmd.append(str(output_file))

    try:
        # Use DEVNULL for stdout to avoid high memory usage from large output
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info("Merge complete")
    except subprocess.CalledProcessError as e:
        logger.error(f"Merge failed: {e.stderr}")
        raise
