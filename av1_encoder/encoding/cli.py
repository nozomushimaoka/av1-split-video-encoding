"""CLI entry point"""

import argparse
import logging
import sys
from pathlib import Path

from ..cli_utils import expand_ffmpeg_params, expand_svtav1_params
from ..core.config import EncodingConfig
from .encoder import EncodingOrchestrator


def main() -> int:
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='AV1 parallel encoding'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    parser.add_argument(
        'input_file',
        help='Input file path'
    )
    parser.add_argument(
        'workspace',
        type=str,
        help='Working directory path (must already exist)'
    )
    parser.add_argument(
        '--parallel', '-l',
        type=int,
        required=True,
        help='Number of parallel encoding jobs'
    )
    parser.add_argument(
        '--gop', '-g',
        type=int,
        required=True,
        help='GOP size (keyframe interval)'
    )
    parser.add_argument(
        '--svtav1-params',
        type=str,
        required=True,
        help='SvtAv1EncApp parameters (comma-separated, e.g. preset=4,crf=30,enable-qm=1)'
    )
    parser.add_argument(
        '--ffmpeg-params',
        type=str,
        default=None,
        help='FFmpeg parameters (comma-separated, e.g. vf=scale=1920:1080,r=30)'
    )
    parser.add_argument(
        '--hardware-decode',
        type=str,
        default=None,
        help='Hardware decode type[:device path] (e.g. cuda, vaapi:/dev/dri/renderD128, qsv)'
    )

    args = parser.parse_args()

    # Set log level
    log_level = logging.DEBUG if args.verbose else logging.INFO

    # Build svtav1_args (expand comma-separated values)
    svtav1_args = expand_svtav1_params(args.svtav1_params)

    # Build ffmpeg_args (expand comma-separated values)
    ffmpeg_args = []
    if args.ffmpeg_params:
        ffmpeg_args = expand_ffmpeg_params(args.ffmpeg_params)

    # Parse hardware_decode
    hw_decode, hw_device = None, None
    if args.hardware_decode:
        parts = args.hardware_decode.split(':', 1)
        hw_decode = parts[0]
        hw_device = parts[1] if len(parts) > 1 else None

    # Build config
    config = EncodingConfig(
        input_file=Path(args.input_file),
        workspace_dir=Path(args.workspace),
        parallel_jobs=args.parallel,
        gop_size=args.gop,
        svtav1_args=svtav1_args,
        ffmpeg_args=ffmpeg_args,
        hardware_decode=hw_decode,
        hardware_decode_device=hw_device
    )

    # Initialize orchestrator
    orchestrator = EncodingOrchestrator(config, log_level=log_level)

    try:
        # Run encoding
        orchestrator.run()
        return 0

    except Exception:
        return 1

if __name__ == '__main__':
    sys.exit(main())
