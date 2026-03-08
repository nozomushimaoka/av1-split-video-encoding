#!/usr/bin/env python3
"""AV1 encoding pipeline - batch encode CLI

Batch encoding for both S3 and local files.
"""

import argparse
import logging
import sys
from pathlib import Path

from av1_encoder.cli_utils import (expand_audio_params, expand_ffmpeg_params,
                                   expand_svtav1_params)
from av1_encoder.core.logging_config import setup_console_logger
from av1_encoder.s3.batch_orchestrator import run_batch_encoding


def main() -> int:
    """Main entry point"""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='AV1 encoding pipeline - batch encode (S3/local)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    parser.add_argument(
        '--pending-files',
        type=Path,
        required=True,
        help='File containing list of inputs to process (output of list_pending)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='.',
        help='Output directory (local path or S3 URI, default: current directory)'
    )
    parser.add_argument(
        '--workspace-base', '-b',
        type=Path,
        default=Path('.'),
        help='Workspace base directory (default: current directory)'
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
        '--audio-params',
        type=str,
        default=None,
        help='Audio parameters (comma-separated, e.g. c:a=aac,b:a=128k)'
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

    # Configure logging for the s3 module
    setup_console_logger('av1_encoder.s3', level=log_level)

    # Build svtav1_args (expand comma-separated values)
    svtav1_args = expand_svtav1_params(args.svtav1_params)

    # Build ffmpeg_args (expand comma-separated values)
    ffmpeg_args = []
    if args.ffmpeg_params:
        ffmpeg_args = expand_ffmpeg_params(args.ffmpeg_params)

    # Build audio_args (expand comma-separated values)
    audio_args = []
    if args.audio_params:
        audio_args = expand_audio_params(args.audio_params)

    # Parse hardware_decode
    hw_decode, hw_device = None, None
    if args.hardware_decode:
        parts = args.hardware_decode.split(':', 1)
        hw_decode = parts[0]
        hw_device = parts[1] if len(parts) > 1 else None

    # Run batch encoding
    return run_batch_encoding(
        pending_files_path=args.pending_files,
        output_dir=args.output_dir,
        workspace_base=args.workspace_base,
        parallel=args.parallel,
        gop_size=args.gop,
        svtav1_args=svtav1_args,
        ffmpeg_args=ffmpeg_args,
        audio_args=audio_args,
        hardware_decode=hw_decode,
        hardware_decode_device=hw_device
    )


if __name__ == '__main__':
    sys.exit(main())
