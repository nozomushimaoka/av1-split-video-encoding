#!/usr/bin/env python3
"""Pending files listing CLI

Detects unprocessed files from S3 and local file systems.
"""

import argparse
import logging
import sys

import boto3

from ..core.logging_config import setup_console_logger
from ..core.path_utils import is_s3_path
from .pending import calculate_pending_files


def main() -> int:
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='List pending files (S3 or local)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging (DEBUG level)'
    )
    parser.add_argument(
        '--input-dir', '-i',
        type=str,
        required=True,
        help='Input directory (local path or S3 URI, e.g. /path/to/input or s3://bucket/input/)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        required=True,
        help='Output directory (local path or S3 URI, e.g. /path/to/output or s3://bucket/output/)'
    )

    args = parser.parse_args()

    # Set log level
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_console_logger('av1_encoder.list_pending', level=log_level)
    logger = logging.getLogger(__name__)

    # Create S3 client only when S3 paths are involved
    s3_client = None
    if is_s3_path(args.input_dir) or is_s3_path(args.output_dir):
        s3_client = boto3.client('s3')

    try:
        logger.debug(f"Input: {args.input_dir}")
        logger.debug(f"Output: {args.output_dir}")

        # Calculate pending files
        pending_files = calculate_pending_files(
            args.input_dir,
            args.output_dir,
            s3_client
        )

        logger.debug(f"Pending file count: {len(pending_files)}")

        # Write to stdout (explicitly set UTF-8 for Windows compatibility)
        if hasattr(sys.stdout, 'reconfigure') and sys.stdout.encoding != 'utf-8':
            sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]

        for filename in pending_files:
            print(filename)

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
