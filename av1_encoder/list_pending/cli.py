#!/usr/bin/env python3
"""未処理ファイル一覧CLI

S3およびローカルファイルシステムから未処理ファイルを検出する。
"""

import argparse
import logging
import sys

import boto3

from ..core.logging_config import setup_console_logger
from ..core.path_utils import is_s3_path
from .pending import calculate_pending_files


def main() -> int:
    # コマンドライン引数のパース
    parser = argparse.ArgumentParser(
        description='未処理ファイルの一覧を取得（S3またはローカル）'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='詳細なログを出力（DEBUGレベル）'
    )
    parser.add_argument(
        '--input-dir', '-i',
        type=str,
        required=True,
        help='入力ディレクトリ（ローカルパスまたはS3 URI、例: /path/to/input または s3://bucket/input/）'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        required=True,
        help='出力ディレクトリ（ローカルパスまたはS3 URI、例: /path/to/output または s3://bucket/output/）'
    )

    args = parser.parse_args()

    # ログレベルの設定
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_console_logger('av1_encoder.list_pending', level=log_level)
    logger = logging.getLogger(__name__)

    # S3パスが含まれる場合のみS3クライアントを作成
    s3_client = None
    if is_s3_path(args.input_dir) or is_s3_path(args.output_dir):
        s3_client = boto3.client('s3')

    try:
        logger.debug(f"入力: {args.input_dir}")
        logger.debug(f"出力: {args.output_dir}")

        # 未処理ファイルを計算
        pending_files = calculate_pending_files(
            args.input_dir,
            args.output_dir,
            s3_client
        )

        logger.debug(f"未処理ファイル数: {len(pending_files)}")

        # 標準出力に出力
        for filename in pending_files:
            print(filename)

        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
