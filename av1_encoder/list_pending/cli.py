#!/usr/bin/env python3
"""S3未処理ファイル一覧CLI"""

import argparse
import logging
import sys

import boto3

from ..core.logging_config import setup_console_logger
from .pending import calculate_pending_files


def main() -> int:
    # コマンドライン引数のパース
    parser = argparse.ArgumentParser(
        description='S3から未処理ファイルの一覧を取得'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='詳細なログを出力（DEBUGレベル）'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        required=True,
        help='S3バケット名'
    )

    args = parser.parse_args()

    # ログレベルの設定
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_console_logger('av1_encoder.list_pending', level=log_level)
    logger = logging.getLogger(__name__)

    # S3クライアントを作成
    s3_client = boto3.client('s3')

    try:
        logger.debug(f"バケット '{args.bucket}' から未処理ファイルを取得中...")

        # 未処理ファイルを計算
        pending_files = calculate_pending_files(s3_client, args.bucket)

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
