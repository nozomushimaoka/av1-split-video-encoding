#!/usr/bin/env python3
"""AV1エンコードパイプライン - S3バッチエンコードCLI"""

import argparse
import logging
import os
import sys

from av1_encoder.s3.batch import run_batch_encoding


def setup_logging() -> None:
    """ロギング設定"""
    logging.basicConfig(
        level=logging.INFO,
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def main() -> int:
    """メイン処理"""
    setup_logging()
    logger = logging.getLogger(__name__)

    # コマンドライン引数のパース
    parser = argparse.ArgumentParser(
        description='AV1エンコードパイプライン - S3オーケストレーション'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        default=os.environ.get('S3_BUCKET'),
        help='S3バケット名（環境変数S3_BUCKETからも取得可能）'
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=10,
        help='並列エンコード数（デフォルト: 10）'
    )
    parser.add_argument(
        '--crf',
        type=int,
        default=36,
        help='CRF値（デフォルト: 36）'
    )
    parser.add_argument(
        '--preset',
        type=int,
        default=6,
        help='プリセット値（デフォルト: 6）'
    )

    args = parser.parse_args()

    # S3バケット名のチェック
    if not args.bucket:
        logger.error("Error: S3バケット名を指定してください（--bucket または環境変数S3_BUCKET）")
        return 1

    # バッチエンコード処理を実行
    return run_batch_encoding(
        bucket=args.bucket,
        parallel=args.parallel,
        crf=args.crf,
        preset=args.preset
    )


if __name__ == '__main__':
    sys.exit(main())
