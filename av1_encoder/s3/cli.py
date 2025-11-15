#!/usr/bin/env python3
"""AV1エンコードパイプライン - S3バッチエンコードCLI"""

import argparse
import logging
import os
import sys
from pathlib import Path

from av1_encoder.s3.batch import run_batch_encoding


def setup_logging() -> None:
    """S3モジュール専用のロギング設定"""
    # S3モジュールのロガーを取得
    s3_logger = logging.getLogger('av1_encoder.s3')

    # 既にハンドラーが設定されている場合はスキップ
    if s3_logger.handlers:
        return

    # ハンドラーを作成
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(
        '[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    # S3モジュールのロガーにのみハンドラーを追加
    s3_logger.addHandler(handler)
    s3_logger.setLevel(logging.INFO)
    # 親ロガーへの伝播を無効化（独立したロガーとして動作）
    s3_logger.propagate = False


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
        '--pending-files',
        type=Path,
        required=True,
        help='処理対象ファイルのリスト（list_pendingコマンドの出力）'
    )
    parser.add_argument(
        '--parallel', '-l',
        type=int,
        required=True,
        help='並列エンコード数'
    )
    parser.add_argument(
        '--gop', '-g',
        type=int,
        required=True,
        help='GOP サイズ（キーフレーム間隔）'
    )
    parser.add_argument(
        'extra_args',
        nargs='*',
        help='追加のFFmpegオプション'
    )

    args = parser.parse_args()

    # S3バケット名のチェック
    if not args.bucket:
        logger.error("Error: S3バケット名を指定してください（--bucket または環境変数S3_BUCKET）")
        return 1

    # バッチエンコード処理を実行
    return run_batch_encoding(
        bucket=args.bucket,
        pending_files_path=args.pending_files,
        parallel=args.parallel,
        gop_size=args.gop,
        extra_args=args.extra_args if args.extra_args else []
    )


if __name__ == '__main__':
    sys.exit(main())
