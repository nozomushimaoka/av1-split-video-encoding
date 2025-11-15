#!/usr/bin/env python3
"""S3未処理ファイル一覧CLI"""

import argparse
import sys

import boto3

from .pending import calculate_pending_files


def main() -> int:
    # コマンドライン引数のパース
    parser = argparse.ArgumentParser(
        description='S3から未処理ファイルの一覧を取得'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        required=True,
        help='S3バケット名'
    )

    args = parser.parse_args()

    # S3クライアントを作成
    s3_client = boto3.client('s3')

    try:
        # 未処理ファイルを計算
        pending_files = calculate_pending_files(s3_client, args.bucket)

        # 標準出力に出力
        for filename in pending_files:
            print(filename)

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
