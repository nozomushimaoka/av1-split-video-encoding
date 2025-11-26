#!/usr/bin/env python3
"""AV1エンコードパイプライン - S3バッチエンコードCLI"""

import argparse
import logging
import os
import sys
from pathlib import Path

from av1_encoder.cli_utils import (expand_audio_params, expand_ffmpeg_params,
                                   expand_svtav1_params)
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
        '--svtav1-params',
        type=str,
        required=True,
        help='SvtAv1EncApp用のパラメータ（カンマ区切り、例: preset=4,crf=30,enable-qm=1）'
    )
    parser.add_argument(
        '--ffmpeg-params',
        type=str,
        default=None,
        help='FFmpeg用のパラメータ（カンマ区切り、例: vf=scale=1920:1080,r=30）'
    )
    parser.add_argument(
        '--audio-params',
        type=str,
        default=None,
        help='音声パラメータ（カンマ区切り、例: c:a=aac,b:a=128k）'
    )

    args = parser.parse_args()

    # S3バケット名のチェック
    if not args.bucket:
        logger.error("Error: S3バケット名を指定してください（--bucket または環境変数S3_BUCKET）")
        return 1

    # svtav1_argsを構築（カンマ区切りを展開）
    svtav1_args = expand_svtav1_params(args.svtav1_params)

    # ffmpeg_argsを構築（カンマ区切りを展開）
    ffmpeg_args = []
    if args.ffmpeg_params:
        ffmpeg_args = expand_ffmpeg_params(args.ffmpeg_params)

    # audio_argsを構築（カンマ区切りを展開）
    audio_args = []
    if args.audio_params:
        audio_args = expand_audio_params(args.audio_params)

    # バッチエンコード処理を実行
    return run_batch_encoding(
        bucket=args.bucket,
        pending_files_path=args.pending_files,
        parallel=args.parallel,
        gop_size=args.gop,
        svtav1_args=svtav1_args,
        ffmpeg_args=ffmpeg_args,
        audio_args=audio_args
    )


if __name__ == '__main__':
    sys.exit(main())
