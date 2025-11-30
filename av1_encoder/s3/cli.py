#!/usr/bin/env python3
"""AV1エンコードパイプライン - バッチエンコードCLI

S3およびローカルファイルの両方に対応したバッチエンコード。
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
    """メイン処理"""
    # コマンドライン引数のパース
    parser = argparse.ArgumentParser(
        description='AV1エンコードパイプライン - バッチエンコード（S3/ローカル対応）'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='詳細なログを出力（DEBUGレベル）'
    )
    parser.add_argument(
        '--pending-files',
        type=Path,
        required=True,
        help='処理対象ファイルのリスト（list_pendingコマンドの出力）'
    )
    parser.add_argument(
        '--output-dir', '-o',
        type=str,
        default='.',
        help='出力先ディレクトリ（ローカルパスまたはS3 URI、デフォルト: カレントディレクトリ）'
    )
    parser.add_argument(
        '--workspace-base', '-b',
        type=Path,
        default=Path('.'),
        help='ワークスペースのベースディレクトリ（デフォルト: カレントディレクトリ）'
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

    # ログレベルの設定
    log_level = logging.DEBUG if args.verbose else logging.INFO

    # S3モジュール専用のロギング設定
    setup_console_logger('av1_encoder.s3', level=log_level)

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
        pending_files_path=args.pending_files,
        output_dir=args.output_dir,
        workspace_base=args.workspace_base,
        parallel=args.parallel,
        gop_size=args.gop,
        svtav1_args=svtav1_args,
        ffmpeg_args=ffmpeg_args,
        audio_args=audio_args
    )


if __name__ == '__main__':
    sys.exit(main())
