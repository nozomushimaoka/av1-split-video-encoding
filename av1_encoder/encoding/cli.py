"""CLIエントリーポイント"""

import argparse
import logging
import sys
from pathlib import Path

from ..cli_utils import expand_ffmpeg_params, expand_svtav1_params
from ..core.config import EncodingConfig
from .encoder import EncodingOrchestrator


def main() -> int:
    # コマンドライン引数パース
    parser = argparse.ArgumentParser(
        description='AV1並列エンコード'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='詳細なログを出力（DEBUGレベル）'
    )
    parser.add_argument(
        'input_file',
        help='入力ファイルパス'
    )
    parser.add_argument(
        'workspace',
        type=str,
        help='作業ディレクトリパス（既存のディレクトリを指定）'
    )
    parser.add_argument(
        '--parallel', '-l',
        type=int,
        required=True,
        help='エンコード並列数'
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
        '--hardware-decode',
        type=str,
        default=None,
        help='ハードウェアデコードタイプ[:デバイスパス] (例: cuda, vaapi:/dev/dri/renderD128, qsv)'
    )

    args = parser.parse_args()

    # ログレベルの設定
    log_level = logging.DEBUG if args.verbose else logging.INFO

    # svtav1_argsを構築（カンマ区切りを展開）
    svtav1_args = expand_svtav1_params(args.svtav1_params)

    # ffmpeg_argsを構築（カンマ区切りを展開）
    ffmpeg_args = []
    if args.ffmpeg_params:
        ffmpeg_args = expand_ffmpeg_params(args.ffmpeg_params)

    # hardware_decodeのパース
    hw_decode, hw_device = None, None
    if args.hardware_decode:
        parts = args.hardware_decode.split(':', 1)
        hw_decode = parts[0]
        hw_device = parts[1] if len(parts) > 1 else None

    # 設定を作成
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

    # オーケストレーター初期化
    orchestrator = EncodingOrchestrator(config, log_level=log_level)

    try:
        # エンコード処理実行
        orchestrator.run()
        return 0

    except Exception:
        return 1

if __name__ == '__main__':
    sys.exit(main())
