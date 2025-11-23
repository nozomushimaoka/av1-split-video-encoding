"""CLIエントリーポイント"""

import argparse
import sys
from pathlib import Path

from ..core.config import EncodingConfig
from ..utils import expand_svtav1_params
from .encoder import EncodingOrchestrator


def main() -> int:
    # コマンドライン引数パース
    parser = argparse.ArgumentParser(
        description='AV1並列エンコード'
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
        '-svtav1-params',
        type=str,
        default=None,
        help='SvtAv1EncApp用のパラメータ（コロン区切り、例: preset=4:crf=30:enable-qm=1）'
    )

    args = parser.parse_args()

    # svtav1_argsを構築（コロン区切りを展開）
    svtav1_args = []
    if args.svtav1_params:
        svtav1_args = expand_svtav1_params(args.svtav1_params)

    # 設定を作成
    config = EncodingConfig(
        input_file=Path(args.input_file),
        workspace_dir=Path(args.workspace),
        parallel_jobs=args.parallel,
        gop_size=args.gop,
        svtav1_args=svtav1_args
    )

    # オーケストレーター初期化
    orchestrator = EncodingOrchestrator(config)

    try:
        # エンコード処理実行
        orchestrator.run()
        return 0

    except Exception:
        return 1

if __name__ == '__main__':
    sys.exit(main())
