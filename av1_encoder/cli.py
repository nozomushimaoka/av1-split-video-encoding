"""CLIエントリーポイント"""

import argparse
import sys
from pathlib import Path

from .config import EncodingConfig
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
        'extra_args',
        nargs='*',
        help='追加のFFmpegオプション'
    )

    args = parser.parse_args()

    # 設定を作成
    config = EncodingConfig(
        input_file=Path(args.input_file),
        workspace_dir=Path(args.workspace),
        parallel_jobs=args.parallel,
        extra_args=args.extra_args if args.extra_args else []
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
