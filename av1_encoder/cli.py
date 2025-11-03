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
        help='入力ファイルパス (例: video.mkv)'
    )
    parser.add_argument(
        '--workspace', '-w',
        type=str,
        required=True,
        help='作業ディレクトリパス（既存のディレクトリを指定）'
    )
    parser.add_argument(
        '--parallel', '-l',
        type=int,
        default=4,
        help='並列ジョブ数 (デフォルト: 4)'
    )
    parser.add_argument(
        'extra_args',
        nargs='*',
        help='追加のFFmpegオプション (例: -- -crf 30 -preset 6 -pix_fmt yuv420p10le)'
    )

    args = parser.parse_args()

    # 作業ディレクトリの検証
    workspace_dir = Path(args.workspace)
    if not workspace_dir.exists():
        print(f"エラー: 作業ディレクトリが存在しません: {workspace_dir}", file=sys.stderr)
        return 1
    if not workspace_dir.is_dir():
        print(f"エラー: 作業ディレクトリがディレクトリではありません: {workspace_dir}", file=sys.stderr)
        return 1

    # 設定を作成
    config = EncodingConfig(
        input_file=Path(args.input_file),
        workspace_dir=workspace_dir,
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
