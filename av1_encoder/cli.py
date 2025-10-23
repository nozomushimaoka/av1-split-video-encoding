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
        help='input/内のファイル名 (例: video.mkv)'
    )
    parser.add_argument(
        '--parallel', '-l',
        type=int,
        default=4,
        help='並列ジョブ数 (デフォルト: 4)'
    )
    parser.add_argument(
        '--crf',
        type=int,
        help='Constant Rate Factor'
    )
    parser.add_argument(
        '--preset',
        type=int,
        help='エンコード速度プリセット'
    )
    parser.add_argument(
        '--keyint',
        type=int,
        help='キーフレーム間隔'
    )
    parser.add_argument(
        '--bucket',
        type=str,
        default='xxx',
        help='S3バケット名'
    )

    args = parser.parse_args()

    # 設定を作成
    config = EncodingConfig(
        input_file=Path(args.input_file),
        parallel_jobs=args.parallel,
        crf=args.crf,
        preset=args.preset,
        keyint=args.keyint,
        s3_bucket=args.bucket
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
