"""CLIエントリーポイント"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from .config import EncodingConfig
from .workspace import prepare_workspace
from .encoder import EncodingOrchestrator


def main() -> int:
    """メイン処理"""
    # コマンドライン引数パース
    parser = argparse.ArgumentParser(
        description='AV1並列エンコード'
    )
    parser.add_argument(
        'input_filename',
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
        input_filename=args.input_filename,
        parallel_jobs=args.parallel,
        crf=args.crf,
        preset=args.preset,
        keyint=args.keyint,
        s3_bucket=args.bucket
    )

    start_time = datetime.now()

    # ワークスペース初期化
    workspace = prepare_workspace(Path(args.input_filename), start_time)
    # ロガー初期化
    logger = init_logger(workspace)
    # オーケストレーター初期化
    orchestrator = EncodingOrchestrator(config, workspace, logger, start_time)

    try:
        # エンコード処理実行
        orchestrator.run()
        return 0

    except Exception as e:
        logger.error(f"処理中にエラーが発生しました: {e}", exc_info=True)
        return 1


def init_logger(log_file: Path) -> logging.Logger:
    logger = logging.getLogger("av1_encoder")
    logger.setLevel(logging.INFO)

    # 既存のハンドラをクリア
    logger.handlers.clear()

    # ファイルハンドラ
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.INFO)

    # コンソールハンドラ
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # フォーマッター
    formatter = logging.Formatter(
        '[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


if __name__ == '__main__':
    sys.exit(main())
