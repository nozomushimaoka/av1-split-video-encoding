"""CLIエントリーポイント"""

import argparse
import sys
from datetime import datetime

from .config import EncodingConfig
from .encoder import EncodingOrchestrator
from .workspace import Workspace


def print_header(
    config: EncodingConfig,
    workspace: Workspace,
    logger
) -> None:
    """ヘッダー情報を出力"""
    s3_path = f"s3://{config.s3_bucket}/input/{config.input_filename}"
    logger.info("=" * 50)
    logger.info("FFmpeg並列エンコード処理開始")
    logger.info("=" * 50)
    logger.info(f"入力: {s3_path}")
    logger.info(f"作業ディレクトリ: {workspace.work_dir}")
    logger.info(f"並列ジョブ数: {config.parallel_jobs}")
    if config.crf is not None:
        logger.info(f"CRF: {config.crf}")
    if config.preset:
        logger.info(f"プリセット: {config.preset}")
    if config.keyint is not None:
        logger.info(f"キーフレーム間隔: {config.keyint}")
    logger.info("=" * 50)


def print_completion(start_time: datetime, workspace: Workspace, logger, config: EncodingConfig) -> None:
    """完了メッセージを出力"""
    end_time = datetime.now()
    elapsed = end_time - start_time
    s3_output_path = f"s3://{config.s3_bucket}/output/{workspace.work_dir.name}/"

    logger.info("=" * 50)
    logger.info("全処理完了")
    logger.info(f"処理時間: {elapsed}")
    logger.info(f"出力先: {s3_output_path}")
    logger.info("=" * 50)


def main() -> int:
    """メイン処理"""
    # コマンドライン引数パース
    parser = argparse.ArgumentParser(
        description='FFmpeg並列エンコード - 統合スクリプト（Python版）'
    )
    parser.add_argument(
        'input_filename',
        help='input/内のファイル名 (例: video.mkv)'
    )
    parser.add_argument(
        '--parallel', '-p',
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
        type=str,
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

    # ワークスペース初期化
    workspace = Workspace(args.input_filename)
    logger = workspace.setup_logging()

    # 開始時刻
    start_time = datetime.now()

    try:
        # ヘッダー出力
        print_header(config, workspace, logger)

        # エンコード処理実行
        orchestrator = EncodingOrchestrator(config, workspace.config, logger)
        orchestrator.run()

        # 完了メッセージ
        print_completion(start_time, workspace, logger, config)

        return 0

    except Exception as e:
        logger.error(f"処理中にエラーが発生しました: {e}", exc_info=True)
        logger.error(f"作業ディレクトリ: {workspace.work_dir}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
