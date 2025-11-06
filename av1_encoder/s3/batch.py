"""バッチエンコード処理のコア機能"""

import logging
import subprocess
from concurrent.futures import Future
from datetime import datetime
from pathlib import Path
from typing import Optional

from av1_encoder.s3.pipeline import S3Pipeline

# モジュールレベルでロガーを作成
logger = logging.getLogger(__name__)


def merge_video_with_audio(
    workspace: Path,
    input_file: Path,
    output_file: Path
) -> None:
    """エンコードされた動画と元の音声を結合"""
    concat_file = workspace / "concat.txt"

    if not concat_file.exists():
        raise FileNotFoundError(f"concat.txtが見つかりません: {concat_file}")

    logger.info("結合中...")

    cmd = [
        'ffmpeg',
        '-f', 'concat',
        '-safe', '0',
        '-i', str(concat_file),
        '-i', str(input_file),
        '-map', '0:v:0',
        '-map', '1:a',
        '-c:v', 'copy',
        '-c:a', 'copy',
        str(output_file)
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True)
        logger.info("結合完了")
    except subprocess.CalledProcessError as e:
        logger.error(f"結合に失敗: {e.stderr.decode('utf-8', errors='ignore')}")
        raise


def cleanup_files(workspace: Path, input_file: Path) -> None:
    """一時ファイルのクリーンアップ"""
    # 入力ファイルを削除
    if input_file.exists():
        input_file.unlink()
        logger.info(f"入力ファイルを削除: {input_file}")

    # 出力ファイルを削除（アップロード後）
    output_file = workspace / "output.mkv"
    if output_file.exists():
        output_file.unlink()
        logger.info(f"出力ファイルを削除: {output_file}")

    # セグメントファイルを削除
    for segment_file in workspace.glob("*.mp4"):
        segment_file.unlink()
        logger.debug(f"セグメントファイルを削除: {segment_file}")


def encode_video(
    input_file: Path,
    workspace: Path,
    parallel: int,
    extra_args: list[str]
) -> None:
    """エンコード処理を実行"""
    from av1_encoder.core.config import EncodingConfig
    from av1_encoder.encoding.encoder import EncodingOrchestrator

    # 設定を作成
    config = EncodingConfig(
        input_file=input_file,
        workspace_dir=workspace,
        parallel_jobs=parallel,
        segment_length=60,  # デフォルト値
        extra_args=extra_args
    )

    # エンコード実行（ログはEncodingOrchestratorが担当）
    orchestrator = EncodingOrchestrator(config)
    orchestrator.run()


def process_single_file(
    s3: S3Pipeline,
    input_file_name: str,
    base_name: str,
    parallel: int,
    extra_args: list[str],
    download_future: Optional[Future[None]] = None
) -> Future[None]:
    """単一ファイルの処理"""
    # 前のダウンロードが完了するまで待機
    if download_future is not None:
        logger.info("前のダウンロードの完了を待機中...")
        download_future.result()  # 例外が発生した場合はここで送出される

    # ローカルパス
    input_file = Path(input_file_name)

    # ファイルが存在しない場合はダウンロード
    if not input_file.exists():
        s3.download_file(input_file_name, input_file)

    # ワークスペース作成
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    workspace = Path(f"encode_{base_name}_{timestamp}")
    workspace.mkdir(parents=True, exist_ok=True)

    try:
        # エンコード
        encode_video(input_file, workspace, parallel, extra_args)

        # 結合
        output_file = workspace / "output.mkv"
        merge_video_with_audio(workspace, input_file, output_file)

        # 入力ファイルを削除
        input_file.unlink()
        logger.info(f"入力ファイルを削除: {input_file}")

        # S3へアップロード（拡張子付きで保存）
        upload_future = s3.upload_file_async(output_file, f"{base_name}.mkv")

        # バックグラウンドでクリーンアップ
        logger.info("次の処理に移行（アップロードは継続中）")
        return upload_future

    except Exception as e:
        logger.error(f"処理中にエラーが発生: {e}")
        raise


def run_batch_encoding(
    bucket: str,
    parallel: int,
    extra_args: list[str]
) -> int:
    """バッチエンコード処理を実行"""
    logger.info(f"S3バケット: {bucket}")

    # S3パイプラインの初期化
    try:
        s3 = S3Pipeline(bucket)
    except Exception as e:
        logger.error(f"S3パイプラインの初期化に失敗: {e}")
        return 1

    try:
        # 処理対象ファイルを計算
        pending_files = s3.calculate_pending_files()

        if not pending_files:
            logger.info("すべてのファイルが処理済みです")
            return 0

        # ファイルを順次処理
        download_future: Optional[Future[None]] = None
        upload_future: Optional[Future[None]] = None

        for i, input_file_name in enumerate(pending_files):
            base_name = input_file_name.replace('.mkv', '')

            logger.info("=" * 50)
            logger.info(f"処理中 ({i+1}/{len(pending_files)}): {input_file_name}")
            logger.info("=" * 50)

            # 次のファイルのダウンロードをバックグラウンドで開始
            next_download_future = None
            if i + 1 < len(pending_files):
                next_file_name = pending_files[i + 1]
                next_local_path = Path(next_file_name)
                next_download_future = s3.download_file_async(
                    next_file_name,
                    next_local_path
                )

            # 現在のファイルを処理
            upload_future = process_single_file(
                s3,
                input_file_name,
                base_name,
                parallel,
                extra_args,
                download_future
            )

            # 次のイテレーションのために保存
            download_future = next_download_future

            logger.info(f"完了: {input_file_name}")

        # 最後のダウンロードとアップロードを待つ
        if download_future is not None:
            logger.info("最後のダウンロードの完了を待機中...")
            download_future.result()

        if upload_future is not None:
            logger.info("最後のアップロードの完了を待機中...")
            upload_future.result()

        logger.info("")
        logger.info("すべての処理が完了しました")

        return 0

    except Exception as e:
        logger.error(f"処理中にエラーが発生: {e}", exc_info=True)
        return 1

    finally:
        s3.shutdown()
