"""バッチオーケストレーションモジュール

複数ファイルのバッチエンコード処理を調整する。
"""
import logging
from concurrent.futures import Future
from pathlib import Path
from typing import Optional

from av1_encoder.s3.file_processor import process_single_file
from av1_encoder.s3.pipeline import S3Pipeline

logger = logging.getLogger(__name__)


def _load_pending_files(pending_files_path: Path) -> list[str] | None:
    """処理対象ファイルリストを読み込む

    Args:
        pending_files_path: 処理対象ファイルリストのパス

    Returns:
        ファイルリスト、エラー時はNone
    """
    logger.info(f"処理対象ファイルを読み込み: {pending_files_path}")
    try:
        with open(pending_files_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error(f"ファイルが見つかりません: {pending_files_path}")
        return None
    except Exception as e:
        logger.error(f"ファイルの読み込みに失敗: {e}")
        return None


def _process_files(
    s3: S3Pipeline,
    pending_files: list[str],
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None,
    audio_args: list[str] | None
) -> None:
    """ファイルを順次処理する

    Args:
        s3: S3パイプライン
        pending_files: 処理対象ファイルリスト
        parallel: 並列エンコード数
        gop_size: GOPサイズ
        svtav1_args: SvtAv1EncApp用の引数
        ffmpeg_args: FFmpeg用の引数
        audio_args: 音声用の引数
    """
    download_future: Optional[Future[None]] = None

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

        # 現在のファイルを処理（アップロードと削除まで完了する）
        process_single_file(
            s3,
            input_file_name,
            base_name,
            parallel,
            gop_size,
            svtav1_args,
            ffmpeg_args,
            audio_args,
            download_future
        )

        # 次のイテレーションのために保存
        download_future = next_download_future

        logger.info(f"完了: {input_file_name}")


def run_batch_encoding(
    bucket: str,
    pending_files_path: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    audio_args: list[str] | None = None
) -> int:
    """バッチエンコード処理を実行

    Args:
        bucket: S3バケット名
        pending_files_path: 処理対象ファイルリストのパス
        parallel: 並列エンコード数
        gop_size: GOPサイズ
        svtav1_args: SvtAv1EncApp用の引数
        ffmpeg_args: FFmpeg用の引数
        audio_args: 音声用の引数

    Returns:
        0: 成功, 1: エラー
    """
    logger.info(f"S3バケット: {bucket}")

    # S3パイプラインの初期化
    try:
        s3 = S3Pipeline(bucket)
    except Exception as e:
        logger.error(f"S3パイプラインの初期化に失敗: {e}")
        return 1

    try:
        # 処理対象ファイルを読み込む
        pending_files = _load_pending_files(pending_files_path)
        if pending_files is None:
            return 1

        if not pending_files:
            logger.info("すべてのファイルが処理済みです")
            return 0

        # ファイルを順次処理
        _process_files(
            s3, pending_files, parallel, gop_size,
            svtav1_args, ffmpeg_args, audio_args
        )

        logger.info("")
        logger.info("すべての処理が完了しました")

        return 0

    except Exception as e:
        logger.error(f"処理中にエラーが発生: {e}", exc_info=True)
        return 1

    finally:
        s3.shutdown()
