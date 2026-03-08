"""バッチオーケストレーションモジュール

複数ファイルのバッチエンコード処理を調整する。
S3およびローカルファイルの両方に対応。
"""
import logging
from concurrent.futures import Future
from pathlib import Path
from typing import Optional

from av1_encoder.core.path_utils import is_s3_path, parse_s3_uri
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


def _has_s3_files(pending_files: list[str], output_dir: str) -> bool:
    """S3ファイルが含まれるかどうかを判定"""
    if is_s3_path(output_dir):
        return True
    return any(is_s3_path(f) for f in pending_files)


def _process_files(
    pending_files: list[str],
    output_dir: str,
    workspace_base: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None,
    audio_args: list[str] | None,
    s3: Optional[S3Pipeline],
    hardware_decode: Optional[str] = None,
    hardware_decode_device: Optional[str] = None
) -> None:
    """ファイルを順次処理する

    Args:
        pending_files: 処理対象ファイルリスト（S3 URIまたはローカル絶対パス）
        output_dir: 出力先ディレクトリ（S3 URIまたはローカルパス）
        workspace_base: ワークスペースのベースディレクトリ
        parallel: 並列エンコード数
        gop_size: GOPサイズ
        svtav1_args: SvtAv1EncApp用の引数
        ffmpeg_args: FFmpeg用の引数
        audio_args: 音声用の引数
        s3: S3パイプライン（S3ファイルを扱う場合に必要）
    """
    download_future: Optional[Future[None]] = None

    for i, input_file_path in enumerate(pending_files):
        # ファイル名を取得（ログ表示用）
        if is_s3_path(input_file_path):
            _, key = parse_s3_uri(input_file_path)
            display_name = Path(key).name
        else:
            display_name = Path(input_file_path).name

        logger.info("=" * 50)
        logger.info(f"処理中 ({i+1}/{len(pending_files)}): {display_name}")
        logger.info("=" * 50)

        # 次のファイルのダウンロードをバックグラウンドで開始（S3ファイルの場合のみ）
        next_download_future = None
        if i + 1 < len(pending_files):
            next_file_path = pending_files[i + 1]
            if is_s3_path(next_file_path) and s3 is not None:
                bucket, key = parse_s3_uri(next_file_path)
                next_local_path = workspace_base / Path(key).name
                next_download_future = s3.download_file_async(
                    bucket,
                    key,
                    next_local_path
                )

        # 現在のファイルを処理
        process_single_file(
            input_file_path=input_file_path,
            output_dir=output_dir,
            workspace_base=workspace_base,
            parallel=parallel,
            gop_size=gop_size,
            svtav1_args=svtav1_args,
            ffmpeg_args=ffmpeg_args,
            audio_args=audio_args,
            s3=s3,
            download_future=download_future,
            hardware_decode=hardware_decode,
            hardware_decode_device=hardware_decode_device
        )

        # 次のイテレーションのために保存
        download_future = next_download_future

        logger.info(f"完了: {display_name}")


def run_batch_encoding(
    pending_files_path: Path,
    output_dir: str,
    workspace_base: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    audio_args: list[str] | None = None,
    hardware_decode: Optional[str] = None,
    hardware_decode_device: Optional[str] = None
) -> int:
    """バッチエンコード処理を実行

    Args:
        pending_files_path: 処理対象ファイルリストのパス
        output_dir: 出力先ディレクトリ（S3 URIまたはローカルパス）
        workspace_base: ワークスペースのベースディレクトリ
        parallel: 並列エンコード数
        gop_size: GOPサイズ
        svtav1_args: SvtAv1EncApp用の引数
        ffmpeg_args: FFmpeg用の引数
        audio_args: 音声用の引数

    Returns:
        0: 成功, 1: エラー
    """
    # 処理対象ファイルを読み込む
    pending_files = _load_pending_files(pending_files_path)
    if pending_files is None:
        return 1

    if not pending_files:
        logger.info("すべてのファイルが処理済みです")
        return 0

    # S3ファイルが含まれる場合のみS3パイプラインを初期化
    s3: Optional[S3Pipeline] = None
    if _has_s3_files(pending_files, output_dir):
        logger.info("S3パイプラインを初期化中...")
        try:
            s3 = S3Pipeline()
        except Exception as e:
            logger.error(f"S3パイプラインの初期化に失敗: {e}")
            return 1

    try:
        # ファイルを順次処理
        _process_files(
            pending_files=pending_files,
            output_dir=output_dir,
            workspace_base=workspace_base,
            parallel=parallel,
            gop_size=gop_size,
            svtav1_args=svtav1_args,
            ffmpeg_args=ffmpeg_args,
            audio_args=audio_args,
            s3=s3,
            hardware_decode=hardware_decode,
            hardware_decode_device=hardware_decode_device
        )

        logger.info("")
        logger.info("すべての処理が完了しました")

        return 0

    except Exception as e:
        logger.error(f"処理中にエラーが発生: {e}", exc_info=True)
        return 1

    finally:
        if s3 is not None:
            s3.shutdown()
