"""ファイル処理モジュール

単一ファイルのエンコード処理とS3/ローカルファイルとの連携を担当する。
"""
import logging
import shutil
from concurrent.futures import Future
from pathlib import Path
from typing import Optional

from av1_encoder.core.path_utils import is_s3_path, parse_s3_uri
from av1_encoder.s3.pipeline import S3Pipeline
from av1_encoder.s3.video_merger import merge_video_with_audio

logger = logging.getLogger(__name__)


def encode_video(
    input_file: Path,
    workspace: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    audio_args: list[str] | None = None
) -> None:
    """エンコード処理を実行"""
    from av1_encoder.core.config import EncodingConfig
    from av1_encoder.encoding.encoder import EncodingOrchestrator

    # 設定を作成
    config = EncodingConfig(
        input_file=input_file,
        workspace_dir=workspace,
        parallel_jobs=parallel,
        gop_size=gop_size,
        segment_length=60,  # デフォルト値
        svtav1_args=svtav1_args,
        ffmpeg_args=ffmpeg_args or [],
        audio_args=audio_args or []
    )

    # エンコード実行（ログはEncodingOrchestratorが担当）
    orchestrator = EncodingOrchestrator(config)
    orchestrator.run()


def _delete_segment_files(workspace: Path, output_file: Path) -> None:
    """セグメントファイルのみを削除（ログファイルとconcat.txtは保持）"""
    logger.info("セグメントファイルを削除中")
    for file in workspace.iterdir():
        if file.is_file() and file != output_file:
            # セグメントファイル(segment_*.ivf)のみを削除
            if file.name.startswith("segment_") and file.suffix == ".ivf":
                file.unlink()
                logger.debug(f"削除: {file.name}")


def _handle_output(
    output_file: Path,
    output_dir: str,
    base_name: str,
    s3: Optional[S3Pipeline]
) -> None:
    """出力ファイルを適切な場所に保存

    Args:
        output_file: ワークスペース内の出力ファイル
        output_dir: 出力先ディレクトリ（S3 URIまたはローカルパス）
        base_name: ベースファイル名（拡張子なし）
        s3: S3パイプライン（S3出力の場合に使用）
    """
    output_filename = f"{base_name}.mkv"

    if is_s3_path(output_dir):
        # S3へアップロード
        bucket, prefix = parse_s3_uri(output_dir)
        # prefixの末尾に/がない場合は追加
        if prefix and not prefix.endswith('/'):
            prefix = prefix + '/'
        key = f"{prefix}{output_filename}"

        logger.info("S3へのアップロード開始")
        if s3 is None:
            raise RuntimeError("S3出力が指定されていますが、S3Pipelineが初期化されていません")
        upload_future = s3.upload_file_async(output_file, bucket, key)

        # アップロード完了を待つ
        logger.info("アップロード完了を待機中...")
        upload_future.result()
        logger.info("アップロード完了")

        # アップロード完了後にoutput.mkvを削除
        if output_file.exists():
            logger.info(f"output.mkvを削除中: {output_file}")
            output_file.unlink()
    else:
        # ローカルディレクトリにコピー
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        final_output = output_path / output_filename

        logger.info(f"出力ファイルをコピー: {final_output}")
        shutil.copy2(output_file, final_output)

        # コピー完了後にoutput.mkvを削除
        if output_file.exists():
            logger.info(f"output.mkvを削除中: {output_file}")
            output_file.unlink()


def process_single_file(
    input_file_path: str,
    output_dir: str,
    workspace_base: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    audio_args: list[str] | None = None,
    s3: Optional[S3Pipeline] = None,
    download_future: Optional[Future[None]] = None
) -> None:
    """単一ファイルの処理

    Args:
        input_file_path: 入力ファイルパス（S3 URIまたはローカル絶対パス）
        output_dir: 出力先ディレクトリ（S3 URIまたはローカルパス）
        workspace_base: ワークスペースのベースディレクトリ
        parallel: 並列エンコード数
        gop_size: GOPサイズ
        svtav1_args: SvtAv1EncApp用の引数
        ffmpeg_args: FFmpeg用の引数
        audio_args: 音声用の引数
        s3: S3パイプライン（S3ファイルを扱う場合に必要）
        download_future: 前のダウンロードのFuture
    """
    # 前のダウンロードが完了するまで待機
    if download_future is not None:
        logger.info("前のダウンロードの完了を待機中...")
        download_future.result()  # 例外が発生した場合はここで送出される

    # 入力ファイルの取得
    is_s3_input = is_s3_path(input_file_path)

    if is_s3_input:
        # S3ファイル: ダウンロード
        bucket, key = parse_s3_uri(input_file_path)
        filename = Path(key).name
        input_file = workspace_base / filename

        if not input_file.exists():
            if s3 is None:
                raise RuntimeError("S3入力が指定されていますが、S3Pipelineが初期化されていません")
            s3.download_file(bucket, key, input_file)
    else:
        # ローカルファイル: そのまま使用
        input_file = Path(input_file_path)
        filename = input_file.name

    base_name = input_file.stem

    # ワークスペース作成
    workspace = workspace_base / f"encode_{base_name}"
    workspace.mkdir(parents=True, exist_ok=True)

    output_file = None
    try:
        # エンコード
        encode_video(input_file, workspace, parallel, gop_size, svtav1_args, ffmpeg_args, audio_args)

        # 結合
        output_file = workspace / "output.mkv"
        merge_video_with_audio(workspace, input_file, output_file, audio_args)

        # S3からダウンロードしたファイルの場合のみ削除
        if is_s3_input:
            input_file.unlink()
            logger.info(f"ダウンロードした入力ファイルを削除: {input_file}")

        # セグメントファイルを削除
        _delete_segment_files(workspace, output_file)

        # 出力ファイルを適切な場所に保存
        _handle_output(output_file, output_dir, base_name, s3)

        logger.info("ファイル処理が完全に完了")

    except Exception as e:
        logger.error(f"処理中にエラーが発生: {e}")
        # エラー時のクリーンアップ（S3からダウンロードしたファイルのみ削除）
        if is_s3_input and input_file.exists():
            logger.info("エラーが発生したため、ダウンロードした入力ファイルを削除します")
            try:
                input_file.unlink()
                logger.info(f"入力ファイルを削除: {input_file}")
            except Exception as cleanup_error:
                logger.warning(f"入力ファイルの削除に失敗: {cleanup_error}")
        raise
