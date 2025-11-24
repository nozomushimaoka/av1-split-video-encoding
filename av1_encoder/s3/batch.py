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
        '-map', '1:a?',
        '-c:v', 'copy',
        '-c:a', 'copy',
        str(output_file)
    ]

    try:
        # capture_output=Trueは大きな出力でメモリを消費するため、DEVNULLにリダイレクト
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True
        )
        logger.info("結合完了")
    except subprocess.CalledProcessError as e:
        logger.error(f"結合に失敗: {e.stderr}")
        raise


def encode_video(
    input_file: Path,
    workspace: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None
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
        ffmpeg_args=ffmpeg_args or []
    )

    # エンコード実行（ログはEncodingOrchestratorが担当）
    orchestrator = EncodingOrchestrator(config)
    orchestrator.run()


def process_single_file(
    s3: S3Pipeline,
    input_file_name: str,
    base_name: str,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None,
    download_future: Optional[Future[None]] = None
) -> None:
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

    output_file = None
    try:
        # エンコード
        encode_video(input_file, workspace, parallel, gop_size, svtav1_args, ffmpeg_args)

        # 結合
        output_file = workspace / "output.mkv"
        merge_video_with_audio(workspace, input_file, output_file)

        # 入力ファイルを削除
        input_file.unlink()
        logger.info(f"入力ファイルを削除: {input_file}")

        # 結合後、セグメントファイルのみを削除（ログファイルとconcat.txtは保持）
        logger.info("セグメントファイルを削除中")
        for file in workspace.iterdir():
            if file.is_file() and file != output_file:
                # セグメントファイル(segment_*.ivf)のみを削除
                if file.name.startswith("segment_") and file.suffix == ".ivf":
                    file.unlink()
                    logger.debug(f"削除: {file.name}")

        # S3へアップロード（拡張子付きで保存）
        logger.info("S3へのアップロード開始（バックグラウンド）")
        upload_future = s3.upload_file_async(output_file, f"{base_name}.mkv")

        # アップロード完了を待つ
        logger.info("アップロード完了を待機中...")
        upload_future.result()  # アップロード完了まで待機
        logger.info("アップロード完了")

        # アップロード完了後にoutput.mkvを削除
        if output_file.exists():
            logger.info(f"output.mkvを削除中: {output_file}")
            output_file.unlink()
            logger.info(f"output.mkvを削除完了: {output_file}")

        logger.info("ファイル処理が完全に完了")

    except Exception as e:
        logger.error(f"処理中にエラーが発生: {e}")
        # エラー時のクリーンアップ
        logger.info("エラーが発生したため、ダウンロードした入力ファイルを削除します")
        if input_file.exists():
            try:
                input_file.unlink()
                logger.info(f"入力ファイルを削除: {input_file}")
            except Exception as cleanup_error:
                logger.warning(f"入力ファイルの削除に失敗: {cleanup_error}")
        raise


def run_batch_encoding(
    bucket: str,
    pending_files_path: Path,
    parallel: int,
    gop_size: int,
    svtav1_args: list[str],
    ffmpeg_args: list[str] | None = None
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
        # 処理対象ファイルをファイルから読み込む
        logger.info(f"処理対象ファイルを読み込み: {pending_files_path}")
        try:
            with open(pending_files_path, 'r', encoding='utf-8') as f:
                pending_files = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"ファイルが見つかりません: {pending_files_path}")
            return 1
        except Exception as e:
            logger.error(f"ファイルの読み込みに失敗: {e}")
            return 1

        if not pending_files:
            logger.info("すべてのファイルが処理済みです")
            return 0

        # ファイルを順次処理
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
                download_future
            )

            # 次のイテレーションのために保存
            download_future = next_download_future

            logger.info(f"完了: {input_file_name}")

        logger.info("")
        logger.info("すべての処理が完了しました")

        return 0

    except Exception as e:
        logger.error(f"処理中にエラーが発生: {e}", exc_info=True)
        return 1

    finally:
        s3.shutdown()
