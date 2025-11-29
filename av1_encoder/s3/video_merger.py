"""動画・音声結合モジュール

エンコードされた動画セグメントと元の音声を結合する。
"""
import logging
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def merge_video_with_audio(
    workspace: Path,
    input_file: Path,
    output_file: Path,
    audio_args: list[str] | None = None
) -> None:
    """エンコードされた動画と元の音声を結合

    Args:
        workspace: ワークスペースディレクトリ
        input_file: 元の入力ファイル（音声ソース）
        output_file: 出力ファイル
        audio_args: 音声引数（展開済み、例: ['-c:a', 'aac', '-b:a', '128k']）
                   指定がない場合は ['-c:a', 'copy'] を使用
    """
    concat_file = workspace / "concat.txt"

    if not concat_file.exists():
        raise FileNotFoundError(f"concat.txtが見つかりません: {concat_file}")

    logger.info("結合中...")

    # 基本的なコマンド構築
    cmd = [
        'ffmpeg',
        '-f', 'concat',
        '-safe', '0',
        '-i', str(concat_file),
        '-i', str(input_file),
        '-map', '0:v:0',
        '-map', '1:a?',
        '-c:v', 'copy',
    ]

    # 音声引数の追加
    if audio_args:
        cmd.extend(audio_args)
    else:
        # デフォルトは音声をコピー
        cmd.extend(['-c:a', 'copy'])

    # 出力ファイルを追加
    cmd.append(str(output_file))

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
