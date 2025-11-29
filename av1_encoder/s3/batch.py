"""バッチエンコード処理のコア機能

このモジュールは後方互換性のためのエントリポイントを提供する。
実装は各サブモジュールに分割されている:
- video_merger: 動画・音声結合
- file_processor: 単一ファイル処理
- batch_orchestrator: バッチ処理調整
"""

# 後方互換性のためにすべての関数をエクスポート
from av1_encoder.s3.batch_orchestrator import run_batch_encoding
from av1_encoder.s3.file_processor import encode_video, process_single_file
from av1_encoder.s3.video_merger import merge_video_with_audio

__all__ = [
    'merge_video_with_audio',
    'encode_video',
    'process_single_file',
    'run_batch_encoding',
]
