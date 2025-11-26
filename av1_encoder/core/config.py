from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class EncodingConfig:
    input_file: Path
    workspace_dir: Path
    parallel_jobs: int
    gop_size: int  # GOP サイズ（必須）
    svtav1_args: List[str] = field(default_factory=list)  # SvtAv1EncApp用パラメータ
    ffmpeg_args: List[str] = field(default_factory=list)  # FFmpeg用パラメータ
    audio_args: List[str] = field(default_factory=list)  # 音声引数（展開済み、例: ['-c:a', 'aac', '-b:a', '128k']）
    segment_length: int = 60  # 秒
