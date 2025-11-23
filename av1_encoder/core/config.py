from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class EncodingConfig:
    input_file: Path
    workspace_dir: Path
    parallel_jobs: int
    gop_size: int  # GOP サイズ（必須）
    svtav1_args: List[str] = field(default_factory=list)  # SvtAv1EncApp用パラメータ
    ffmpeg_args: List[str] = field(default_factory=list)  # FFmpeg用パラメータ
    segment_length: int = 60  # 秒
