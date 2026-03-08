from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class EncodingConfig:
    input_file: Path
    workspace_dir: Path
    parallel_jobs: int
    gop_size: int  # GOP size (required)
    svtav1_args: List[str] = field(default_factory=list)  # SvtAv1EncApp parameters
    ffmpeg_args: List[str] = field(default_factory=list)  # FFmpeg parameters
    audio_args: List[str] = field(default_factory=list)  # Audio args (expanded, e.g. ['-c:a', 'aac', '-b:a', '128k'])
    segment_length: int = 60  # seconds
    cuda_decode: bool = False  # Whether to use CUDA hardware decoding
    hardware_decode: Optional[str] = None         # e.g. 'cuda', 'vaapi', 'qsv'
    hardware_decode_device: Optional[str] = None  # e.g. '/dev/dri/renderD128'
