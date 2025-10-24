from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class EncodingConfig:
    input_file: Path
    s3_bucket: str
    parallel_jobs: int
    segment_length: int = 60  # 秒
    extra_args: List[str] = field(default_factory=list)  # 追加FFmpegオプション (例: ['-crf', '30', '-preset', '6', '-pix_fmt', 'yuv420p10le'])
