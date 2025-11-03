from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class EncodingConfig:
    input_file: Path
    workspace_dir: Path
    parallel_jobs: int
    extra_args: List[str] = field(default_factory=list)
    segment_length: int = 60  # 秒
