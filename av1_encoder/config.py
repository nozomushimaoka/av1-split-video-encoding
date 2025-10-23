from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class EncodingConfig:
    input_filename: str
    s3_bucket: str
    parallel_jobs: int
    crf: Optional[int] = None
    preset: Optional[int] = None
    keyint: Optional[int] = None
    segment_length: int = 60  # 秒

@dataclass
class SegmentInfo:
    index: int
    start_time: int
    duration: int
    is_final: bool
