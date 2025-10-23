"""設定とデータモデル"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class EncodingConfig:
    """エンコード設定"""
    input_filename: str
    parallel_jobs: int = 4
    crf: Optional[int] = None
    preset: Optional[str] = None
    keyint: Optional[int] = None
    s3_bucket: str = "xxx"
    segment_length: int = 60  # 秒


@dataclass
class WorkspaceConfig:
    """作業ディレクトリ設定"""
    work_dir: Path
    segments_dir: Path
    logs_dir: Path
    local_input_file: Path
    local_output_file: Path
    concat_file: Path
    log_file: Path


@dataclass
class SegmentInfo:
    """セグメント情報"""
    index: int
    start_time: int
    duration: int
    total_duration: float
