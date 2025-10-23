"""ワークスペース管理"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class Workspace:
    work_dir: Path
    segments_dir: Path
    logs_dir: Path
    local_input_file: Path
    local_output_file: Path
    concat_file: Path
    log_file: Path

def prepare_workspace(input_file: Path, timestamp: datetime):
    # ファイル名から作業ディレクトリ名を生成
    input_basename = input_file.stem
    timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
    work_dir = Path(f"encode_{input_basename}_{timestamp_str}")
    work_dir.mkdir(exist_ok=True)

    # サブディレクトリ作成
    segments_dir = work_dir / "segments"
    logs_dir = work_dir / "logs"
    segments_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    return Workspace(
        work_dir=work_dir,
        segments_dir=segments_dir,
        logs_dir=logs_dir,
        local_input_file=input_file,
        local_output_file=work_dir / input_basename,
        concat_file=work_dir / "concat.txt",
        log_file=work_dir / "encode.log"
    )
