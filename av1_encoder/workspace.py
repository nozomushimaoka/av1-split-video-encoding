"""ワークスペース管理"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class Workspace:
    work_dir: Path
    segments_dir: Path
    logs_dir: Path
    log_file: Path
    concat_file: Path
    output_file: Path

    def prepare_directory(self) -> None:
        self.work_dir.mkdir(exist_ok=True)
        # サブディレクトリ作成
        self.segments_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)


def make_workspace(input_file: Path, timestamp: datetime) -> Workspace:
    input_basename = input_file.stem
    work_dir = _generate_workspace_path(input_file.stem, timestamp)

    segments_dir = work_dir / "segments"
    logs_dir = work_dir / "logs"

    return Workspace(
        work_dir=work_dir,
        segments_dir=segments_dir,
        logs_dir=logs_dir,
        log_file=logs_dir / "main.log",
        concat_file=work_dir / "concat.txt",
        output_file=work_dir / f"{input_basename}.mkv",
    )


def _generate_workspace_path(input_basename: str, timestamp: datetime) -> Path:
    # 作業ディレクトリパスを生成
    timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
    return Path(f"encode_{input_basename}_{timestamp_str}")
