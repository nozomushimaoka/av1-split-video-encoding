"""ワークスペース管理"""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Workspace:
    work_dir: Path
    log_file: Path
    concat_file: Path
    output_file: Path


def make_workspace_from_path(workspace_dir: Path) -> Workspace:
    return Workspace(
        work_dir=workspace_dir,
        log_file=workspace_dir / "main.log",
        concat_file=workspace_dir / "concat.txt",
        output_file=workspace_dir / "output.mkv",
    )
