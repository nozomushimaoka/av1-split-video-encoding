"""ワークスペース管理"""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class Workspace:
    work_dir: Path
    log_file: Path
    concat_file: Path
    output_file: Path

    def prepare_directory(self) -> None:
        # 親ディレクトリは既に存在している前提
        # フラット構造なのでサブディレクトリの作成は不要
        pass


def make_workspace_from_path(workspace_dir: Path, input_file: Path) -> Workspace:
    """既存の作業ディレクトリからWorkspaceオブジェクトを構築

    Args:
        workspace_dir: 既存の作業ディレクトリパス
        input_file: 入力ファイルパス（出力ファイル名の生成に使用）

    Returns:
        Workspace: ワークスペースオブジェクト
    """
    input_basename = input_file.stem

    return Workspace(
        work_dir=workspace_dir,
        log_file=workspace_dir / "main.log",
        concat_file=workspace_dir / "concat.txt",
        output_file=workspace_dir / "output.mkv",
    )
