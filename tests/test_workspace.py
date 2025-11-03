from datetime import datetime
from pathlib import Path
import pytest

from av1_encoder.workspace import Workspace, make_workspace_from_path


@pytest.fixture
def sample_timestamp():
    """テスト用のタイムスタンプを作成するフィクスチャ"""
    return datetime(2025, 10, 23, 14, 30, 45)


@pytest.fixture
def workspace(tmp_path):
    """テスト用のWorkspaceインスタンスを作成するフィクスチャ"""
    work_dir = tmp_path / "test_workspace"

    return Workspace(
        work_dir=work_dir,
        log_file=work_dir / "main.log",
        concat_file=work_dir / "concat.txt",
        output_file=work_dir / "output.mkv"
    )


class TestWorkspaceデータクラス:
    """Workspaceデータクラスのテスト"""

    def test_workspaceを作成(self, tmp_path):
        """Workspaceが正しく作成されることをテスト"""
        work_dir = tmp_path / "workspace"
        log_file = work_dir / "main.log"
        concat_file = work_dir / "concat.txt"
        output_file = work_dir / "output.mkv"

        workspace = Workspace(
            work_dir=work_dir,
            log_file=log_file,
            concat_file=concat_file,
            output_file=output_file
        )

        assert workspace.work_dir == work_dir
        assert workspace.log_file == log_file
        assert workspace.concat_file == concat_file
        assert workspace.output_file == output_file

    def test_workspaceの属性が正しいPath型(self, workspace):
        """Workspaceの各属性がPathオブジェクトであることをテスト"""
        assert isinstance(workspace.work_dir, Path)
        assert isinstance(workspace.log_file, Path)
        assert isinstance(workspace.concat_file, Path)
        assert isinstance(workspace.output_file, Path)


class TestMakeWorkspaceFromPath:
    """make_workspace_from_path関数のテスト"""

    def test_workspaceを生成(self, tmp_path):
        """make_workspace_from_pathが正しいWorkspaceを生成することをテスト"""
        workspace_dir = tmp_path / "my_workspace"
        workspace_dir.mkdir()

        workspace = make_workspace_from_path(workspace_dir)

        # 正しいワークディレクトリパスが設定されることを確認
        assert workspace.work_dir == workspace_dir

        # フラット構造のファイルパスが正しいことを確認
        assert workspace.log_file == workspace_dir / "main.log"
        assert workspace.concat_file == workspace_dir / "concat.txt"
        assert workspace.output_file == workspace_dir / "output.mkv"

    def test_workspaceの出力ファイル名は常にoutput_mkv(self, tmp_path):
        """出力ファイル名が常にoutput.mkvであることをテスト"""
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        workspace = make_workspace_from_path(workspace_dir)

        assert workspace.output_file.name == "output.mkv"
        assert workspace.output_file.parent == workspace_dir

    def test_異なるワークスペースディレクトリで異なるワークスペース(self, tmp_path):
        """異なるワークスペースディレクトリで異なるワークスペースが生成されることをテスト"""
        workspace_dir1 = tmp_path / "workspace1"
        workspace_dir1.mkdir()
        workspace_dir2 = tmp_path / "workspace2"
        workspace_dir2.mkdir()

        workspace1 = make_workspace_from_path(workspace_dir1)
        workspace2 = make_workspace_from_path(workspace_dir2)

        # 異なるワークディレクトリが設定されることを確認
        assert workspace1.work_dir != workspace2.work_dir
        assert workspace1.work_dir == workspace_dir1
        assert workspace2.work_dir == workspace_dir2

    def test_入力ファイル名は出力ファイル名に影響しない(self, tmp_path):
        """入力ファイル名が異なっても出力ファイル名は常にoutput.mkvであることをテスト"""
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        workspace1 = make_workspace_from_path(workspace_dir)
        workspace2 = make_workspace_from_path(workspace_dir)

        # どちらも出力ファイル名はoutput.mkvになる
        assert workspace1.output_file.name == "output.mkv"
        assert workspace2.output_file.name == "output.mkv"
