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


class TestWorkspaceのprepare_directory:
    """Workspaceのprepare_directoryメソッドのテスト"""

    def test_prepare_directoryはno_op(self, workspace):
        """prepare_directoryはno-opであることをテスト（フラット構造のため）"""
        # 親ディレクトリは手動で作成
        workspace.work_dir.mkdir(parents=True, exist_ok=True)

        # prepare_directoryを呼んでもエラーが出ないことを確認
        workspace.prepare_directory()

        # 作業ディレクトリは存在する
        assert workspace.work_dir.exists()
        assert workspace.work_dir.is_dir()

    def test_既存のディレクトリに対してエラーが出ない(self, workspace):
        """既存のディレクトリに対してprepare_directoryを呼んでもエラーが出ないことをテスト"""
        # 親ディレクトリを作成
        workspace.work_dir.mkdir(parents=True, exist_ok=True)

        # 最初の呼び出し
        workspace.prepare_directory()

        # 2回目の呼び出し（エラーが出ないことを確認）
        workspace.prepare_directory()
        assert workspace.work_dir.exists()

    def test_ファイルを含むディレクトリでprepare_directoryを再実行(self, workspace):
        """ファイルを含むディレクトリでprepare_directoryを再実行してもエラーが出ないことをテスト"""
        # 親ディレクトリを作成
        workspace.work_dir.mkdir(parents=True, exist_ok=True)
        workspace.prepare_directory()

        # ディレクトリにファイルを作成
        test_file = workspace.work_dir / "test.txt"
        test_file.write_text("test content")

        # 再度prepare_directoryを呼び出し
        workspace.prepare_directory()

        # ディレクトリとファイルが存在することを確認
        assert workspace.work_dir.exists()
        assert test_file.exists()
        assert test_file.read_text() == "test content"


class TestMakeWorkspaceFromPath:
    """make_workspace_from_path関数のテスト"""

    def test_workspaceを生成(self, tmp_path):
        """make_workspace_from_pathが正しいWorkspaceを生成することをテスト"""
        input_file = tmp_path / "input_video.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "my_workspace"
        workspace_dir.mkdir()

        workspace = make_workspace_from_path(workspace_dir, input_file)

        # 正しいワークディレクトリパスが設定されることを確認
        assert workspace.work_dir == workspace_dir

        # フラット構造のファイルパスが正しいことを確認
        assert workspace.log_file == workspace_dir / "main.log"
        assert workspace.concat_file == workspace_dir / "concat.txt"
        assert workspace.output_file == workspace_dir / "output.mkv"

    def test_workspaceの出力ファイル名は常にoutput_mkv(self, tmp_path):
        """出力ファイル名が常にoutput.mkvであることをテスト"""
        input_file = tmp_path / "my_movie.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        workspace = make_workspace_from_path(workspace_dir, input_file)

        assert workspace.output_file.name == "output.mkv"
        assert workspace.output_file.parent == workspace_dir

    def test_異なるワークスペースディレクトリで異なるワークスペース(self, tmp_path):
        """異なるワークスペースディレクトリで異なるワークスペースが生成されることをテスト"""
        input_file = tmp_path / "video.mp4"
        input_file.touch()

        workspace_dir1 = tmp_path / "workspace1"
        workspace_dir1.mkdir()
        workspace_dir2 = tmp_path / "workspace2"
        workspace_dir2.mkdir()

        workspace1 = make_workspace_from_path(workspace_dir1, input_file)
        workspace2 = make_workspace_from_path(workspace_dir2, input_file)

        # 異なるワークディレクトリが設定されることを確認
        assert workspace1.work_dir != workspace2.work_dir
        assert workspace1.work_dir == workspace_dir1
        assert workspace2.work_dir == workspace_dir2

    def test_入力ファイル名は出力ファイル名に影響しない(self, tmp_path):
        """入力ファイル名が異なっても出力ファイル名は常にoutput.mkvであることをテスト"""
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        input_file1 = tmp_path / "video.avi"
        input_file1.touch()
        input_file2 = tmp_path / "movie.mp4"
        input_file2.touch()

        workspace1 = make_workspace_from_path(workspace_dir, input_file1)
        workspace2 = make_workspace_from_path(workspace_dir, input_file2)

        # どちらも出力ファイル名はoutput.mkvになる
        assert workspace1.output_file.name == "output.mkv"
        assert workspace2.output_file.name == "output.mkv"
