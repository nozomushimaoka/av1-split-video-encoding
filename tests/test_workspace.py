from datetime import datetime
from pathlib import Path
import pytest

from av1_encoder.workspace import Workspace, make_workspace, _generate_workspace_path


@pytest.fixture
def sample_timestamp():
    """テスト用のタイムスタンプを作成するフィクスチャ"""
    return datetime(2025, 10, 23, 14, 30, 45)


@pytest.fixture
def workspace(tmp_path):
    """テスト用のWorkspaceインスタンスを作成するフィクスチャ"""
    work_dir = tmp_path / "test_workspace"
    segments_dir = work_dir / "segments"
    logs_dir = work_dir / "logs"

    return Workspace(
        work_dir=work_dir,
        segments_dir=segments_dir,
        logs_dir=logs_dir,
        log_file=logs_dir / "main.log",
        concat_file=work_dir / "concat.txt",
        output_file=work_dir / "output.mkv"
    )


class TestWorkspaceデータクラス:
    """Workspaceデータクラスのテスト"""

    def test_workspaceを作成(self, tmp_path):
        """Workspaceが正しく作成されることをテスト"""
        work_dir = tmp_path / "workspace"
        segments_dir = work_dir / "segments"
        logs_dir = work_dir / "logs"
        log_file = logs_dir / "main.log"
        concat_file = work_dir / "concat.txt"
        output_file = work_dir / "output.mkv"

        workspace = Workspace(
            work_dir=work_dir,
            segments_dir=segments_dir,
            logs_dir=logs_dir,
            log_file=log_file,
            concat_file=concat_file,
            output_file=output_file
        )

        assert workspace.work_dir == work_dir
        assert workspace.segments_dir == segments_dir
        assert workspace.logs_dir == logs_dir
        assert workspace.log_file == log_file
        assert workspace.concat_file == concat_file
        assert workspace.output_file == output_file

    def test_workspaceの属性が正しいPath型(self, workspace):
        """Workspaceの各属性がPathオブジェクトであることをテスト"""
        assert isinstance(workspace.work_dir, Path)
        assert isinstance(workspace.segments_dir, Path)
        assert isinstance(workspace.logs_dir, Path)
        assert isinstance(workspace.log_file, Path)
        assert isinstance(workspace.concat_file, Path)
        assert isinstance(workspace.output_file, Path)


class TestWorkspaceのprepare_directory:
    """Workspaceのprepare_directoryメソッドのテスト"""

    def test_ディレクトリを作成(self, workspace):
        """prepare_directoryが必要なディレクトリを作成することをテスト"""
        # 事前にディレクトリが存在しないことを確認
        assert not workspace.work_dir.exists()
        assert not workspace.segments_dir.exists()
        assert not workspace.logs_dir.exists()

        workspace.prepare_directory()

        # ディレクトリが作成されたことを確認
        assert workspace.work_dir.exists()
        assert workspace.work_dir.is_dir()
        assert workspace.segments_dir.exists()
        assert workspace.segments_dir.is_dir()
        assert workspace.logs_dir.exists()
        assert workspace.logs_dir.is_dir()

    def test_既存のディレクトリに対してエラーが出ない(self, workspace):
        """既存のディレクトリに対してprepare_directoryを呼んでもエラーが出ないことをテスト"""
        # 最初の呼び出し
        workspace.prepare_directory()
        assert workspace.work_dir.exists()

        # 2回目の呼び出し（エラーが出ないことを確認）
        workspace.prepare_directory()
        assert workspace.work_dir.exists()
        assert workspace.segments_dir.exists()
        assert workspace.logs_dir.exists()

    def test_ファイルを含むディレクトリでprepare_directoryを再実行(self, workspace):
        """ファイルを含むディレクトリでprepare_directoryを再実行してもエラーが出ないことをテスト"""
        workspace.prepare_directory()

        # ディレクトリにファイルを作成
        test_file = workspace.segments_dir / "test.txt"
        test_file.write_text("test content")

        # 再度prepare_directoryを呼び出し
        workspace.prepare_directory()

        # ディレクトリとファイルが存在することを確認
        assert workspace.work_dir.exists()
        assert test_file.exists()
        assert test_file.read_text() == "test content"


class TestMakeWorkspace:
    """make_workspace関数のテスト"""

    def test_workspaceを生成(self, tmp_path, sample_timestamp):
        """make_workspaceが正しいWorkspaceを生成することをテスト"""
        input_file = tmp_path / "input_video.mp4"
        input_file.touch()

        workspace = make_workspace(input_file, sample_timestamp)

        # 正しいワークディレクトリ名が生成されることを確認
        expected_work_dir_name = "encode_input_video_20251023_143045"
        assert workspace.work_dir.name == expected_work_dir_name

        # サブディレクトリパスが正しいことを確認
        assert workspace.segments_dir == workspace.work_dir / "segments"
        assert workspace.logs_dir == workspace.work_dir / "logs"
        assert workspace.log_file == workspace.logs_dir / "main.log"
        assert workspace.concat_file == workspace.work_dir / "concat.txt"
        assert workspace.output_file == workspace.work_dir / "input_video.mkv"

    def test_workspaceの出力ファイル名が入力ファイル名に基づく(self, tmp_path, sample_timestamp):
        """出力ファイル名が入力ファイルのベース名に基づくことをテスト"""
        input_file = tmp_path / "my_movie.mp4"
        input_file.touch()

        workspace = make_workspace(input_file, sample_timestamp)

        assert workspace.output_file.name == "my_movie.mkv"
        assert workspace.output_file.parent == workspace.work_dir

    def test_異なるタイムスタンプで異なるワークスペース(self, tmp_path):
        """異なるタイムスタンプで異なるワークスペースが生成されることをテスト"""
        input_file = tmp_path / "video.mp4"
        input_file.touch()

        timestamp1 = datetime(2025, 10, 23, 10, 0, 0)
        timestamp2 = datetime(2025, 10, 23, 11, 0, 0)

        workspace1 = make_workspace(input_file, timestamp1)
        workspace2 = make_workspace(input_file, timestamp2)

        # 異なるワークディレクトリが生成されることを確認
        assert workspace1.work_dir != workspace2.work_dir
        assert workspace1.work_dir.name == "encode_video_20251023_100000"
        assert workspace2.work_dir.name == "encode_video_20251023_110000"

    def test_入力ファイルの拡張子は無視される(self, tmp_path, sample_timestamp):
        """入力ファイルの拡張子が無視され、ステムのみが使用されることをテスト"""
        input_file = tmp_path / "video.avi"
        input_file.touch()

        workspace = make_workspace(input_file, sample_timestamp)

        # 拡張子がmkvに変更されることを確認
        assert workspace.output_file.name == "video.mkv"
        assert workspace.work_dir.name.startswith("encode_video_")


class TestGenerateWorkspacePath:
    """_generate_workspace_path関数のテスト"""

    def test_ワークスペースパスを生成(self, sample_timestamp):
        """_generate_workspace_pathが正しいパスを生成することをテスト"""
        input_basename = "test_video"

        result = _generate_workspace_path(input_basename, sample_timestamp)

        expected = Path("encode_test_video_20251023_143045")
        assert result == expected

    def test_タイムスタンプが正しくフォーマットされる(self):
        """タイムスタンプが正しい形式でフォーマットされることをテスト"""
        input_basename = "video"
        timestamp = datetime(2025, 1, 5, 9, 8, 7)

        result = _generate_workspace_path(input_basename, timestamp)

        # ゼロパディングが正しいことを確認
        assert result == Path("encode_video_20250105_090807")

    def test_特殊文字を含むベース名(self, sample_timestamp):
        """特殊文字を含むベース名でもパスを生成できることをテスト"""
        input_basename = "my-video_file"

        result = _generate_workspace_path(input_basename, sample_timestamp)

        assert result == Path("encode_my-video_file_20251023_143045")

    def test_返り値がPathオブジェクト(self, sample_timestamp):
        """_generate_workspace_pathがPathオブジェクトを返すことをテスト"""
        input_basename = "video"

        result = _generate_workspace_path(input_basename, sample_timestamp)

        assert isinstance(result, Path)

    def test_異なる年月日時分秒でユニークなパス(self):
        """異なる年月日時分秒で異なるパスが生成されることをテスト"""
        input_basename = "video"

        timestamp1 = datetime(2025, 10, 23, 14, 30, 45)
        timestamp2 = datetime(2025, 10, 23, 14, 30, 46)

        result1 = _generate_workspace_path(input_basename, timestamp1)
        result2 = _generate_workspace_path(input_basename, timestamp2)

        assert result1 != result2
        assert result1 == Path("encode_video_20251023_143045")
        assert result2 == Path("encode_video_20251023_143046")
