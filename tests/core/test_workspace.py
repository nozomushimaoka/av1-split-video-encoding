from datetime import datetime
from pathlib import Path
import pytest

from av1_encoder.core.workspace import Workspace, make_workspace_from_path


@pytest.fixture
def sample_timestamp():
    """Fixture that creates a sample timestamp for tests"""
    return datetime(2025, 10, 23, 14, 30, 45)


@pytest.fixture
def workspace(tmp_path):
    """Fixture that creates a Workspace instance for tests"""
    work_dir = tmp_path / "test_workspace"

    return Workspace(
        work_dir=work_dir,
        log_file=work_dir / "main.log",
        concat_file=work_dir / "concat.txt",
        output_file=work_dir / "output.mkv"
    )


class TestWorkspaceDataclass:
    """Tests for the Workspace dataclass"""

    def test_create_workspace(self, tmp_path):
        """Test that a Workspace is created correctly"""
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

    def test_workspace_attributes_are_path_type(self, workspace):
        """Test that each Workspace attribute is a Path object"""
        assert isinstance(workspace.work_dir, Path)
        assert isinstance(workspace.log_file, Path)
        assert isinstance(workspace.concat_file, Path)
        assert isinstance(workspace.output_file, Path)


class TestMakeWorkspaceFromPath:
    """Tests for the make_workspace_from_path function"""

    def test_create_workspace(self, tmp_path):
        """Test that make_workspace_from_path produces a correct Workspace"""
        workspace_dir = tmp_path / "my_workspace"
        workspace_dir.mkdir()

        workspace = make_workspace_from_path(workspace_dir)

        # Verify the correct work directory is set
        assert workspace.work_dir == workspace_dir

        # Verify flat file paths are correct
        assert workspace.log_file == workspace_dir / "main.log"
        assert workspace.concat_file == workspace_dir / "concat.txt"
        assert workspace.output_file == workspace_dir / "output.mkv"

    def test_output_filename_is_always_output_mkv(self, tmp_path):
        """Test that the output filename is always output.mkv"""
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        workspace = make_workspace_from_path(workspace_dir)

        assert workspace.output_file.name == "output.mkv"
        assert workspace.output_file.parent == workspace_dir

    def test_different_workspace_dirs_produce_different_workspaces(self, tmp_path):
        """Test that different workspace directories produce different Workspace objects"""
        workspace_dir1 = tmp_path / "workspace1"
        workspace_dir1.mkdir()
        workspace_dir2 = tmp_path / "workspace2"
        workspace_dir2.mkdir()

        workspace1 = make_workspace_from_path(workspace_dir1)
        workspace2 = make_workspace_from_path(workspace_dir2)

        # Verify different work directories are set
        assert workspace1.work_dir != workspace2.work_dir
        assert workspace1.work_dir == workspace_dir1
        assert workspace2.work_dir == workspace_dir2

    def test_input_filename_does_not_affect_output_filename(self, tmp_path):
        """Test that the output filename is always output.mkv regardless of input"""
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        workspace1 = make_workspace_from_path(workspace_dir)
        workspace2 = make_workspace_from_path(workspace_dir)

        # Both should have output.mkv as the output filename
        assert workspace1.output_file.name == "output.mkv"
        assert workspace2.output_file.name == "output.mkv"
