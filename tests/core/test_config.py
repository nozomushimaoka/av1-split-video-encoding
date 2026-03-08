"""Tests for the EncodingConfig dataclass"""

from pathlib import Path
import pytest

from av1_encoder.core.config import EncodingConfig


class TestEncodingConfig:
    """Tests for the EncodingConfig dataclass"""

    def test_create_config(self, tmp_path):
        """Test that EncodingConfig is created correctly"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=120,
            svtav1_args=['--crf', '30', '--preset', '6']
        )

        assert config.input_file == input_file
        assert config.workspace_dir == workspace_dir
        assert config.parallel_jobs == 4
        assert config.segment_length == 120
        assert config.svtav1_args == ['--crf', '30', '--preset', '6']

    def test_config_default_values(self, tmp_path):
        """Test that EncodingConfig default values are correct"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240        )

        assert config.svtav1_args == []  # default value
        assert config.segment_length == 60  # default value

    def test_set_svtav1_args_to_empty_list(self, tmp_path):
        """Test that svtav1_args can be explicitly set to an empty list"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            svtav1_args=[]
        )

        assert config.svtav1_args == []

    def test_path_type_fields_are_correct(self, tmp_path):
        """Test that path-type fields in EncodingConfig are Path objects"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240        )

        assert isinstance(config.input_file, Path)
        assert isinstance(config.workspace_dir, Path)

    def test_integer_type_fields_are_correct(self, tmp_path):
        """Test that integer-type fields are correct"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=8,
            gop_size=240,
            segment_length=120
        )

        assert isinstance(config.parallel_jobs, int)
        assert isinstance(config.segment_length, int)
        assert config.parallel_jobs == 8
        assert config.segment_length == 120

    def test_set_different_svtav1_args(self, tmp_path):
        """Test that different svtav1_args can be set"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        svtav1_args1 = ['--crf', '25']
        svtav1_args2 = ['--crf', '30', '--preset', '5', '--pix_fmt', 'yuv420p10le']

        config1 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            svtav1_args=svtav1_args1
        )

        config2 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            svtav1_args=svtav1_args2
        )

        assert config1.svtav1_args == svtav1_args1
        assert config2.svtav1_args == svtav1_args2
        assert config1.svtav1_args != config2.svtav1_args

    def test_multiple_config_instances_are_independent(self, tmp_path):
        """Test that multiple EncodingConfig instances are independent of each other"""
        input_file1 = tmp_path / "input1.mkv"
        input_file1.touch()
        input_file2 = tmp_path / "input2.mkv"
        input_file2.touch()
        workspace_dir1 = tmp_path / "workspace1"
        workspace_dir1.mkdir()
        workspace_dir2 = tmp_path / "workspace2"
        workspace_dir2.mkdir()

        config1 = EncodingConfig(
            input_file=input_file1,
            workspace_dir=workspace_dir1,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            svtav1_args=['--crf', '30']
        )

        config2 = EncodingConfig(
            input_file=input_file2,
            workspace_dir=workspace_dir2,
            parallel_jobs=8,
            gop_size=240,
            segment_length=120,
            svtav1_args=['--crf', '25']
        )

        # Verify modifying config1 does not affect config2
        assert config1.input_file != config2.input_file
        assert config1.workspace_dir != config2.workspace_dir
        assert config1.parallel_jobs != config2.parallel_jobs
        assert config1.segment_length != config2.segment_length
        assert config1.svtav1_args != config2.svtav1_args

    def test_set_custom_value_for_segment_length(self, tmp_path):
        """Test that a custom value can be set for segment_length"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=10  # custom value
        )

        assert config.segment_length == 10

    def test_set_various_values_for_parallel_jobs(self, tmp_path):
        """Test that various values can be set for parallel_jobs"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        # 1 thread
        config1 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=1,
            gop_size=240        )
        assert config1.parallel_jobs == 1

        # Many threads
        config2 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=32,
            gop_size=240        )
        assert config2.parallel_jobs == 32

    def test_set_complex_options_for_svtav1_args(self, tmp_path):
        """Test that complex options can be set for svtav1_args"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        complex_args = [
            '--crf', '30',
            '--preset', '6',
            '--pix_fmt', 'yuv420p10le',
            '--svtav1-params', 'tune=0:enable-qm=1:qm-min=0'
        ]

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            svtav1_args=complex_args
        )

        assert config.svtav1_args == complex_args
        assert len(config.svtav1_args) == 8

    def test_convert_string_path_to_path_object(self, tmp_path):
        """Test converting a string path to a Path object"""
        input_file_str = str(tmp_path / "input.mkv")
        workspace_dir_str = str(tmp_path / "workspace")

        # Create with strings
        config = EncodingConfig(
            input_file=Path(input_file_str),
            workspace_dir=Path(workspace_dir_str),
            parallel_jobs=4,
            gop_size=240        )

        # Verify they are Path objects
        assert isinstance(config.input_file, Path)
        assert isinstance(config.workspace_dir, Path)
        assert str(config.input_file) == input_file_str
        assert str(config.workspace_dir) == workspace_dir_str

    def test_gop_size_is_set(self, tmp_path):
        """Test that gop_size is set correctly"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240
        )

        assert config.gop_size == 240

    def test_set_custom_value_for_gop_size(self, tmp_path):
        """Test that a custom value can be set for gop_size"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=120
        )

        assert config.gop_size == 120
