"""EncodingConfig データクラスのテスト"""

from pathlib import Path
import pytest

from av1_encoder.core.config import EncodingConfig


class TestEncodingConfig:
    """EncodingConfigデータクラスのテスト"""

    def test_configを作成(self, tmp_path):
        """EncodingConfigが正しく作成されることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=120,
            extra_args=['-crf', '30', '-preset', '6', '-g', '240']
        )

        assert config.input_file == input_file
        assert config.workspace_dir == workspace_dir
        assert config.parallel_jobs == 4
        assert config.segment_length == 120
        assert config.extra_args == ['-crf', '30', '-preset', '6', '-g', '240']

    def test_configのデフォルト値(self, tmp_path):
        """EncodingConfigのデフォルト値が正しいことをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240        )

        assert config.extra_args == []  # デフォルト値
        assert config.segment_length == 60  # デフォルト値

    def test_configのextra_argsを空リストに設定(self, tmp_path):
        """extra_argsを明示的に空リストに設定できることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            extra_args=[]
        )

        assert config.extra_args == []

    def test_configのパス型が正しい(self, tmp_path):
        """EncodingConfigのパス型フィールドがPathオブジェクトであることをテスト"""
        input_file = tmp_path / "input.mp4"
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

    def test_整数型フィールドが正しい(self, tmp_path):
        """整数型フィールドが正しいことをテスト"""
        input_file = tmp_path / "input.mp4"
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

    def test_異なるextra_argsを設定(self, tmp_path):
        """異なるextra_argsを設定できることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        extra_args1 = ['-crf', '25']
        extra_args2 = ['-crf', '30', '-preset', '5', '-pix_fmt', 'yuv420p10le']

        config1 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            extra_args=extra_args1
        )

        config2 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            extra_args=extra_args2
        )

        assert config1.extra_args == extra_args1
        assert config2.extra_args == extra_args2
        assert config1.extra_args != config2.extra_args

    def test_複数のconfigインスタンスが独立している(self, tmp_path):
        """複数のEncodingConfigインスタンスが互いに独立していることをテスト"""
        input_file1 = tmp_path / "input1.mp4"
        input_file1.touch()
        input_file2 = tmp_path / "input2.mp4"
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
            extra_args=['-crf', '30']
        )

        config2 = EncodingConfig(
            input_file=input_file2,
            workspace_dir=workspace_dir2,
            parallel_jobs=8,
            gop_size=240,
            segment_length=120,
            extra_args=['-crf', '25']
        )

        # config1を変更してもconfig2に影響しないことを確認
        assert config1.input_file != config2.input_file
        assert config1.workspace_dir != config2.workspace_dir
        assert config1.parallel_jobs != config2.parallel_jobs
        assert config1.segment_length != config2.segment_length
        assert config1.extra_args != config2.extra_args

    def test_segment_lengthにカスタム値を設定(self, tmp_path):
        """segment_lengthにカスタム値を設定できることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=10  # カスタム値
        )

        assert config.segment_length == 10

    def test_parallel_jobsに様々な値を設定(self, tmp_path):
        """parallel_jobsに様々な値を設定できることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        # 1スレッド
        config1 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=1,
            gop_size=240        )
        assert config1.parallel_jobs == 1

        # 多数のスレッド
        config2 = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=32,
            gop_size=240        )
        assert config2.parallel_jobs == 32

    def test_extra_argsに複雑なオプションを設定(self, tmp_path):
        """extra_argsに複雑なオプションを設定できることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()

        complex_args = [
            '-crf', '30',
            '-preset', '6',
            '-pix_fmt', 'yuv420p10le',
            '-svtav1-params', 'tune=0:enable-qm=1:qm-min=0',
            '-g', '240',
            '-keyint_min', '240'
        ]

        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            extra_args=complex_args
        )

        assert config.extra_args == complex_args
        assert len(config.extra_args) == 12

    def test_文字列パスからPathオブジェクトへの変換(self, tmp_path):
        """文字列パスからPathオブジェクトへの変換をテスト"""
        input_file_str = str(tmp_path / "input.mp4")
        workspace_dir_str = str(tmp_path / "workspace")

        # 文字列で作成
        config = EncodingConfig(
            input_file=Path(input_file_str),
            workspace_dir=Path(workspace_dir_str),
            parallel_jobs=4,
            gop_size=240        )

        # Pathオブジェクトであることを確認
        assert isinstance(config.input_file, Path)
        assert isinstance(config.workspace_dir, Path)
        assert str(config.input_file) == input_file_str
        assert str(config.workspace_dir) == workspace_dir_str
    def test_gop_sizeが設定される(self, tmp_path):
        """gop_sizeが正しく設定されることをテスト"""
        input_file = tmp_path / "input.mp4"
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

    def test_gop_sizeにカスタム値を設定(self, tmp_path):
        """gop_sizeにカスタム値を設定できることをテスト"""
        input_file = tmp_path / "input.mp4"
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
