from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from av1_encoder.encoding.cli import main
from av1_encoder.core.config import EncodingConfig


class TestCLIArgParsing:
    """Tests for CLI argument parsing"""

    def test_parse_minimum_args(self, tmp_path):
        """Test argument parsing with only the input file specified"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            # Verify EncodingOrchestrator was called with the correct configuration
            assert mock_orchestrator_class.call_count == 1
            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('input.mkv')
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 4
            assert config.svtav1_args == ['--crf', '30']

            # Verify run was called
            mock_orchestrator.run.assert_called_once()

            # Verify success code is returned
            assert result == 0

    def test_parse_all_args(self, tmp_path):
        """Test argument parsing with all arguments specified"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'video.mkv',
            str(workspace),
            '--parallel', '8', '--gop', '240',
            '--svtav1-params', 'crf=30,preset=6,g=240'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            # Verify EncodingOrchestrator was called with the correct configuration
            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('video.mkv')
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 8
            # Expanded form from CLI
            assert config.svtav1_args == ['--crf', '30', '--preset', '6', '--g', '240']

            assert result == 0

    def test_specify_parallel_with_short_option(self, tmp_path):
        """Test that parallel_jobs can be specified with the short option -l"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '-l', '16', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.parallel_jobs == 16
            assert result == 0

    def test_parse_with_only_some_options(self, tmp_path):
        """Test parsing with only some options specified"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'test.mkv',
            str(workspace),
            '--parallel', '4', '--gop', '240',
            '--svtav1-params', 'crf=25'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('test.mkv')
            assert config.parallel_jobs == 4
            # Expanded form from CLI
            assert config.svtav1_args == ['--crf', '25']
            assert result == 0


class TestCLIMainFunction:
    """Tests for the CLI main function"""

    def test_returns_0_on_encode_success(self, tmp_path):
        """Test that 0 is returned when encoding succeeds"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            assert result == 0

    def test_returns_1_on_encode_failure(self, tmp_path):
        """Test that 1 is returned when encoding fails"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = RuntimeError("encoding error")

            result = main()

            assert result == 1

    def test_returns_1_on_any_exception(self, tmp_path):
        """Test that 1 is returned when any exception occurs"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = Exception("unexpected error")

            result = main()

            assert result == 1

    def test_orchestrator_run_is_called(self, tmp_path):
        """Test that orchestrator.run() is called"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            mock_orchestrator.run.assert_called_once()


class TestCLIEncodingConfigCreation:
    """Tests for creating EncodingConfig from CLI"""

    def test_encoding_config_created_correctly(self, tmp_path):
        """Test that EncodingConfig is created correctly from CLI arguments"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'my_video.mkv',
            str(workspace),
            '--parallel', '12', '--gop', '240',
            '--svtav1-params', 'crf=28,preset=5,g=120'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            # Verify EncodingConfig instance was created
            assert mock_orchestrator_class.call_count == 1
            config = mock_orchestrator_class.call_args[0][0]

            assert isinstance(config, EncodingConfig)
            assert config.input_file == Path('my_video.mkv')
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 12
            # Expanded form from CLI
            assert config.svtav1_args == ['--crf', '28', '--preset', '5', '--g', '120']

    def test_input_file_converted_to_path_object(self, tmp_path):
        """Test that the input_file argument is converted to a Path object"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'test/path/to/video.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert isinstance(config.input_file, Path)
            assert config.input_file == Path('test/path/to/video.mkv')


class TestCLIDefaultValues:
    """Tests for CLI default values"""

    def test_parallel_default_is_none(self, tmp_path):
        """Test that the default value of parallel is None"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.parallel_jobs == 4

    def test_svtav1_params_is_required(self, tmp_path):
        """Test that -svtav1-params is a required parameter"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()


class TestCLIArgparseBehavior:
    """Tests for argparse behavior"""

    def test_system_exit_with_no_args(self):
        """Test that SystemExit is raised when no arguments are provided"""
        test_args = ['prog']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_system_exit_with_string_for_integer_arg(self, tmp_path):
        """Test that SystemExit is raised when a string is given for an integer argument"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', 'not-a-number', '--gop', '240']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()


class TestCLIArgTypes:
    """Tests for CLI argument types"""

    def test_integer_args_converted_correctly(self, tmp_path):
        """Test that integer arguments are converted correctly"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'input.mkv',
            str(workspace),
            '--parallel', '10', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]

            assert isinstance(config.parallel_jobs, int)
            assert config.parallel_jobs == 10

    def test_string_args_handled_correctly(self, tmp_path):
        """Test that string arguments are handled correctly"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'path/to/input.mkv',
            str(workspace),
            '--parallel', '4', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert isinstance(config.input_file, Path)
            assert config.input_file == Path('path/to/input.mkv')


class TestCLIEdgeCases:
    """Tests for CLI edge cases"""

    def test_negative_value_args(self, tmp_path):
        """Test that negative value arguments are handled correctly"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '-1', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            # argparse allows negative values, so -1 should be set
            assert config.parallel_jobs == -1

    def test_svtav1_args_handled_correctly(self, tmp_path):
        """Test that svtav1_args is handled correctly"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mkv', str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=0,preset=0']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            # Expanded form from CLI
            assert config.svtav1_args == ['--crf', '0', '--preset', '0']

    def test_very_long_file_path(self, tmp_path):
        """Test that very long file paths are handled correctly"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        long_path = 'a' * 200 + '.mkv'
        test_args = ['prog', long_path, str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == Path(long_path)

    def test_filename_with_special_chars(self, tmp_path):
        """Test that filenames with special characters are handled correctly"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        special_filename = 'test-file_123 (copy).mkv'
        test_args = ['prog', special_filename, str(workspace), '--parallel', '4', '--gop', '240', '--svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == Path(special_filename)
