"""Tests for S3 CLI"""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from av1_encoder.core.logging_config import setup_console_logger
from av1_encoder.s3.cli import main


class TestSetupConsoleLogger:
    """Tests for the setup_console_logger function (moved to logging config module)"""

    def test_logging_is_configured(self):
        """Test that logging is configured correctly"""
        # Reset the logger
        s3_logger = logging.getLogger('av1_encoder.s3')
        s3_logger.handlers.clear()

        setup_console_logger('av1_encoder.s3')

        # Verify a handler was added to the S3 logger
        assert len(s3_logger.handlers) == 1

        # Verify the handler is a StreamHandler
        handler = s3_logger.handlers[0]
        assert isinstance(handler, logging.StreamHandler)

        # Verify the log level is set to INFO
        assert s3_logger.level == logging.INFO

        # Verify propagation to parent logger is disabled
        assert s3_logger.propagate is False

    def test_skip_when_handler_already_exists(self):
        """Test that setup is skipped when a handler already exists"""
        # Reset the logger
        s3_logger = logging.getLogger('av1_encoder.s3')
        s3_logger.handlers.clear()

        # First call
        setup_console_logger('av1_encoder.s3')
        assert len(s3_logger.handlers) == 1

        # Second call (no handler should be added)
        setup_console_logger('av1_encoder.s3')
        assert len(s3_logger.handlers) == 1  # still 1


class TestMainCommandLineArgs:
    """Tests for CLI command-line arguments"""

    def test_specify_all_args(self, tmp_path):
        """Test specifying all arguments"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("s3://my-bucket/input/video1.mkv\n")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--output-dir', 's3://my-bucket/output/',
            '--workspace-base', str(workspace_base),
            '--parallel', '8', '--gop', '240',
            '--svtav1-params', 'crf=30,preset=5'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # Verify run_batch_encoding was called with the correct arguments
            mock_run.assert_called_once_with(
                pending_files_path=pending_files_path,
                output_dir='s3://my-bucket/output/',
                workspace_base=workspace_base,
                parallel=8,
                gop_size=240,
                # Expanded form from CLI
                svtav1_args=['--crf', '30', '--preset', '5'],
                ffmpeg_args=[],
                audio_args=[],
                hardware_decode=None,
                hardware_decode_device=None
            )
            assert result == 0

    def test_specify_audio_params(self, tmp_path):
        """Test specifying audio parameters"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("s3://my-bucket/input/video1.mkv\n")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--output-dir', 's3://my-bucket/output/',
            '--workspace-base', str(workspace_base),
            '--parallel', '8', '--gop', '240',
            '--svtav1-params', 'crf=30,preset=5',
            '--audio-params', 'c:a=aac,b:a=128k'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # Verify audio_args was expanded correctly
            mock_run.assert_called_once_with(
                pending_files_path=pending_files_path,
                output_dir='s3://my-bucket/output/',
                workspace_base=workspace_base,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5'],
                ffmpeg_args=[],
                audio_args=['-c:a', 'aac', '-b:a', '128k'],
                hardware_decode=None,
                hardware_decode_device=None
            )
            assert result == 0

    def test_use_default_values(self, tmp_path, monkeypatch):
        """Test using default values"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        # Change current directory to tmp_path
        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '10', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # Verify default values were used
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['output_dir'] == '.'
            assert call_kwargs['workspace_base'] == Path('.')
            assert result == 0

    def test_short_options(self, tmp_path):
        """Test that short options work correctly"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '-o', '/output/dir',
            '-b', str(workspace_base),
            '-l', '8',
            '-g', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # Verify short options were processed correctly
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['output_dir'] == '/output/dir'
            assert call_kwargs['workspace_base'] == workspace_base
            assert call_kwargs['parallel'] == 8
            assert call_kwargs['gop_size'] == 240
            assert result == 0

    def test_error_when_parallel_not_specified(self, tmp_path):
        """Test that SystemExit is raised when parallel count is not specified"""
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_error_when_svtav1_params_not_specified(self, tmp_path):
        """Test that SystemExit is raised when svtav1_params is not specified"""
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '8',
            '--gop', '240'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()


class TestMainExecution:
    """Tests for main function execution"""

    def test_returns_0_on_success(self, tmp_path, monkeypatch):
        """Test that 0 is returned when processing succeeds"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            assert result == 0

    def test_returns_1_on_failure(self, tmp_path, monkeypatch):
        """Test that 1 is returned when processing fails"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 1

            result = main()

            assert result == 1

    def test_setup_console_logger_is_called(self, tmp_path, monkeypatch):
        """Test that setup_console_logger is called"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.setup_console_logger') as mock_setup_logging, \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            mock_setup_logging.assert_called_once_with('av1_encoder.s3', level=logging.INFO)


class TestMainArgTypes:
    """Tests for the types of main function arguments"""

    def test_integer_args_converted_correctly(self, tmp_path, monkeypatch):
        """Test that integer arguments are converted correctly"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '12', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # Verify integer type was used
            call_kwargs = mock_run.call_args[1]
            assert isinstance(call_kwargs['parallel'], int)
            assert call_kwargs['parallel'] == 12

    def test_invalid_integer_value_raises_system_exit(self, tmp_path):
        """Test that SystemExit is raised when an invalid integer value is specified"""
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', 'invalid', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_svtav1_args_passed_as_list(self, tmp_path, monkeypatch):
        """Test that svtav1_args is passed correctly as a list"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30,preset=6'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # Verify svtav1_args was passed as a list
            call_kwargs = mock_run.call_args[1]
            assert isinstance(call_kwargs['svtav1_args'], list)
            # Expanded form from CLI
            assert call_kwargs['svtav1_args'] == ['--crf', '30', '--preset', '6']


class TestMainEdgeCases:
    """Tests for edge cases in the main function"""

    def test_negative_value_args(self, tmp_path, monkeypatch):
        """Test that negative value arguments are processed correctly"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '-1', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # Verify negative value was passed
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['parallel'] == -1

    def test_very_large_value_args(self, tmp_path, monkeypatch):
        """Test that very large value arguments are processed correctly"""
        # Create pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("/path/to/video1.mkv\n")

        monkeypatch.chdir(tmp_path)

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '1000000', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # Verify large value was passed
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['parallel'] == 1000000


class TestMainArgparseBehavior:
    """Tests for argparse behavior"""

    def test_help_option(self):
        """Test that SystemExit is raised with the help option"""
        test_args = ['prog', '--help']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()

            # Help exits normally (code 0)
            assert exc_info.value.code == 0

    def test_unknown_option_raises_system_exit(self):
        """Test that SystemExit is raised with an unknown option"""
        test_args = [
            'prog',
            '--unknown-option', 'value'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()
