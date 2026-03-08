import logging
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from av1_encoder.core.config import EncodingConfig
from av1_encoder.core.ffmpeg import SegmentInfo
from av1_encoder.core.workspace import Workspace
from av1_encoder.encoding.encoder import EncodingOrchestrator, _worker_init


@pytest.fixture
def encoding_config(tmp_path):
    """Fixture to create an EncodingConfig for testing"""
    input_file = tmp_path / "input.mkv"
    input_file.touch()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    return EncodingConfig(
        input_file=input_file,
        workspace_dir=workspace_dir,
        parallel_jobs=2,
        gop_size=240,
        segment_length=60,
        svtav1_args=['--crf', '30', '--preset', '6']
    )


@pytest.fixture
def mock_workspace(tmp_path):
    """Fixture to create a mock Workspace for testing"""
    work_dir = tmp_path / "workspace"
    work_dir.mkdir(exist_ok=True)

    return Workspace(
        work_dir=work_dir,
        log_file=work_dir / "main.log",
        concat_file=work_dir / "concat.txt",
        output_file=work_dir / "output.mkv"
    )


@pytest.fixture
def mock_logger():
    """Fixture to create a mock logger"""
    logger = Mock(spec=logging.Logger)
    return logger


class TestWorkerInit:
    """Tests for the _worker_init function"""

    def test_reset_signal_handlers_to_default(self):
        """Test that _worker_init resets signal handlers to default (Unix)"""
        import signal

        with patch('av1_encoder.encoding.encoder.get_available_signals') as mock_get_signals, \
             patch('signal.signal') as mock_signal:

            # Simulate Unix environment
            mock_get_signals.return_value = {
                'SIGINT': signal.SIGINT,
                'SIGTERM': signal.SIGTERM
            }

            _worker_init()

            # Verify signal.signal was called twice (SIGINT, SIGTERM)
            assert mock_signal.call_count == 2

    def test_reset_signal_handlers_to_default_windows(self):
        """Test that _worker_init works correctly in a Windows environment"""
        import signal

        with patch('av1_encoder.encoding.encoder.get_available_signals') as mock_get_signals, \
             patch('signal.signal') as mock_signal:

            # Simulate Windows environment (SIGINT only)
            mock_get_signals.return_value = {'SIGINT': signal.SIGINT}

            _worker_init()

            # Verify signal.signal was called only once
            assert mock_signal.call_count == 1
            # Verify SIG_DFL was set for SIGINT
            assert mock_signal.call_args[0] == (signal.SIGINT, signal.SIG_DFL)


class TestEncodingOrchestratorInitialization:
    """Tests for EncodingOrchestrator initialization"""

    def test_required_components_created_on_init(self, encoding_config, tmp_path):
        """Test that EncodingOrchestrator is initialized with the required components"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path') as mock_make_workspace, \
             patch('av1_encoder.encoding.encoder.FFmpegService') as mock_ffmpeg_class, \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger') as mock_setup_logger:

            mock_workspace = Mock()
            mock_workspace.log_file = tmp_path / "test.log"
            mock_make_workspace.return_value = mock_workspace
            mock_logger = Mock()
            mock_setup_logger.return_value = mock_logger

            orchestrator = EncodingOrchestrator(encoding_config)

            # Verify config is set
            assert orchestrator.config == encoding_config

            # Verify start_time is set
            assert isinstance(orchestrator.start_time, datetime)

            # Verify workspace is created
            mock_make_workspace.assert_called_once_with(encoding_config.workspace_dir)
            assert orchestrator.workspace == mock_workspace

            # Verify logger is initialized
            mock_setup_logger.assert_called_once_with(
                "av1_encoder", mock_workspace.log_file, level=logging.INFO
            )
            assert orchestrator.logger == mock_logger

            # Verify FFmpegService is created
            mock_ffmpeg_class.assert_called_once()

    def test_start_time_is_close_to_current_time(self, encoding_config):
        """Test that start_time is set to the current time"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            before = datetime.now()
            orchestrator = EncodingOrchestrator(encoding_config)
            after = datetime.now()

            # Verify start_time is the current time at initialization
            assert before <= orchestrator.start_time <= after


class TestEncodingOrchestratorRun:
    """Tests for the run method of EncodingOrchestrator"""

    def test_run_executes_steps_in_correct_order(self, encoding_config):
        """Test that the run method executes each step in the correct order"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'), \
             patch.object(EncodingOrchestrator, '_encode_segments') as mock_encode, \
             patch.object(EncodingOrchestrator, '_generate_concat_file') as mock_generate_concat, \
             patch.object(EncodingOrchestrator, '_print_completion') as mock_completion:

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.run()

            # Verify each method was called
            mock_encode.assert_called_once()
            mock_generate_concat.assert_called_once()
            mock_completion.assert_called_once()

    def test_run_logs_and_reraises_on_error(self, encoding_config):
        """Test that run logs and re-raises an exception when an error occurs"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            error = RuntimeError("test error")

            with patch.object(orchestrator, '_encode_segments', side_effect=error):

                with pytest.raises(RuntimeError, match="test error"):
                    orchestrator.run()

                # Verify logger.exception was called
                orchestrator.logger.exception.assert_called_once_with("Error")

    def test_keyboard_interrupt_handling(self, encoding_config):
        """Test that KeyboardInterrupt is handled appropriately"""
        import sys
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # KeyboardInterrupt during encoding
            with patch.object(orchestrator, '_encode_segments', side_effect=KeyboardInterrupt):
                with pytest.raises(SystemExit) as exc_info:
                    orchestrator.run()

                # Verify exit with code 1 for cross-platform compatibility
                assert exc_info.value.code == 1

                # Verify error log was output
                orchestrator.logger.error.assert_called_once_with("Processing interrupted")


class TestEncodingOrchestratorSignalHandler:
    """Tests for EncodingOrchestrator signal handling"""

    def test_raise_keyboard_interrupt_on_signal_in_main_process(self, encoding_config):
        """Test that KeyboardInterrupt is raised when a signal is received in the main process"""
        import os
        import signal

        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # Verify PID matches the main process
            assert orchestrator._main_pid == os.getpid()

            # Directly invoke signal handler
            with pytest.raises(KeyboardInterrupt):
                orchestrator._signal_handler(signal.SIGINT, None)

            # Verify log was output
            orchestrator.logger.warning.assert_called_once()
            assert "Interrupt signal received" in orchestrator.logger.warning.call_args[0][0]

    def test_ignore_signal_in_worker_process(self, encoding_config):
        """Test that signals are ignored in worker processes"""
        import signal

        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # Change PID to simulate a worker process
            original_pid = orchestrator._main_pid
            orchestrator._main_pid = original_pid + 1000

            # No exception should be raised when signal handler is called
            orchestrator._signal_handler(signal.SIGINT, None)

            # Verify no log was output
            orchestrator.logger.warning.assert_not_called()

    def test_signal_handler_is_set(self, encoding_config):
        """Test that signal handlers are set when run is executed"""
        import signal

        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'), \
             patch.object(EncodingOrchestrator, '_encode_segments'), \
             patch.object(EncodingOrchestrator, '_generate_concat_file'), \
             patch.object(EncodingOrchestrator, '_print_completion'), \
             patch('signal.signal') as mock_signal:

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.run()

            # Verify signal.signal was called at least twice (SIGINT, SIGTERM)
            assert mock_signal.call_count >= 2

            # Verify handlers were set for SIGINT and SIGTERM
            signal_calls = [call[0] for call in mock_signal.call_args_list]
            assert any(signal.SIGINT in call for call in signal_calls)
            assert any(signal.SIGTERM in call for call in signal_calls)


class TestEncodingOrchestratorPrintCompletion:
    """Tests for the _print_completion method of EncodingOrchestrator"""

    def test_log_completion_info(self, encoding_config, mock_workspace):
        """Test that _print_completion logs the processing time"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.start_time = datetime.now() - timedelta(seconds=100)

            orchestrator._print_completion()

            # Verify log was output
            assert orchestrator.logger.info.call_count == 1
            calls = [call[0][0] for call in orchestrator.logger.info.call_args_list]
            assert "Done" in calls[0]
            assert "Elapsed" in calls[0]


class TestEncodingOrchestratorListSegments:
    """Tests for the _list_segments method of EncodingOrchestrator"""

    def test_generate_segment_list(self, encoding_config, mock_workspace):
        """Test that _list_segments generates the correct segment list"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            # Set up mocks for get_fps() and get_gop_size()
            orchestrator.ffmpeg.get_fps.return_value = 24.0
            # encoding_config has get_gop_size() method, used directly (default 240)

            with patch.object(orchestrator, '_calc_num_segments', return_value=3):
                segments = orchestrator._list_segments()

                assert len(segments) == 3

                # First segment
                assert segments[0].index == 0
                assert segments[0].start_time == 0
                assert segments[0].duration == 60.0
                assert segments[0].is_final is False
                assert segments[0].file == mock_workspace.work_dir / "segment_0000.ivf"
                assert segments[0].log_file == mock_workspace.work_dir / "segment_0000.log"

                # Second segment
                assert segments[1].index == 1
                assert segments[1].start_time == 60.0
                assert segments[1].is_final is False

                # Final segment
                assert segments[2].index == 2
                assert segments[2].start_time == 120.0
                assert segments[2].is_final is True

    def test_single_segment_case(self, encoding_config, mock_workspace):
        """Test when the video has only one segment"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            # Set up mock for get_fps()
            orchestrator.ffmpeg.get_fps.return_value = 24.0

            with patch.object(orchestrator, '_calc_num_segments', return_value=1):
                segments = orchestrator._list_segments()

                assert len(segments) == 1
                assert segments[0].index == 0
                assert segments[0].is_final is True


class TestEncodingOrchestratorCalcNumSegments:
    """Tests for the _calc_num_segments method of EncodingOrchestrator"""

    def test_calculate_segment_count(self, encoding_config, mock_workspace):
        """Test that _calc_num_segments calculates the segment count correctly"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.ffmpeg = Mock()

            # 180 second video, 60 second segment length
            orchestrator.ffmpeg.get_duration.return_value = 180.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 3

            # 181 second video, 60 second segment length (rounds up to 4 segments)
            orchestrator.ffmpeg.get_duration.return_value = 181.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 4

            # 60 second video, 60 second segment length
            orchestrator.ffmpeg.get_duration.return_value = 60.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 1

    def test_fractional_segment_count_rounds_up(self, encoding_config, mock_workspace):
        """Test that a fractional segment count is rounded up"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.ffmpeg = Mock()

            # 100 second video, 60 second segment length (2 segments)
            orchestrator.ffmpeg.get_duration.return_value = 100.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 2


class TestEncodingOrchestratorCompletedSegments:
    """Tests for completed segment management in EncodingOrchestrator"""

    def test_return_empty_set_when_completed_txt_missing(self, encoding_config, mock_workspace):
        """Test that an empty set is returned when completed.txt does not exist"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            result = orchestrator._load_completed_segments()
            assert result == set()

    def test_load_segment_numbers_from_completed_txt(self, encoding_config, mock_workspace):
        """Test that segment numbers are loaded from completed.txt"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            # Create completed.txt
            completed_file = mock_workspace.work_dir / "completed.txt"
            completed_file.write_text("0\n2\n5\n")

            result = orchestrator._load_completed_segments()
            assert result == {0, 2, 5}

    def test_load_correctly_with_empty_lines_in_completed_txt(self, encoding_config, mock_workspace):
        """Test that empty lines in completed.txt are handled correctly"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            # Create completed.txt with empty lines
            completed_file = mock_workspace.work_dir / "completed.txt"
            completed_file.write_text("0\n\n2\n\n")

            result = orchestrator._load_completed_segments()
            assert result == {0, 2}

    def test_mark_segment_completed(self, encoding_config, mock_workspace):
        """Test that _mark_segment_completed appends the segment number"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            completed_file = mock_workspace.work_dir / "completed.txt"

            # Mark segment 0 as completed
            orchestrator._mark_segment_completed(0)
            assert completed_file.read_text() == "0\n"

            # Mark segment 2 as additionally completed
            orchestrator._mark_segment_completed(2)
            assert completed_file.read_text() == "0\n2\n"

            # Mark segment 1 as additionally completed
            orchestrator._mark_segment_completed(1)
            assert completed_file.read_text() == "0\n2\n1\n"


class TestEncodingOrchestratorEncodeSegments:
    """Tests for the _encode_segments method of EncodingOrchestrator"""

    def test_skip_segments_recorded_in_completed_txt(self, encoding_config, mock_workspace, tmp_path):
        """Test that segments recorded in completed.txt are skipped"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.VideoProbe') as mock_probe_class, \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            # Set up video_probe mock
            mock_probe = mock_probe_class.return_value
            mock_probe.get_total_frames.return_value = 7200
            mock_probe.get_fps.return_value = 60.0

            # Segments 0 and 1 are already recorded in completed.txt
            completed_file = mock_workspace.work_dir / "completed.txt"
            completed_file.write_text("0\n1\n")

            segments = [
                SegmentInfo(0, 0, 60, False, mock_workspace.work_dir / "segment_0000.ivf", Path("seg0.log")),
                SegmentInfo(1, 60, 60, False, mock_workspace.work_dir / "segment_0001.ivf", Path("seg1.log")),
                SegmentInfo(2, 120, 60, True, mock_workspace.work_dir / "segment_0002.ivf", Path("seg2.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoding.encoder.ProcessPoolExecutor') as mock_executor_class:

                # Encode only segment 2
                mock_future = Mock()
                mock_future.result.return_value = True

                mock_executor = MagicMock()
                mock_executor.submit.return_value = mock_future
                mock_executor.__enter__.return_value = mock_executor
                mock_executor.__exit__.return_value = False
                mock_executor_class.return_value = mock_executor

                def mock_as_completed(future_dict):
                    return list(future_dict.keys())

                with patch('av1_encoder.encoding.encoder.as_completed', side_effect=mock_as_completed):
                    orchestrator._encode_segments()

                # Verify only segment 2 was submitted
                assert mock_executor.submit.call_count == 1
                submitted_segment = mock_executor.submit.call_args[0][1]
                assert submitted_segment.index == 2

                # Verify skip log was output
                info_calls = [call[0][0] for call in orchestrator.logger.info.call_args_list]
                assert any("Skipping" in call and "2" in call for call in info_calls)

    def test_skip_all_when_all_segments_completed(self, encoding_config, mock_workspace, tmp_path):
        """Test that encoding is skipped when all segments are already in completed.txt"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # All segments are recorded in completed.txt
            completed_file = mock_workspace.work_dir / "completed.txt"
            completed_file.write_text("0\n1\n")

            segments = [
                SegmentInfo(0, 0, 60, False, mock_workspace.work_dir / "segment_0000.ivf", Path("seg0.log")),
                SegmentInfo(1, 60, 60, True, mock_workspace.work_dir / "segment_0001.ivf", Path("seg1.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoding.encoder.ProcessPoolExecutor') as mock_executor_class:

                orchestrator._encode_segments()

                # Verify ProcessPoolExecutor was not called
                mock_executor_class.assert_not_called()

                # Verify completion message was output
                info_calls = [call[0][0] for call in orchestrator.logger.info.call_args_list]
                assert any("All segments are already completed" in call for call in info_calls)

    def test_encode_segments_in_parallel(self, encoding_config, mock_workspace):
        """Test that _encode_segments encodes segments in parallel"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.VideoProbe') as mock_probe_class, \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            # Set up video_probe mock
            mock_probe = mock_probe_class.return_value
            mock_probe.get_total_frames.return_value = 7200  # 2 min × 60fps
            mock_probe.get_fps.return_value = 60.0

            # Return 3 segments via mock
            segments = [
                SegmentInfo(0, 0, 60, False, Path("seg0.ivf"), Path("seg0.log")),
                SegmentInfo(1, 60, 60, False, Path("seg1.ivf"), Path("seg1.log")),
                SegmentInfo(2, 120, 60, True, Path("seg2.ivf"), Path("seg2.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoding.encoder.ProcessPoolExecutor') as mock_executor_class:

                # Mock futures that return success
                mock_futures = []
                for i in range(3):
                    mock_future = Mock()
                    mock_future.result.return_value = True
                    mock_futures.append(mock_future)

                # Set up mock executor
                mock_executor = MagicMock()
                mock_executor.submit.side_effect = mock_futures
                mock_executor.__enter__.return_value = mock_executor
                mock_executor.__exit__.return_value = False
                mock_executor_class.return_value = mock_executor

                # Mock as_completed to return keys from the passed dict
                def mock_as_completed(future_dict):
                    return list(future_dict.keys())

                with patch('av1_encoder.encoding.encoder.as_completed', side_effect=mock_as_completed):
                    orchestrator._encode_segments()

                # Verify ProcessPoolExecutor was created with the correct max_workers
                assert mock_executor_class.call_count == 1
                call_kwargs = mock_executor_class.call_args[1]
                assert call_kwargs['max_workers'] == 2

                # Verify submit was called for each segment
                assert mock_executor.submit.call_count == 3

    def test_raise_error_on_encode_failure(self, encoding_config, mock_workspace):
        """Test that a RuntimeError is raised when encoding fails"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.VideoProbe') as mock_probe_class, \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            # Set up video_probe mock
            mock_probe = mock_probe_class.return_value
            mock_probe.get_total_frames.return_value = 7200
            mock_probe.get_fps.return_value = 60.0

            segments = [
                SegmentInfo(0, 0, 60, False, Path("seg0.ivf"), Path("seg0.log")),
                SegmentInfo(1, 60, 60, True, Path("seg1.ivf"), Path("seg1.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoding.encoder.ProcessPoolExecutor') as mock_executor_class:

                # One succeeds, one fails
                mock_future1 = Mock()
                mock_future1.result.return_value = True
                mock_future2 = Mock()
                mock_future2.result.return_value = False

                mock_futures = [mock_future1, mock_future2]

                # Set up mock executor
                mock_executor = MagicMock()
                mock_executor.submit.side_effect = mock_futures
                mock_executor.__enter__.return_value = mock_executor
                mock_executor.__exit__.return_value = False
                mock_executor_class.return_value = mock_executor

                # Mock as_completed to return keys from the passed dict
                def mock_as_completed(future_dict):
                    return list(future_dict.keys())

                with patch('av1_encoder.encoding.encoder.as_completed', side_effect=mock_as_completed):
                    with pytest.raises(RuntimeError, match="Encoding failed for segment"):
                        orchestrator._encode_segments()


class TestEncodingOrchestratorGenerateConcatFile:
    """Tests for the _generate_concat_file method of EncodingOrchestrator"""

    def test_generate_concat_txt(self, encoding_config, mock_workspace):
        """Test that _generate_concat_file generates concat.txt"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # Create segment files
            segment_files = [
                mock_workspace.work_dir / "segment_0000.ivf",
                mock_workspace.work_dir / "segment_0001.ivf",
                mock_workspace.work_dir / "segment_0002.ivf"
            ]

            for seg_file in segment_files:
                seg_file.touch()

            orchestrator._generate_concat_file()

            # Verify concat.txt was generated
            assert mock_workspace.concat_file.exists()

            # Verify content of concat.txt
            with open(mock_workspace.concat_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            assert len(lines) == 3
            for i, line in enumerate(lines):
                assert line == f"file '{segment_files[i].resolve()}'\n"


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
        assert config.svtav1_args == ['--crf', '30', '--preset', '6']
        assert config.segment_length == 120

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

        assert config.svtav1_args == []
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
