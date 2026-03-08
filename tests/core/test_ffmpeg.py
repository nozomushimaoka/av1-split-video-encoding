import logging
from unittest.mock import Mock, patch

import pytest

from av1_encoder.core.config import EncodingConfig
from av1_encoder.core.ffmpeg import FFmpegService, SegmentInfo
from av1_encoder.core.logging_config import setup_segment_logger


@pytest.fixture
def ffmpeg_service():
    """Fixture to create an FFmpegService instance"""
    return FFmpegService()


@pytest.fixture
def segment_info(tmp_path):
    """Fixture to create a SegmentInfo for testing"""
    return SegmentInfo(
        index=0,
        start_time=0,
        duration=60,
        is_final=False,
        file=tmp_path / "segment_0.ivf",
        log_file=tmp_path / "segment_0.log"
    )


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
        parallel_jobs=4,
        gop_size=240,
        segment_length=60,
        svtav1_args=['--crf', '30', '--preset', '6']
    )


@pytest.fixture
def mock_logger():
    """Fixture to create a mock logger"""
    mock_logger = Mock()
    mock_logger.handlers = []
    return mock_logger


class TestSegmentInfo:
    """Tests for the SegmentInfo dataclass"""

    def test_create_segment_info(self, tmp_path):
        """Test that SegmentInfo is created correctly"""
        segment_info = SegmentInfo(
            index=1,
            start_time=60,
            duration=60,
            is_final=False,
            file=tmp_path / "segment_1.ivf",
            log_file=tmp_path / "segment_1.log"
        )

        assert segment_info.index == 1
        assert segment_info.start_time == 60
        assert segment_info.duration == 60
        assert segment_info.is_final is False
        assert segment_info.file == tmp_path / "segment_1.ivf"
        assert segment_info.log_file == tmp_path / "segment_1.log"


class TestFFmpegServiceGetDuration:
    """Tests for the get_duration method of FFmpegService"""

    def test_get_video_duration(self, ffmpeg_service, tmp_path):
        """Test getting video duration using ffprobe"""
        input_file = tmp_path / "input.mkv"

        # Mock ffprobe output
        mock_result = Mock()
        mock_result.stdout = '''{
    "format": {
        "filename": "xxx.mkv",
        "nb_streams": 2,
        "nb_programs": 0,
        "nb_stream_groups": 0,
        "format_name": "matroska,webm",
        "format_long_name": "Matroska / WebM",
        "start_time": "0.000000",
        "duration": "2112.857000",
        "size": "2601514857",
        "bit_rate": "9850225",
        "probe_score": 100,
        "tags": {
            "ENCODER": "Lavf61.7.100"
        }
    }
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result) as mock_run:
            duration = ffmpeg_service.get_duration(input_file)

            # Verify the correct command was used
            mock_run.assert_called_once_with(
                [
                    'ffprobe', '-v', 'quiet', '-print_format', 'json',
                    '-show_format', str(input_file)
                ],
                capture_output=True,
                text=True,
                check=True
            )

            # Verify the correct duration is returned
            assert duration == 2112.857

    def test_get_video_duration_integer_value(self, ffmpeg_service, tmp_path):
        """Test getting video duration with an integer value"""
        input_file = tmp_path / "input.mkv"

        mock_result = Mock()
        mock_result.stdout = '''{
    "format": {
        "filename": "xxx.mkv",
        "nb_streams": 2,
        "nb_programs": 0,
        "nb_stream_groups": 0,
        "format_name": "matroska,webm",
        "format_long_name": "Matroska / WebM",
        "start_time": "0.000000",
        "duration": "2112.000000",
        "size": "2601514857",
        "bit_rate": "9850225",
        "probe_score": 100,
        "tags": {
            "ENCODER": "Lavf61.7.100"
        }
    }
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result):
            duration = ffmpeg_service.get_duration(input_file)
            assert duration == 2112.0


class TestFFmpegServiceGetFps:
    """Tests for the get_fps method of FFmpegService"""

    def test_get_frame_rate_fractional_format(self, ffmpeg_service, tmp_path):
        """Test getting frame rate in fractional format (24000/1001)"""
        input_file = tmp_path / "input.mkv"

        mock_result = Mock()
        mock_result.stdout = '''{
    "streams": [
        {
            "index": 0,
            "codec_name": "h264",
            "codec_type": "video",
            "r_frame_rate": "24000/1001",
            "avg_frame_rate": "24000/1001"
        }
    ]
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result) as mock_run:
            fps = ffmpeg_service.get_fps(input_file)

            # Verify the correct command was used
            mock_run.assert_called_once_with(
                [
                    'ffprobe', '-v', 'quiet', '-print_format', 'json',
                    '-show_streams', '-select_streams', 'v:0', str(input_file)
                ],
                capture_output=True,
                text=True,
                check=True
            )

            # Verify the correct frame rate is returned (23.976...)
            assert abs(fps - 23.976023976023978) < 0.0001

    def test_get_frame_rate_integer_format(self, ffmpeg_service, tmp_path):
        """Test getting frame rate in integer format (30)"""
        input_file = tmp_path / "input.mkv"

        mock_result = Mock()
        mock_result.stdout = '''{
    "streams": [
        {
            "index": 0,
            "codec_name": "h264",
            "codec_type": "video",
            "r_frame_rate": "30/1",
            "avg_frame_rate": "30/1"
        }
    ]
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result):
            fps = ffmpeg_service.get_fps(input_file)
            assert fps == 30.0

    def test_get_frame_rate_60fps(self, ffmpeg_service, tmp_path):
        """Test getting 60fps frame rate"""
        input_file = tmp_path / "input.mkv"

        mock_result = Mock()
        mock_result.stdout = '''{
    "streams": [
        {
            "index": 0,
            "codec_name": "h264",
            "codec_type": "video",
            "r_frame_rate": "60/1",
            "avg_frame_rate": "60/1"
        }
    ]
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result):
            fps = ffmpeg_service.get_fps(input_file)
            assert fps == 60.0


class TestFFmpegServiceBuildFfmpegCommand:
    """Tests for the _build_ffmpeg_command method of FFmpegService"""

    def test_build_ffmpeg_command_normal_segment(self, ffmpeg_service, tmp_path):
        """Test building an FFmpeg command for a normal (non-final) segment"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60
        )

        cmd = ffmpeg_service._build_ffmpeg_command(
            input_file=input_file,
            start_time=120.0,
            duration=60.0,
            is_final_segment=False,
            config=config
        )

        # Verify command structure
        assert cmd[0] == 'ffmpeg'
        assert '-ss' in cmd
        assert '120.0' in cmd
        assert '-i' in cmd
        assert str(input_file) in cmd
        assert '-t' in cmd  # -t option present because not final segment
        assert '60.0' in cmd
        assert '-f' in cmd
        assert 'yuv4mpegpipe' in cmd
        assert '-strict' in cmd
        assert '-1' in cmd
        assert cmd[-1] == '-'

    def test_build_ffmpeg_command_final_segment(self, ffmpeg_service, tmp_path):
        """Test building an FFmpeg command for the final segment (no -t option)"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60
        )

        cmd = ffmpeg_service._build_ffmpeg_command(
            input_file=input_file,
            start_time=300.0,
            duration=60.0,
            is_final_segment=True,
            config=config
        )

        # Verify -t option is not present
        assert '-t' not in cmd
        assert '60.0' not in cmd
        # Other basic structure is the same
        assert cmd[0] == 'ffmpeg'
        assert '-ss' in cmd
        assert '300.0' in cmd
        assert '-f' in cmd
        assert 'yuv4mpegpipe' in cmd

    def test_build_ffmpeg_command_with_extra_params(self, ffmpeg_service, tmp_path):
        """Test building an FFmpeg command with additional parameters"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            ffmpeg_args=['-vf', 'scale=1920:1080', '-r', '24']
        )

        cmd = ffmpeg_service._build_ffmpeg_command(
            input_file=input_file,
            start_time=0.0,
            duration=60.0,
            is_final_segment=False,
            config=config
        )

        # Verify additional parameters are included
        assert '-vf' in cmd
        assert 'scale=1920:1080' in cmd
        assert '-r' in cmd
        assert '24' in cmd
        # Y4M format output options are at the end
        assert '-f' in cmd
        assert 'yuv4mpegpipe' in cmd
        assert cmd[-3:] == ['-strict', '-1', '-']

    def test_build_ffmpeg_command_without_extra_params(self, ffmpeg_service, tmp_path):
        """Test building an FFmpeg command without extra parameters"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60
        )

        cmd = ffmpeg_service._build_ffmpeg_command(
            input_file=input_file,
            start_time=0.0,
            duration=60.0,
            is_final_segment=False,
            config=config
        )

        # Verify only the basic structure is present
        assert cmd[0] == 'ffmpeg'
        assert '-ss' in cmd
        assert '-i' in cmd
        assert '-t' in cmd
        assert '-f' in cmd
        assert 'yuv4mpegpipe' in cmd


    def test_hardware_decode_cuda(self, ffmpeg_service, tmp_path):
        """Test that -hwaccel cuda -hwaccel_output_format cuda appears before -i when hardware_decode='cuda'"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            hardware_decode='cuda'
        )

        cmd = ffmpeg_service._build_ffmpeg_command(
            input_file=input_file,
            start_time=0.0,
            duration=60.0,
            is_final_segment=False,
            config=config
        )

        assert '-hwaccel' in cmd
        assert 'cuda' in cmd
        assert '-hwaccel_output_format' in cmd
        assert '-hwaccel_device' not in cmd
        # -hwaccel must appear before -i
        assert cmd.index('-hwaccel') < cmd.index('-i')

    def test_hardware_decode_vaapi_with_device(self, ffmpeg_service, tmp_path):
        """Test that -hwaccel_device is included when hardware_decode='vaapi' and device is set"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            hardware_decode='vaapi',
            hardware_decode_device='/dev/dri/renderD128'
        )

        cmd = ffmpeg_service._build_ffmpeg_command(
            input_file=input_file,
            start_time=0.0,
            duration=60.0,
            is_final_segment=False,
            config=config
        )

        assert '-hwaccel' in cmd
        assert 'vaapi' in cmd
        assert '-hwaccel_output_format' in cmd
        assert '-hwaccel_device' in cmd
        assert '/dev/dri/renderD128' in cmd
        assert cmd.index('-hwaccel') < cmd.index('-i')

    def test_hardware_decode_none(self, ffmpeg_service, tmp_path):
        """Test that -hwaccel is not included when hardware_decode is None"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60
        )

        cmd = ffmpeg_service._build_ffmpeg_command(
            input_file=input_file,
            start_time=0.0,
            duration=60.0,
            is_final_segment=False,
            config=config
        )

        assert '-hwaccel' not in cmd
        assert '-hwaccel_output_format' not in cmd
        assert '-hwaccel_device' not in cmd

class TestFFmpegServiceBuildSvtav1Command:
    """Tests for the _build_svtav1_command method of FFmpegService"""

    def test_build_svtav1_command_basic(self, ffmpeg_service, tmp_path):
        """Test building a basic SvtAv1EncApp command"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        output_file = tmp_path / "segment_0.ivf"
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60
        )

        cmd = ffmpeg_service._build_svtav1_command(
            output_file=output_file,
            config=config
        )

        # Verify command structure
        assert cmd[0] == 'SvtAv1EncApp'
        assert '-i' in cmd
        assert 'stdin' in cmd
        assert '--keyint' in cmd
        assert '240' in cmd
        assert '-b' in cmd
        assert str(output_file) in cmd

    def test_build_svtav1_command_with_extra_options(self, ffmpeg_service, tmp_path):
        """Test building a SvtAv1EncApp command with additional options"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        output_file = tmp_path / "segment_0.ivf"
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            svtav1_args=['--crf', '30', '--preset', '6']
        )

        cmd = ffmpeg_service._build_svtav1_command(
            output_file=output_file,
            config=config
        )

        # Verify basic structure
        assert cmd[0] == 'SvtAv1EncApp'
        assert '-i' in cmd
        assert 'stdin' in cmd
        assert '--keyint' in cmd
        assert '240' in cmd
        # Verify additional options
        assert '--crf' in cmd
        assert '30' in cmd
        assert '--preset' in cmd
        assert '6' in cmd
        # Output file is last
        assert '-b' in cmd
        assert str(output_file) in cmd

    def test_build_svtav1_command_with_complex_options(self, ffmpeg_service, tmp_path):
        """Test building a SvtAv1EncApp command with complex additional options"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        output_file = tmp_path / "segment_0.ivf"
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            svtav1_args=[
                '--preset', '4',
                '--crf', '25',
                '--enable-qm', '1',
                '--qm-min', '8',
                '--scd', '1'
            ]
        )

        cmd = ffmpeg_service._build_svtav1_command(
            output_file=output_file,
            config=config
        )

        # Verify complex additional options
        assert '--preset' in cmd
        assert '4' in cmd
        assert '--crf' in cmd
        assert '25' in cmd
        assert '--enable-qm' in cmd
        assert '1' in cmd
        assert '--qm-min' in cmd
        assert '8' in cmd
        assert '--scd' in cmd

    def test_build_svtav1_command_with_different_gop_size(self, ffmpeg_service, tmp_path):
        """Test building a SvtAv1EncApp command with a different GOP size"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        output_file = tmp_path / "segment_0.ivf"
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=120,  # Different GOP size
            segment_length=60
        )

        cmd = ffmpeg_service._build_svtav1_command(
            output_file=output_file,
            config=config
        )

        # Verify GOP size
        assert '--keyint' in cmd
        assert '120' in cmd
        assert '240' not in cmd


class TestSetupSegmentLogger:
    """Tests for the setup_segment_logger function (moved to logging_config module)"""

    def test_setup_segment_logger(self, tmp_path):
        """Test creating and configuring a segment-specific logger"""
        log_file = tmp_path / "segment_0.log"
        segment_idx = 0

        logger = setup_segment_logger(
            segment_idx=segment_idx,
            log_file=log_file
        )

        # Verify basic logger configuration
        assert logger.name == "av1_encoder.segment_0"
        assert logger.level == logging.DEBUG
        assert logger.propagate is False
        assert len(logger.handlers) == 1

        # Verify handler configuration
        handler = logger.handlers[0]
        assert isinstance(handler, logging.FileHandler)
        assert handler.level == logging.DEBUG

        # Cleanup
        for h in logger.handlers[:]:
            h.close()
            logger.removeHandler(h)

    def test_setup_segment_logger_different_index(self, tmp_path):
        """Test configuring a logger with a different segment index"""
        log_file = tmp_path / "segment_5.log"
        segment_idx = 5

        logger = setup_segment_logger(
            segment_idx=segment_idx,
            log_file=log_file
        )

        # Verify the correct index is in the logger name
        assert logger.name == "av1_encoder.segment_5"

        # Cleanup
        for h in logger.handlers[:]:
            h.close()
            logger.removeHandler(h)

    def test_setup_segment_logger_clears_handlers(self, tmp_path):
        """Test that existing handlers are cleared before adding a new one"""
        log_file = tmp_path / "segment_0.log"
        segment_idx = 0

        # Create the first logger
        logger1 = setup_segment_logger(
            segment_idx=segment_idx,
            log_file=log_file
        )
        assert len(logger1.handlers) == 1
        first_handler = logger1.handlers[0]

        # Create again with the same index (existing handlers should be cleared)
        logger2 = setup_segment_logger(
            segment_idx=segment_idx,
            log_file=log_file
        )

        # Verify only one new handler exists
        assert len(logger2.handlers) == 1
        # Verify the same logger instance is returned
        assert logger1 is logger2

        # Cleanup
        for h in logger2.handlers[:]:
            h.close()
            logger2.removeHandler(h)


class TestFFmpegServiceEncodeSegment:
    """Tests for the encode_segment method of FFmpegService"""

    def test_encode_segment_success(self, ffmpeg_service, segment_info, encoding_config, tmp_path, mock_logger):
        """Test successfully encoding a segment"""
        input_file = tmp_path / "input.mkv"

        # Mock FFmpeg process
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = iter([])
        mock_ffmpeg_process.wait.return_value = 0

        # Mock SvtAv1EncApp process
        mock_svtav1_process = Mock()
        mock_svtav1_process.stdout = iter([])
        mock_svtav1_process.stderr = iter(["Encoding frame 100", "Encoding complete"])
        mock_svtav1_process.wait.return_value = 0

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.setup_segment_logger', return_value=mock_logger), \
             patch('av1_encoder.core.ffmpeg.cleanup_logger'):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # Verify success is returned
            assert result is True

            # Verify Popen was called twice (FFmpeg + SvtAv1EncApp)
            assert mock_popen.call_count == 2

            # Verify FFmpeg command
            ffmpeg_call = mock_popen.call_args_list[0]
            ffmpeg_cmd = ffmpeg_call[0][0]
            assert ffmpeg_cmd[0] == 'ffmpeg'
            assert '-ss' in ffmpeg_cmd
            assert '0' in ffmpeg_cmd  # start_time
            assert '-i' in ffmpeg_cmd
            assert str(input_file) in ffmpeg_cmd
            assert '-t' in ffmpeg_cmd  # -t option present because is_final is False
            assert '60' in ffmpeg_cmd  # duration
            assert '-f' in ffmpeg_cmd
            assert 'yuv4mpegpipe' in ffmpeg_cmd

            # Verify SvtAv1EncApp command
            svtav1_call = mock_popen.call_args_list[1]
            svtav1_cmd = svtav1_call[0][0]
            assert svtav1_cmd[0] == 'SvtAv1EncApp'
            assert '-i' in svtav1_cmd
            assert 'stdin' in svtav1_cmd
            assert '--keyint' in svtav1_cmd
            assert '240' in svtav1_cmd
            assert '--crf' in svtav1_cmd
            assert '30' in svtav1_cmd
            assert '--preset' in svtav1_cmd
            assert '6' in svtav1_cmd
            assert '-b' in svtav1_cmd

    def test_encode_final_segment(self, ffmpeg_service, tmp_path, encoding_config, mock_logger):
        """Test encoding the final segment (no -t option)"""
        segment_info = SegmentInfo(
            index=5,
            start_time=300,
            duration=60,
            is_final=True,  # Final segment
            file=tmp_path / "segment_5.ivf",
            log_file=tmp_path / "segment_5.log"
        )
        input_file = tmp_path / "input.mkv"

        # Mock FFmpeg process
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = iter([])
        mock_ffmpeg_process.wait.return_value = 0

        # Mock SvtAv1EncApp process
        mock_svtav1_process = Mock()
        mock_svtav1_process.stdout = iter([])
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.setup_segment_logger', return_value=mock_logger), \
             patch('av1_encoder.core.ffmpeg.cleanup_logger'):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            assert result is True

            # Verify -t option is not present (FFmpeg command)
            ffmpeg_call = mock_popen.call_args_list[0]
            ffmpeg_cmd = ffmpeg_call[0][0]
            assert '-t' not in ffmpeg_cmd

    def test_encode_segment_without_extra_args(self, ffmpeg_service, segment_info, tmp_path, mock_logger):
        """Test encoding a segment without extra_args"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60
        )

        # Mock FFmpeg process
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = iter([])
        mock_ffmpeg_process.wait.return_value = 0

        # Mock SvtAv1EncApp process
        mock_svtav1_process = Mock()
        mock_svtav1_process.stdout = iter([])
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.setup_segment_logger', return_value=mock_logger), \
             patch('av1_encoder.core.ffmpeg.cleanup_logger'):

            result = ffmpeg_service.encode_segment(segment_info, input_file, config)

            assert result is True

            # Verify no extra options are included (SvtAv1EncApp command)
            svtav1_call = mock_popen.call_args_list[1]
            svtav1_cmd = svtav1_call[0][0]
            assert '--crf' not in svtav1_cmd
            assert '--preset' not in svtav1_cmd
            # --keyint is automatically added, so it should be present
            assert '--keyint' in svtav1_cmd
            assert '240' in svtav1_cmd

    def test_encode_segment_with_custom_svtav1_args(self, ffmpeg_service, segment_info, tmp_path, mock_logger):
        """Test encoding a segment with custom svtav1_args"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            svtav1_args=[
                '--pix_fmt', 'yuv420p10le',
                '--svtav1-params', 'tune=0:enable-qm=1:qm-min=0',
                '--crf', '25'
            ]
        )

        # Mock FFmpeg process
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = iter([])
        mock_ffmpeg_process.wait.return_value = 0

        # Mock SvtAv1EncApp process
        mock_svtav1_process = Mock()
        mock_svtav1_process.stdout = iter([])
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.setup_segment_logger', return_value=mock_logger), \
             patch('av1_encoder.core.ffmpeg.cleanup_logger'):

            result = ffmpeg_service.encode_segment(segment_info, input_file, config)

            assert result is True

            # Verify custom options are included (SvtAv1EncApp command)
            svtav1_call = mock_popen.call_args_list[1]
            svtav1_cmd = svtav1_call[0][0]
            assert '--pix_fmt' in svtav1_cmd
            assert 'yuv420p10le' in svtav1_cmd
            assert '--svtav1-params' in svtav1_cmd
            assert 'tune=0:enable-qm=1:qm-min=0' in svtav1_cmd
            assert '--crf' in svtav1_cmd
            assert '25' in svtav1_cmd

    def test_encode_segment_with_pre_expanded_params(self, ffmpeg_service, segment_info, tmp_path, mock_logger):
        """Test encoding a segment with pre-expanded parameters"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            # Pre-expanded form from CLI
            svtav1_args=['--preset', '4', '--crf', '30', '--enable-qm', '1', '--qm-min', '8', '--scd', '1']
        )

        # Mock FFmpeg process
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = iter([])
        mock_ffmpeg_process.wait.return_value = 0

        # Mock SvtAv1EncApp process
        mock_svtav1_process = Mock()
        mock_svtav1_process.stdout = iter([])
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.setup_segment_logger', return_value=mock_logger), \
             patch('av1_encoder.core.ffmpeg.cleanup_logger'):

            result = ffmpeg_service.encode_segment(segment_info, input_file, config)

            assert result is True

            # Verify -svtav1-params is expanded (SvtAv1EncApp command)
            svtav1_call = mock_popen.call_args_list[1]
            svtav1_cmd = svtav1_call[0][0]
            assert '--preset' in svtav1_cmd
            assert '4' in svtav1_cmd
            assert '--crf' in svtav1_cmd
            assert '30' in svtav1_cmd
            assert '--enable-qm' in svtav1_cmd
            assert '1' in svtav1_cmd
            assert '--qm-min' in svtav1_cmd
            assert '8' in svtav1_cmd
            assert '--scd' in svtav1_cmd
            # Since it's expanded, -svtav1-params itself should not be present
            assert '-svtav1-params' not in svtav1_cmd

    def test_encode_segment_failure(self, ffmpeg_service, segment_info, encoding_config, tmp_path, mock_logger):
        """Test that encoding a segment fails"""
        input_file = tmp_path / "input.mkv"

        # Mock FFmpeg process (success)
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = iter([])
        mock_ffmpeg_process.wait.return_value = 0

        # Mock SvtAv1EncApp process (failure)
        mock_svtav1_process = Mock()
        mock_svtav1_process.stdout = iter([])
        mock_svtav1_process.stderr = iter(["Error message"])
        mock_svtav1_process.wait.return_value = 1  # Error code

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect), \
             patch('av1_encoder.core.ffmpeg.setup_segment_logger', return_value=mock_logger), \
             patch('av1_encoder.core.ffmpeg.cleanup_logger'):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # Verify failure is returned
            assert result is False

    def test_encode_segment_logger_cleanup(self, ffmpeg_service, segment_info, encoding_config, tmp_path):
        """Test that logger handlers are cleaned up after encoding"""
        input_file = tmp_path / "input.mkv"

        # Mock FFmpeg process
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = iter([])
        mock_ffmpeg_process.wait.return_value = 0

        # Mock SvtAv1EncApp process
        mock_svtav1_process = Mock()
        mock_svtav1_process.stdout = iter([])
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        mock_logger = Mock()
        mock_logger.handlers = []

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect), \
             patch('av1_encoder.core.ffmpeg.setup_segment_logger', return_value=mock_logger), \
             patch('av1_encoder.core.ffmpeg.cleanup_logger') as mock_cleanup:

            ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # Verify cleanup was called
            mock_cleanup.assert_called_once_with(mock_logger)


