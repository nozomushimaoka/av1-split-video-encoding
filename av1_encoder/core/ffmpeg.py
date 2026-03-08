"""Segment encoding module

Encodes video segments using FFmpeg and SvtAv1EncApp.
Delegates metadata retrieval to VideoProbe and command building to CommandBuilder.
"""
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path

from .command_builder import CommandBuilder
from .config import EncodingConfig
from .logging_config import cleanup_logger, setup_segment_logger
from .video_probe import VideoProbe


@dataclass
class SegmentInfo:
    index: int
    start_time: float
    duration: float
    is_final: bool
    file: Path
    log_file: Path


class FFmpegService:
    """Segment encoding service

    Provides video metadata retrieval and segment encoding.
    """

    def __init__(self) -> None:
        self._video_probe = VideoProbe()
        self._command_builder = CommandBuilder()

    def get_duration(self, input_file: Path) -> float:
        """Get video duration in seconds"""
        return self._video_probe.get_duration(input_file)

    def get_fps(self, input_file: Path) -> float:
        """Get video frame rate"""
        return self._video_probe.get_fps(input_file)

    def _build_ffmpeg_command(
        self,
        input_file: Path,
        start_time: float,
        duration: float,
        is_final_segment: bool,
        config: EncodingConfig
    ) -> list[str]:
        """Build FFmpeg decode command (outputs Y4M format to stdout)"""
        return self._command_builder.build_ffmpeg_decode_command(
            input_file=input_file,
            start_time=start_time,
            duration=duration,
            is_final_segment=is_final_segment,
            config=config
        )

    def _build_svtav1_command(
        self,
        output_file: Path,
        config: EncodingConfig
    ) -> list[str]:
        """Build SvtAv1EncApp command"""
        return self._command_builder.build_svtav1_encode_command(
            output_file=output_file,
            config=config
        )

    def encode_segment(
        self,
        segment_info: SegmentInfo,
        input_file: Path,
        config: EncodingConfig
    ) -> bool:
        segment_idx = segment_info.index
        start_time = segment_info.start_time
        duration = segment_info.duration
        is_final_segment = segment_info.is_final

        # Build FFmpeg decode command (outputs Y4M format to stdout)
        ffmpeg_cmd = self._build_ffmpeg_command(
            input_file=input_file,
            start_time=start_time,
            duration=duration,
            is_final_segment=is_final_segment,
            config=config
        )

        # Build SvtAv1EncApp command
        svtav1_cmd = self._build_svtav1_command(
            output_file=segment_info.file,
            config=config
        )

        # Create segment-specific logger
        segment_logger = setup_segment_logger(
            segment_idx=segment_idx,
            log_file=segment_info.log_file
        )

        # Execute
        try:
            segment_logger.debug(f"FFmpeg command: {' '.join(ffmpeg_cmd)}")
            segment_logger.debug(f"SvtAv1EncApp command: {' '.join(svtav1_cmd)}")

            # Start FFmpeg process (pipe stdout)
            # Windows requires larger buffer for binary pipe to avoid deadlock
            ffmpeg_process = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=65536  # 64KB buffer for cross-platform reliability
            )

            # Start SvtAv1EncApp process (receives stdin from FFmpeg)
            svtav1_process = subprocess.Popen(
                svtav1_cmd,
                stdin=ffmpeg_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1  # Line buffering is acceptable for text mode
            )

            # Close FFmpeg stdout (SvtAv1EncApp takes full control)
            if ffmpeg_process.stdout:
                ffmpeg_process.stdout.close()

            # Read FFmpeg stderr in a separate thread
            def read_ffmpeg_stderr():
                if ffmpeg_process.stderr:
                    for line in ffmpeg_process.stderr:
                        decoded_line = line.decode('utf-8', errors='replace').rstrip()
                        segment_logger.debug(f"[FFmpeg] {decoded_line}")

            # Read SvtAv1EncApp stdout in a separate thread (prevent buffer blocking)
            def read_svtav1_stdout():
                if svtav1_process.stdout:
                    for line in svtav1_process.stdout:
                        segment_logger.debug(f"[SvtAv1EncApp stdout] {line.rstrip()}")

            # Read SvtAv1EncApp stderr in a separate thread (prevent buffer blocking)
            def read_svtav1_stderr():
                if svtav1_process.stderr:
                    for line in svtav1_process.stderr:
                        segment_logger.debug(f"[SvtAv1EncApp] {line.rstrip()}")

            ffmpeg_thread = threading.Thread(target=read_ffmpeg_stderr)
            ffmpeg_thread.start()

            svtav1_stdout_thread = threading.Thread(target=read_svtav1_stdout)
            svtav1_stdout_thread.start()

            svtav1_stderr_thread = threading.Thread(target=read_svtav1_stderr)
            svtav1_stderr_thread.start()

            # Wait for SvtAv1EncApp to finish
            svtav1_return_code = svtav1_process.wait()

            # Wait for FFmpeg to finish
            ffmpeg_return_code = ffmpeg_process.wait()

            # Wait for all threads to finish
            ffmpeg_thread.join(timeout=5)
            svtav1_stdout_thread.join(timeout=5)
            svtav1_stderr_thread.join(timeout=5)

            # If either process failed
            if ffmpeg_return_code != 0:
                segment_logger.error(f"FFmpeg error (exit code: {ffmpeg_return_code})")
                return False

            if svtav1_return_code != 0:
                segment_logger.error(f"SvtAv1EncApp error (exit code: {svtav1_return_code})")
                return False

            segment_logger.info(f"Segment {segment_idx} completed")
            return True

        finally:
            # Clean up handlers
            cleanup_logger(segment_logger)
