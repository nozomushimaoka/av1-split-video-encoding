"""Encode command builder module

Builds command-line arguments for FFmpeg and SvtAv1EncApp.
"""
from pathlib import Path

from .config import EncodingConfig


class CommandBuilder:
    """Class for building encode commands"""

    def build_ffmpeg_decode_command(
        self,
        input_file: Path,
        start_time: float,
        duration: float,
        is_final_segment: bool,
        config: EncodingConfig
    ) -> list[str]:
        """Build FFmpeg decode command (outputs Y4M format to stdout)

        Args:
            input_file: Path to the input video file
            start_time: Start time in seconds
            duration: Segment length in seconds
            is_final_segment: Whether this is the final segment
            config: Encoding configuration

        Returns:
            List of FFmpeg command arguments
        """
        ffmpeg_cmd = ['ffmpeg']
        if config.hardware_decode:
            ffmpeg_cmd.extend(['-hwaccel', config.hardware_decode,
                               '-hwaccel_output_format', config.hardware_decode])
            if config.hardware_decode_device:
                ffmpeg_cmd.extend(['-hwaccel_device', config.hardware_decode_device])
        ffmpeg_cmd.extend(['-ss', str(start_time), '-i', str(input_file)])

        # Specify duration with -t option for all segments except the final one
        if not is_final_segment:
            ffmpeg_cmd.extend(['-t', str(duration)])

        # Additional FFmpeg parameters (already expanded)
        if config.ffmpeg_args:
            ffmpeg_cmd.extend(config.ffmpeg_args)

        # Pipe output in Y4M format
        ffmpeg_cmd.extend([
            '-f', 'yuv4mpegpipe',
            '-strict', '-1',
            '-'
        ])

        return ffmpeg_cmd

    def build_svtav1_encode_command(
        self,
        output_file: Path,
        config: EncodingConfig
    ) -> list[str]:
        """Build SvtAv1EncApp command

        Args:
            output_file: Path to the output file
            config: Encoding configuration

        Returns:
            List of SvtAv1EncApp command arguments
        """
        svtav1_cmd = [
            'SvtAv1EncApp',
            '-i', 'stdin',
            '--keyint', str(config.gop_size)
        ]

        # Additional options (SvtAv1EncApp format, already expanded)
        if config.svtav1_args:
            svtav1_cmd.extend(config.svtav1_args)

        # Specify output file
        svtav1_cmd.extend(['-b', str(output_file)])

        return svtav1_cmd
