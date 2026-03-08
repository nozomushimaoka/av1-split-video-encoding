"""Video metadata retrieval module

Uses ffprobe to retrieve video file metadata (duration, frame rate, etc.).
"""
import json
import subprocess
from pathlib import Path


class VideoProbe:
    """Class for retrieving video file metadata"""

    def get_duration(self, input_file: Path) -> float:
        """Get video duration in seconds

        Args:
            input_file: Path to the video file

        Returns:
            Duration in seconds

        Raises:
            subprocess.CalledProcessError: If ffprobe execution fails
            KeyError: If metadata does not contain duration
        """
        result = subprocess.run(
            [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_format', str(input_file)
            ],
            capture_output=True,
            text=True,
            check=True
        )

        data = json.loads(result.stdout)
        duration = float(data['format']['duration'])
        return duration

    def get_fps(self, input_file: Path) -> float:
        """Get video frame rate

        Args:
            input_file: Path to the video file

        Returns:
            Frame rate (fps)

        Raises:
            subprocess.CalledProcessError: If ffprobe execution fails
            KeyError: If metadata does not contain frame rate
        """
        result = subprocess.run(
            [
                'ffprobe', '-v', 'quiet', '-print_format', 'json',
                '-show_streams', '-select_streams', 'v:0', str(input_file)
            ],
            capture_output=True,
            text=True,
            check=True
        )

        data = json.loads(result.stdout)
        fps_str = data['streams'][0]['r_frame_rate']  # e.g. "24000/1001"

        # Convert fraction format to float
        if '/' in fps_str:
            num, den = map(int, fps_str.split('/'))
            return num / den
        else:
            return float(fps_str)

    def get_total_frames(self, input_file: Path) -> int:
        """Get total number of frames in the video

        Args:
            input_file: Path to the video file

        Returns:
            Total frame count
        """
        duration = self.get_duration(input_file)
        fps = self.get_fps(input_file)
        return int(duration * fps)
