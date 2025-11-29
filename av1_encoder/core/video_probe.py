"""動画メタデータ取得モジュール

ffprobeを使用して動画ファイルのメタデータ（再生時間、フレームレートなど）を取得する。
"""
import json
import subprocess
from pathlib import Path


class VideoProbe:
    """動画ファイルのメタデータを取得するクラス"""

    def get_duration(self, input_file: Path) -> float:
        """動画の再生時間（秒）を取得する

        Args:
            input_file: 動画ファイルのパス

        Returns:
            再生時間（秒）

        Raises:
            subprocess.CalledProcessError: ffprobeの実行に失敗した場合
            KeyError: メタデータに再生時間が含まれていない場合
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
        """動画のフレームレートを取得する

        Args:
            input_file: 動画ファイルのパス

        Returns:
            フレームレート（fps）

        Raises:
            subprocess.CalledProcessError: ffprobeの実行に失敗した場合
            KeyError: メタデータにフレームレートが含まれていない場合
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
        fps_str = data['streams'][0]['r_frame_rate']  # 例: "24000/1001"

        # 分数形式をfloatに変換
        if '/' in fps_str:
            num, den = map(int, fps_str.split('/'))
            return num / den
        else:
            return float(fps_str)
