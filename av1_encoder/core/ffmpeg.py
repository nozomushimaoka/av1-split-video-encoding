import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import List

from .config import EncodingConfig


@dataclass
class SegmentInfo:
    index: int
    start_time: float
    duration: float
    is_final: bool
    file: Path
    log_file: Path


def _convert_ffmpeg_args_to_svtav1(args: List[str]) -> List[str]:
    """FFmpeg形式のパラメータをSvtAv1EncApp形式に変換する

    例: ['-crf', '30', '-preset', '6'] -> ['--crf', '30', '--preset', '6']
    """
    converted = []
    for arg in args:
        if arg.startswith('-') and not arg.startswith('--'):
            # 単一ハイフンを二重ハイフンに変換（-crf -> --crf）
            converted.append('-' + arg)
        else:
            converted.append(arg)
    return converted


class FFmpegService:
    def get_duration(self, input_file: Path) -> float:
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
        """動画のフレームレートを取得する"""
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

        # FFmpegデコードコマンド構築（Y4M形式でstdoutに出力）
        ffmpeg_cmd = [
            'ffmpeg',
            '-ss', str(start_time),
            '-i', str(input_file)
        ]

        # 最終セグメント以外は-tオプションで長さを指定
        if not is_final_segment:
            ffmpeg_cmd.extend(['-t', str(duration)])

        # Y4M形式でパイプ出力（10-bit）
        ffmpeg_cmd.extend([
            '-f', 'yuv4mpegpipe',
            '-pix_fmt', 'yuv420p10le',
            '-strict', '-1',
            '-'
        ])

        # SvtAv1EncAppコマンド構築
        svtav1_cmd = [
            'SvtAv1EncApp',
            '-i', 'stdin',
            '--keyint', str(config.gop_size)
        ]

        # 追加オプション（FFmpeg形式からSvtAv1EncApp形式に変換）
        if config.extra_args:
            converted_args = _convert_ffmpeg_args_to_svtav1(config.extra_args)
            svtav1_cmd.extend(converted_args)

        # 出力ファイル指定
        svtav1_cmd.extend(['-b', str(segment_info.file)])

        # セグメント専用ロガーを作成
        segment_logger = logging.getLogger(f"av1_encoder.segment_{segment_idx}")
        segment_logger.setLevel(logging.DEBUG)  # DEBUGレベルでFFmpeg出力を記録
        segment_logger.handlers.clear()
        segment_logger.propagate = False

        # ファイルハンドラを追加
        file_handler = logging.FileHandler(segment_info.log_file, encoding='utf-8', mode='w')
        file_handler.setLevel(logging.DEBUG)  # ファイルには全て記録
        formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        segment_logger.addHandler(file_handler)

        # 実行
        try:
            segment_logger.debug(f"FFmpegコマンド: {' '.join(ffmpeg_cmd)}")
            segment_logger.debug(f"SvtAv1EncAppコマンド: {' '.join(svtav1_cmd)}")

            # FFmpegプロセスを起動（stdoutをパイプ出力）
            ffmpeg_process = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0
            )

            # SvtAv1EncAppプロセスを起動（stdinをFFmpegから受け取り）
            svtav1_process = subprocess.Popen(
                svtav1_cmd,
                stdin=ffmpeg_process.stdout,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )

            # FFmpegのstdoutを閉じる（SvtAv1EncAppが完全に制御するため）
            if ffmpeg_process.stdout:
                ffmpeg_process.stdout.close()

            # SvtAv1EncAppの出力を1行ずつ読み取ってログに記録
            if svtav1_process.stderr:
                for line in svtav1_process.stderr:
                    segment_logger.debug(f"[SvtAv1EncApp] {line.rstrip()}")

            # SvtAv1EncAppの終了を待つ
            svtav1_return_code = svtav1_process.wait()

            # FFmpegの終了を待つ
            ffmpeg_return_code = ffmpeg_process.wait()

            # FFmpegのエラー出力をログに記録
            if ffmpeg_process.stderr:
                ffmpeg_errors = ffmpeg_process.stderr.read().decode('utf-8', errors='replace')
                if ffmpeg_errors:
                    segment_logger.debug(f"[FFmpeg stderr] {ffmpeg_errors}")

            # どちらかのプロセスが失敗した場合
            if ffmpeg_return_code != 0:
                segment_logger.error(f"FFmpegエラー (終了コード: {ffmpeg_return_code})")
                return False

            if svtav1_return_code != 0:
                segment_logger.error(f"SvtAv1EncAppエラー (終了コード: {svtav1_return_code})")
                return False

            segment_logger.info(f"セグメント {segment_idx} 完了")
            return True

        finally:
            # ハンドラをクリーンアップ
            for handler in segment_logger.handlers[:]:
                handler.close()
                segment_logger.removeHandler(handler)

