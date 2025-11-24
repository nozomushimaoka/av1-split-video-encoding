import json
import logging
import subprocess
import threading
from dataclasses import dataclass
from pathlib import Path

from .config import EncodingConfig


@dataclass
class SegmentInfo:
    index: int
    start_time: float
    duration: float
    is_final: bool
    file: Path
    log_file: Path


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

    def _build_ffmpeg_command(
        self,
        input_file: Path,
        start_time: float,
        duration: float,
        is_final_segment: bool,
        config: EncodingConfig
    ) -> list[str]:
        """FFmpegデコードコマンドを構築（Y4M形式でstdoutに出力）"""
        ffmpeg_cmd = [
            'ffmpeg',
            '-ss', str(start_time),
            '-i', str(input_file)
        ]

        # 最終セグメント以外は-tオプションで長さを指定
        if not is_final_segment:
            ffmpeg_cmd.extend(['-t', str(duration)])

        # 追加のFFmpegパラメータ（既に展開済み）
        if config.ffmpeg_args:
            ffmpeg_cmd.extend(config.ffmpeg_args)

        # Y4M形式でパイプ出力
        ffmpeg_cmd.extend([
            '-f', 'yuv4mpegpipe',
            '-strict', '-1',
            '-'
        ])

        return ffmpeg_cmd

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
        ffmpeg_cmd = self._build_ffmpeg_command(
            input_file=input_file,
            start_time=start_time,
            duration=duration,
            is_final_segment=is_final_segment,
            config=config
        )

        # SvtAv1EncAppコマンド構築
        svtav1_cmd = [
            'SvtAv1EncApp',
            '-i', 'stdin',
            '--keyint', str(config.gop_size)
        ]

        # 追加オプション（SvtAv1EncApp形式、既に展開済み）
        if config.svtav1_args:
            svtav1_cmd.extend(config.svtav1_args)

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

            # FFmpegのstderrを別スレッドで読み取る
            def read_ffmpeg_stderr():
                if ffmpeg_process.stderr:
                    for line in ffmpeg_process.stderr:
                        decoded_line = line.decode('utf-8', errors='replace').rstrip()
                        segment_logger.debug(f"[FFmpeg] {decoded_line}")

            # SvtAv1EncAppのstdoutを別スレッドで読み取る（バッファ詰まり防止）
            def read_svtav1_stdout():
                if svtav1_process.stdout:
                    for line in svtav1_process.stdout:
                        segment_logger.debug(f"[SvtAv1EncApp stdout] {line.rstrip()}")

            # SvtAv1EncAppのstderrを別スレッドで読み取る（バッファ詰まり防止）
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

            # SvtAv1EncAppの終了を待つ
            svtav1_return_code = svtav1_process.wait()

            # FFmpegの終了を待つ
            ffmpeg_return_code = ffmpeg_process.wait()

            # 全スレッドの終了を待つ
            ffmpeg_thread.join(timeout=5)
            svtav1_stdout_thread.join(timeout=5)
            svtav1_stderr_thread.join(timeout=5)

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

