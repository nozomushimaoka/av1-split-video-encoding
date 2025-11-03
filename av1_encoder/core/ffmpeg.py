import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path

from .config import EncodingConfig


@dataclass
class SegmentInfo:
    index: int
    start_time: int
    duration: int
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

        # FFmpegコマンド構築（Input Seekingで高速化）
        cmd = [
            'ffmpeg',
            '-ss', str(start_time),
            '-i', str(input_file)
        ]

        # 最終セグメント以外は-tオプションで長さを指定
        if not is_final_segment:
            cmd.extend(['-t', str(duration)])

        cmd.extend(['-c:v', 'libsvtav1'])

        # 追加オプション
        if config.extra_args:
            cmd.extend(config.extra_args)

        cmd.extend(['-an', '-y', str(segment_info.file)])

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
            segment_logger.debug(f"コマンド: {' '.join(cmd)}")

            # FFmpegをリアルタイムで実行し、出力をロガーでキャプチャ
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            # 出力を1行ずつ読み取ってログに記録
            if process.stdout:
                for line in process.stdout:
                    segment_logger.debug(line.rstrip())

            # プロセスの終了を待つ
            return_code = process.wait()

            if return_code != 0:
                segment_logger.error(f"エラー (終了コード: {return_code})")
                return False

            segment_logger.info(f"セグメント {segment_idx} 完了")
            return True

        finally:
            # ハンドラをクリーンアップ
            for handler in segment_logger.handlers[:]:
                handler.close()
                segment_logger.removeHandler(handler)

