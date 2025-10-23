import json
import logging
import subprocess
from pathlib import Path
from typing import List

from .config import EncodingConfig, SegmentInfo


class FFmpegService:
    def get_duration(self, input_file: Path) -> float:
        """
        動画の長さを秒数で取得
        """
        try:
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

        except (subprocess.CalledProcessError, KeyError, ValueError, json.JSONDecodeError) as e:
            raise RuntimeError(f"動画の長さ取得に失敗: {e}") from e

    def encode_segment(
        self,
        segment_info: SegmentInfo,
        input_file: Path,
        segments_dir: Path,
        logs_dir: Path,
        config: EncodingConfig
    ) -> bool:
        segment_idx = segment_info.index
        start_time = segment_info.start_time
        duration = segment_info.duration
        total_duration = segment_info.total_duration

        # ファイル名（0埋め4桁）
        segment_file = segments_dir / f"segment_{segment_idx:04d}.mp4"
        log_file = logs_dir / f"segment_{segment_idx:04d}.log"

        # 終了時刻計算
        end_time = start_time + duration

        # 最終セグメントの長さ調整
        actual_duration = duration
        if end_time > total_duration:
            actual_duration = total_duration - start_time

        # FFmpegコマンド構築（Input Seekingで高速化）
        cmd = [
            'ffmpeg',
            '-ss', str(start_time),
            '-i', str(input_file),
            '-t', str(actual_duration),
            '-c:v', 'libsvtav1'
        ]

        # オプション追加
        if config.crf is not None:
            cmd.extend(['-crf', str(config.crf)])
        if config.preset:
            cmd.extend(['-preset', config.preset])
        if config.keyint is not None:
            cmd.extend(['-g', str(config.keyint), '-keyint_min', str(config.keyint)])

        cmd.extend(['-an', '-y', str(segment_file)])

        # セグメント専用ロガーを作成
        segment_logger = logging.getLogger(f"av1_encoder.segment_{segment_idx}")
        segment_logger.setLevel(logging.DEBUG)  # DEBUGレベルでFFmpeg出力を記録
        segment_logger.handlers.clear()
        segment_logger.propagate = False

        # ファイルハンドラを追加
        file_handler = logging.FileHandler(log_file, encoding='utf-8', mode='w')
        file_handler.setLevel(logging.DEBUG)  # ファイルには全て記録
        formatter = logging.Formatter('[%(asctime)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(formatter)
        segment_logger.addHandler(file_handler)

        # 実行
        try:
            segment_logger.info(f"セグメント {segment_idx} 開始")
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

    def concat_segments(
        self,
        segment_files: List[Path],
        concat_file: Path,
        output_file: Path
    ) -> None:
        # concat.txtを作成
        with open(concat_file, 'w', encoding='utf-8') as f:
            for segment_file in segment_files:
                abs_path = segment_file.resolve()
                f.write(f"file '{abs_path}'\n")

        # ビデオを結合
        try:
            subprocess.run(
                [
                    'ffmpeg', '-f', 'concat', '-safe', '0',
                    '-i', str(concat_file),
                    '-c', 'copy', str(output_file)
                ],
                capture_output=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode('utf-8', errors='ignore')
            raise RuntimeError(f"ビデオ結合失敗: {error_msg}") from e

    def extract_audio(self, input_file: Path, audio_file: Path) -> None:
        try:
            subprocess.run(
                [
                    'ffmpeg', '-i', str(input_file),
                    '-map', '0:a',
                    '-c', 'copy', str(audio_file)
                ],
                capture_output=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode('utf-8', errors='ignore')
            raise RuntimeError(f"音声抽出失敗: {error_msg}") from e

    def merge_video_audio(
        self,
        video_file: Path,
        audio_file: Path,
        output_file: Path
    ) -> None:
        try:
            subprocess.run(
                [
                    'ffmpeg',
                    '-i', str(video_file),
                    '-i', str(audio_file),
                    '-c', 'copy',
                    '-map', '0:v:0', '-map', '1:a',
                    str(output_file)
                ],
                capture_output=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode('utf-8', errors='ignore')
            raise RuntimeError(f"多重化失敗: {error_msg}") from e
