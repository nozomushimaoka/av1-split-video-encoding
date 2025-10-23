"""FFmpeg操作サービス"""

import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Tuple, List

from .config import EncodingConfig, SegmentInfo


class FFmpegService:
    """FFmpeg操作を担当するクラス"""

    def __init__(self, logger: logging.Logger):
        """
        Args:
            logger: ロガーインスタンス
        """
        self.logger = logger

    def get_duration(self, input_file: Path) -> float:
        """
        動画の長さを取得

        Args:
            input_file: 入力ファイルパス

        Returns:
            動画の長さ（秒）

        Raises:
            RuntimeError: 取得失敗時
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
            self.logger.info(f"動画の長さ: {duration:.2f}秒 ({self._format_timecode(int(duration))})")
            return duration

        except (subprocess.CalledProcessError, KeyError, ValueError, json.JSONDecodeError) as e:
            self.logger.error(f"動画の長さ取得に失敗: {e}")
            raise RuntimeError(f"動画の長さ取得に失敗: {e}") from e

    def has_audio_stream(self, input_file: Path) -> bool:
        """
        音声ストリームの有無を確認

        Args:
            input_file: 入力ファイルパス

        Returns:
            音声ストリームがあればTrue
        """
        try:
            result = subprocess.run(
                [
                    'ffprobe', '-v', 'quiet', '-select_streams', 'a',
                    '-show_entries', 'stream=codec_type', '-of', 'csv=p=0',
                    str(input_file)
                ],
                capture_output=True,
                text=True,
                check=True
            )
            return bool(result.stdout.strip())
        except subprocess.CalledProcessError:
            return False

    def encode_segment(
        self,
        segment_info: SegmentInfo,
        input_file: Path,
        segments_dir: Path,
        logs_dir: Path,
        config: EncodingConfig
    ) -> Tuple[int, bool, str]:
        """
        セグメントをエンコード（並列実行される関数）

        Args:
            segment_info: セグメント情報
            input_file: 入力ファイルパス
            segments_dir: セグメント出力ディレクトリ
            logs_dir: ログ出力ディレクトリ
            config: エンコード設定

        Returns:
            (セグメントインデックス, 成功フラグ, メッセージ)
        """
        segment_idx = segment_info.index
        start_time = segment_info.start_time
        duration = segment_info.duration
        total_duration = segment_info.total_duration

        # ファイル名（0埋め4桁）
        segment_file = segments_dir / f"segment_{segment_idx:04d}.mp4"
        log_file = logs_dir / f"segment_{segment_idx:04d}.log"

        # タイムコード
        start_tc = self._format_timecode(start_time)
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

        # 実行
        try:
            with open(log_file, 'w', encoding='utf-8') as log_fh:
                log_fh.write(
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"セグメント {segment_idx} 開始\n"
                )
                log_fh.write(f"コマンド: {' '.join(cmd)}\n\n")

                subprocess.run(
                    cmd,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    check=True
                )

                log_fh.write(
                    f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"セグメント {segment_idx} 完了\n"
                )

            return (segment_idx, True, f"完了: セグメント {segment_idx:04d}")

        except subprocess.CalledProcessError as e:
            error_msg = f"エラー: セグメント {segment_idx:04d} - ログ: {log_file}"
            with open(log_file, 'a', encoding='utf-8') as log_fh:
                log_fh.write(
                    f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"エラー (終了コード: {e.returncode})\n"
                )
            return (segment_idx, False, error_msg)

    def concat_segments(
        self,
        segment_files: List[Path],
        concat_file: Path,
        output_file: Path
    ) -> None:
        """
        セグメントを結合

        Args:
            segment_files: セグメントファイルリスト
            concat_file: concat用ファイルパス
            output_file: 出力ファイルパス

        Raises:
            RuntimeError: 結合失敗時
        """
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
            self.logger.error(f"ビデオ結合失敗: {error_msg}")
            raise RuntimeError(f"ビデオ結合失敗: {error_msg}") from e

    def extract_audio(self, input_file: Path, audio_file: Path) -> None:
        """
        音声を抽出

        Args:
            input_file: 入力ファイルパス
            audio_file: 音声出力ファイルパス

        Raises:
            RuntimeError: 抽出失敗時
        """
        try:
            subprocess.run(
                [
                    'ffmpeg', '-i', str(input_file),
                    '-vn', '-c:a', 'aac', str(audio_file)
                ],
                capture_output=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode('utf-8', errors='ignore')
            self.logger.error(f"音声抽出失敗: {error_msg}")
            raise RuntimeError(f"音声抽出失敗: {error_msg}") from e

    def merge_video_audio(
        self,
        video_file: Path,
        audio_file: Path,
        output_file: Path
    ) -> None:
        """
        ビデオと音声を多重化

        Args:
            video_file: ビデオファイルパス
            audio_file: 音声ファイルパス
            output_file: 出力ファイルパス

        Raises:
            RuntimeError: 多重化失敗時
        """
        try:
            subprocess.run(
                [
                    'ffmpeg',
                    '-i', str(video_file),
                    '-i', str(audio_file),
                    '-c:v', 'copy', '-c:a', 'copy',
                    '-map', '0:v:0', '-map', '1:a',
                    str(output_file)
                ],
                capture_output=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.decode('utf-8', errors='ignore')
            self.logger.error(f"多重化失敗: {error_msg}")
            raise RuntimeError(f"多重化失敗: {error_msg}") from e

    @staticmethod
    def _format_timecode(seconds: int) -> str:
        """タイムコードをHH:MM:SS形式にフォーマット"""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
