"""エンコードコマンド構築モジュール

FFmpegおよびSvtAv1EncAppのコマンドライン引数を構築する。
"""
from pathlib import Path

from .config import EncodingConfig


class CommandBuilder:
    """エンコードコマンドを構築するクラス"""

    def build_ffmpeg_decode_command(
        self,
        input_file: Path,
        start_time: float,
        duration: float,
        is_final_segment: bool,
        config: EncodingConfig
    ) -> list[str]:
        """FFmpegデコードコマンドを構築（Y4M形式でstdoutに出力）

        Args:
            input_file: 入力動画ファイルのパス
            start_time: 開始時間（秒）
            duration: セグメントの長さ（秒）
            is_final_segment: 最終セグメントかどうか
            config: エンコード設定

        Returns:
            FFmpegコマンドの引数リスト
        """
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

    def build_svtav1_encode_command(
        self,
        output_file: Path,
        config: EncodingConfig
    ) -> list[str]:
        """SvtAv1EncAppコマンドを構築

        Args:
            output_file: 出力ファイルのパス
            config: エンコード設定

        Returns:
            SvtAv1EncAppコマンドの引数リスト
        """
        svtav1_cmd = [
            'SvtAv1EncApp',
            '-i', 'stdin',
            '--keyint', str(config.gop_size)
        ]

        # 追加オプション（SvtAv1EncApp形式、既に展開済み）
        if config.svtav1_args:
            svtav1_cmd.extend(config.svtav1_args)

        # 出力ファイル指定
        svtav1_cmd.extend(['-b', str(output_file)])

        return svtav1_cmd
