"""CLI ユーティリティ関数のテスト"""

import pytest

from av1_encoder.cli_utils import expand_ffmpeg_params, expand_svtav1_params


class TestExpandSvtav1Params:
    """expand_svtav1_params関数のテスト"""

    def test_単一パラメータを展開(self):
        """単一のパラメータを正しく展開することをテスト"""
        result = expand_svtav1_params("crf=30")
        assert result == ['--crf', '30']

    def test_複数パラメータを展開(self):
        """複数のパラメータを正しく展開することをテスト"""
        result = expand_svtav1_params("preset=4:crf=30:enable-qm=1")
        assert result == ['--preset', '4', '--crf', '30', '--enable-qm', '1']

    def test_空文字列を処理(self):
        """空文字列の場合に空リストを返すことをテスト"""
        result = expand_svtav1_params("")
        assert result == []

    def test_等号を含まないパラメータはスキップ(self):
        """等号を含まないパラメータはスキップされることをテスト"""
        result = expand_svtav1_params("preset=4:invalid:crf=30")
        assert result == ['--preset', '4', '--crf', '30']

    def test_値に等号を含むパラメータ(self):
        """値に等号を含むパラメータを正しく処理することをテスト"""
        result = expand_svtav1_params("key=value=with=equals")
        assert result == ['--key', 'value=with=equals']

    def test_数値パラメータ(self):
        """数値パラメータを正しく展開することをテスト"""
        result = expand_svtav1_params("crf=0:preset=13:qm-min=8")
        assert result == ['--crf', '0', '--preset', '13', '--qm-min', '8']

    def test_複雑なパラメータ組み合わせ(self):
        """複雑なパラメータの組み合わせを正しく展開することをテスト"""
        result = expand_svtav1_params("preset=4:crf=30:enable-qm=1:qm-min=8:scd=1")
        assert result == [
            '--preset', '4',
            '--crf', '30',
            '--enable-qm', '1',
            '--qm-min', '8',
            '--scd', '1'
        ]

    def test_ハイフンを含むキー(self):
        """ハイフンを含むキー名を正しく処理することをテスト"""
        result = expand_svtav1_params("enable-qm=1:qm-min=8")
        assert result == ['--enable-qm', '1', '--qm-min', '8']

    def test_空の値を持つパラメータ(self):
        """空の値を持つパラメータを正しく処理することをテスト"""
        result = expand_svtav1_params("key=")
        assert result == ['--key', '']

    def test_長い値を持つパラメータ(self):
        """長い値を持つパラメータを正しく処理することをテスト"""
        long_value = "a" * 100
        result = expand_svtav1_params(f"key={long_value}")
        assert result == ['--key', long_value]

    def test_特殊文字を含む値(self):
        """特殊文字を含む値を正しく処理することをテスト"""
        result = expand_svtav1_params("key=value-with_special.chars")
        assert result == ['--key', 'value-with_special.chars']

    def test_コロンのみの文字列(self):
        """コロンのみの文字列を正しく処理することをテスト"""
        result = expand_svtav1_params(":::")
        assert result == []

    def test_先頭と末尾にコロン(self):
        """先頭と末尾にコロンがある場合を正しく処理することをテスト"""
        result = expand_svtav1_params(":preset=4:crf=30:")
        assert result == ['--preset', '4', '--crf', '30']

class TestExpandFfmpegParams:
    """expand_ffmpeg_params関数のテスト"""

    def test_単一パラメータを展開(self):
        """単一のパラメータを正しく展開することをテスト"""
        result = expand_ffmpeg_params("r=30")
        assert result == ['-r', '30']

    def test_複数パラメータを展開(self):
        """複数のパラメータを正しく展開することをテスト"""
        result = expand_ffmpeg_params("vf=scale=1920:1080,r=30,c:v=libx264")
        assert result == ['-vf', 'scale=1920:1080', '-r', '30', '-c:v', 'libx264']

    def test_空文字列を処理(self):
        """空文字列の場合に空リストを返すことをテスト"""
        result = expand_ffmpeg_params("")
        assert result == []

    def test_等号を含まないパラメータはスキップ(self):
        """等号を含まないパラメータはスキップされることをテスト"""
        result = expand_ffmpeg_params("r=30,invalid,vf=scale=1920:1080")
        assert result == ['-r', '30', '-vf', 'scale=1920:1080']

    def test_値に等号を含むパラメータ(self):
        """値に等号を含むパラメータを正しく処理することをテスト"""
        result = expand_ffmpeg_params("metadata=key=value")
        assert result == ['-metadata', 'key=value']

    def test_値にコロンを含むパラメータ(self):
        """値にコロンを含むパラメータ（scale等）を正しく処理することをテスト"""
        result = expand_ffmpeg_params("vf=scale=1920:1080")
        assert result == ['-vf', 'scale=1920:1080']

    def test_複雑なフィルター(self):
        """複雑なフィルター指定を正しく処理することをテスト"""
        result = expand_ffmpeg_params("vf=scale=1920:1080:flags=lanczos,r=30")
        assert result == ['-vf', 'scale=1920:1080:flags=lanczos', '-r', '30']

    def test_コーデックパラメータ(self):
        """コーデックパラメータを正しく処理することをテスト"""
        result = expand_ffmpeg_params("c:v=libx264,c:a=aac")
        assert result == ['-c:v', 'libx264', '-c:a', 'aac']

    def test_空の値を持つパラメータ(self):
        """空の値を持つパラメータを正しく処理することをテスト"""
        result = expand_ffmpeg_params("key=")
        assert result == ['-key', '']

    def test_カンマのみの文字列(self):
        """カンマのみの文字列を正しく処理することをテスト"""
        result = expand_ffmpeg_params(",,,")
        assert result == []

    def test_先頭と末尾にカンマ(self):
        """先頭と末尾にカンマがある場合を正しく処理することをテスト"""
        result = expand_ffmpeg_params(",vf=scale=1920:1080,r=30,")
        assert result == ['-vf', 'scale=1920:1080', '-r', '30']

    def test_ハイフン付きプレフィックス(self):
        """ハイフン1つがプレフィックスとして正しく付与されることをテスト"""
        result = expand_ffmpeg_params("vf=yadif")
        assert result == ['-vf', 'yadif']

    def test_エスケープされたカンマを含むパラメータ(self):
        """\\,でエスケープされたカンマを正しく処理することをテスト"""
        result = expand_ffmpeg_params(r"vf=scale=1920:-1\,fps=30,pix_fmt=yuv420p10le")
        assert result == ['-vf', 'scale=1920:-1,fps=30', '-pix_fmt', 'yuv420p10le']

    def test_複数のエスケープされたカンマ(self):
        """複数の\\,エスケープを正しく処理することをテスト"""
        result = expand_ffmpeg_params(r"vf=eq=contrast=1.2\,brightness=0.1\,saturation=1.5")
        assert result == ['-vf', 'eq=contrast=1.2,brightness=0.1,saturation=1.5']

    def test_エスケープと非エスケープの混在(self):
        """エスケープされたカンマと通常のカンマが混在する場合をテスト"""
        result = expand_ffmpeg_params(r"vf=scale=1920:-1\,fps=30,pix_fmt=yuv420p10le,r=60")
        assert result == ['-vf', 'scale=1920:-1,fps=30', '-pix_fmt', 'yuv420p10le', '-r', '60']

    def test_エスケープのみのパラメータ(self):
        """エスケープされたカンマのみを含むパラメータをテスト"""
        result = expand_ffmpeg_params(r"vf=hue=s=0\,eq=brightness=0.1")
        assert result == ['-vf', 'hue=s=0,eq=brightness=0.1']
