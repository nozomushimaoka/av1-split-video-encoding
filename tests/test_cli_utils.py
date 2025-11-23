"""CLI ユーティリティ関数のテスト"""

import pytest

from av1_encoder.cli_utils import expand_svtav1_params


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
