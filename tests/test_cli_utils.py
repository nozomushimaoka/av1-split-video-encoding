"""Tests for CLI utility functions"""

import pytest

from av1_encoder.cli_utils import (expand_audio_params, expand_ffmpeg_params,
                                   expand_svtav1_params)


class TestExpandSvtav1Params:
    """Tests for the expand_svtav1_params function"""

    def test_expand_single_param(self):
        """Test that a single parameter is expanded correctly"""
        result = expand_svtav1_params("crf=30")
        assert result == ['--crf', '30']

    def test_expand_multiple_params(self):
        """Test that multiple parameters are expanded correctly"""
        result = expand_svtav1_params("preset=4,crf=30,enable-qm=1")
        assert result == ['--preset', '4', '--crf', '30', '--enable-qm', '1']

    def test_handle_empty_string(self):
        """Test that an empty string returns an empty list"""
        result = expand_svtav1_params("")
        assert result == []

    def test_skip_params_without_equals(self):
        """Test that parameters without an equals sign are skipped"""
        result = expand_svtav1_params("preset=4,invalid,crf=30")
        assert result == ['--preset', '4', '--crf', '30']

    def test_param_with_equals_in_value(self):
        """Test that a parameter with an equals sign in the value is handled correctly"""
        result = expand_svtav1_params("key=value=with=equals")
        assert result == ['--key', 'value=with=equals']

    def test_numeric_params(self):
        """Test that numeric parameters are expanded correctly"""
        result = expand_svtav1_params("crf=0,preset=13,qm-min=8")
        assert result == ['--crf', '0', '--preset', '13', '--qm-min', '8']

    def test_complex_param_combination(self):
        """Test that a complex combination of parameters is expanded correctly"""
        result = expand_svtav1_params("preset=4,crf=30,enable-qm=1,qm-min=8,scd=1")
        assert result == [
            '--preset', '4',
            '--crf', '30',
            '--enable-qm', '1',
            '--qm-min', '8',
            '--scd', '1'
        ]

    def test_key_with_hyphen(self):
        """Test that key names containing hyphens are handled correctly"""
        result = expand_svtav1_params("enable-qm=1,qm-min=8")
        assert result == ['--enable-qm', '1', '--qm-min', '8']

    def test_param_with_empty_value(self):
        """Test that a parameter with an empty value is handled correctly"""
        result = expand_svtav1_params("key=")
        assert result == ['--key', '']

    def test_param_with_long_value(self):
        """Test that a parameter with a long value is handled correctly"""
        long_value = "a" * 100
        result = expand_svtav1_params(f"key={long_value}")
        assert result == ['--key', long_value]

    def test_param_with_special_chars_in_value(self):
        """Test that a parameter with special characters in the value is handled correctly"""
        result = expand_svtav1_params("key=value-with_special.chars")
        assert result == ['--key', 'value-with_special.chars']

    def test_commas_only_string(self):
        """Test that a string with only commas is handled correctly"""
        result = expand_svtav1_params(",,,")
        assert result == []

    def test_leading_and_trailing_commas(self):
        """Test that leading and trailing commas are handled correctly"""
        result = expand_svtav1_params(",preset=4,crf=30,")
        assert result == ['--preset', '4', '--crf', '30']

class TestExpandFfmpegParams:
    """Tests for the expand_ffmpeg_params function"""

    def test_expand_single_param(self):
        """Test that a single parameter is expanded correctly"""
        result = expand_ffmpeg_params("r=30")
        assert result == ['-r', '30']

    def test_expand_multiple_params(self):
        """Test that multiple parameters are expanded correctly"""
        result = expand_ffmpeg_params("vf=scale=1920:1080,r=30,c:v=libx264")
        assert result == ['-vf', 'scale=1920:1080', '-r', '30', '-c:v', 'libx264']

    def test_handle_empty_string(self):
        """Test that an empty string returns an empty list"""
        result = expand_ffmpeg_params("")
        assert result == []

    def test_skip_params_without_equals(self):
        """Test that parameters without an equals sign are skipped"""
        result = expand_ffmpeg_params("r=30,invalid,vf=scale=1920:1080")
        assert result == ['-r', '30', '-vf', 'scale=1920:1080']

    def test_param_with_equals_in_value(self):
        """Test that a parameter with an equals sign in the value is handled correctly"""
        result = expand_ffmpeg_params("metadata=key=value")
        assert result == ['-metadata', 'key=value']

    def test_param_with_colon_in_value(self):
        """Test that a parameter with a colon in the value (e.g., scale) is handled correctly"""
        result = expand_ffmpeg_params("vf=scale=1920:1080")
        assert result == ['-vf', 'scale=1920:1080']

    def test_complex_filter(self):
        """Test that a complex filter specification is handled correctly"""
        result = expand_ffmpeg_params("vf=scale=1920:1080:flags=lanczos,r=30")
        assert result == ['-vf', 'scale=1920:1080:flags=lanczos', '-r', '30']

    def test_codec_params(self):
        """Test that codec parameters are handled correctly"""
        result = expand_ffmpeg_params("c:v=libx264,c:a=aac")
        assert result == ['-c:v', 'libx264', '-c:a', 'aac']

    def test_param_with_empty_value(self):
        """Test that a parameter with an empty value is handled correctly"""
        result = expand_ffmpeg_params("key=")
        assert result == ['-key', '']

    def test_commas_only_string(self):
        """Test that a string with only commas is handled correctly"""
        result = expand_ffmpeg_params(",,,")
        assert result == []

    def test_leading_and_trailing_commas(self):
        """Test that leading and trailing commas are handled correctly"""
        result = expand_ffmpeg_params(",vf=scale=1920:1080,r=30,")
        assert result == ['-vf', 'scale=1920:1080', '-r', '30']

    def test_single_hyphen_prefix(self):
        """Test that a single hyphen prefix is correctly applied"""
        result = expand_ffmpeg_params("vf=yadif")
        assert result == ['-vf', 'yadif']

    def test_param_with_escaped_comma(self):
        """Test that \\, escaped commas are handled correctly"""
        result = expand_ffmpeg_params(r"vf=scale=1920:-1\,fps=30,pix_fmt=yuv420p10le")
        assert result == ['-vf', 'scale=1920:-1,fps=30', '-pix_fmt', 'yuv420p10le']

    def test_multiple_escaped_commas(self):
        """Test that multiple \\, escapes are handled correctly"""
        result = expand_ffmpeg_params(r"vf=eq=contrast=1.2\,brightness=0.1\,saturation=1.5")
        assert result == ['-vf', 'eq=contrast=1.2,brightness=0.1,saturation=1.5']

    def test_mixed_escaped_and_unescaped_commas(self):
        """Test that a mix of escaped and unescaped commas is handled correctly"""
        result = expand_ffmpeg_params(r"vf=scale=1920:-1\,fps=30,pix_fmt=yuv420p10le,r=60")
        assert result == ['-vf', 'scale=1920:-1,fps=30', '-pix_fmt', 'yuv420p10le', '-r', '60']

    def test_param_with_only_escaped_commas(self):
        """Test a parameter containing only escaped commas"""
        result = expand_ffmpeg_params(r"vf=hue=s=0\,eq=brightness=0.1")
        assert result == ['-vf', 'hue=s=0,eq=brightness=0.1']


class TestExpandAudioParams:
    """Tests for the expand_audio_params function"""

    def test_expand_single_param(self):
        """Test that a single parameter is expanded correctly"""
        result = expand_audio_params("c:a=aac")
        assert result == ['-c:a', 'aac']

    def test_expand_multiple_params(self):
        """Test that multiple parameters are expanded correctly"""
        result = expand_audio_params("c:a=aac,b:a=128k,ar=48000")
        assert result == ['-c:a', 'aac', '-b:a', '128k', '-ar', '48000']

    def test_handle_empty_string(self):
        """Test that an empty string returns an empty list"""
        result = expand_audio_params("")
        assert result == []

    def test_copy_codec(self):
        """Test that the copy codec is handled correctly"""
        result = expand_audio_params("c:a=copy")
        assert result == ['-c:a', 'copy']

    def test_opus_codec_and_params(self):
        """Test that the Opus codec and its parameters are handled correctly"""
        result = expand_audio_params("c:a=libopus,b:a=96k,ac=1")
        assert result == ['-c:a', 'libopus', '-b:a', '96k', '-ac', '1']

    def test_sample_rate_and_channels(self):
        """Test that sample rate and channel count are handled correctly"""
        result = expand_audio_params("c:a=aac,ar=48000,ac=2")
        assert result == ['-c:a', 'aac', '-ar', '48000', '-ac', '2']

    def test_quality_based_encoding(self):
        """Test that quality-based encoding (-q:a) is handled correctly"""
        result = expand_audio_params("c:a=libvorbis,q:a=5")
        assert result == ['-c:a', 'libvorbis', '-q:a', '5']

    def test_skip_params_without_equals(self):
        """Test that parameters without an equals sign are skipped"""
        result = expand_audio_params("c:a=aac,invalid,b:a=128k")
        assert result == ['-c:a', 'aac', '-b:a', '128k']

    def test_param_with_escaped_comma(self):
        """Test that \\, escaped commas are handled correctly"""
        result = expand_audio_params(r"c:a=aac,af=volume=0.5\,aformat=s16,b:a=128k")
        assert result == ['-c:a', 'aac', '-af', 'volume=0.5,aformat=s16', '-b:a', '128k']

    def test_param_with_empty_value(self):
        """Test that a parameter with an empty value is handled correctly"""
        result = expand_audio_params("c:a=")
        assert result == ['-c:a', '']

    def test_commas_only_string(self):
        """Test that a string with only commas is handled correctly"""
        result = expand_audio_params(",,,")
        assert result == []

    def test_leading_and_trailing_commas(self):
        """Test that leading and trailing commas are handled correctly"""
        result = expand_audio_params(",c:a=aac,b:a=128k,")
        assert result == ['-c:a', 'aac', '-b:a', '128k']

    def test_single_hyphen_prefix(self):
        """Test that a single hyphen prefix is correctly applied"""
        result = expand_audio_params("c:a=aac")
        assert result == ['-c:a', 'aac']

    def test_multiple_audio_stream_specifiers(self):
        """Test that multiple audio stream specifiers are handled correctly"""
        result = expand_audio_params("c:a:0=aac,c:a:1=libopus")
        assert result == ['-c:a:0', 'aac', '-c:a:1', 'libopus']
