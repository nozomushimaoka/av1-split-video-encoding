from unittest.mock import Mock, patch, mock_open
import subprocess
import pytest

from av1_encoder.ffmpeg import FFmpegService, SegmentInfo
from av1_encoder.config import EncodingConfig


@pytest.fixture
def ffmpeg_service():
    """FFmpegServiceインスタンスを作成するフィクスチャ"""
    return FFmpegService()


@pytest.fixture
def segment_info(tmp_path):
    """テスト用のSegmentInfoを作成するフィクスチャ"""
    return SegmentInfo(
        index=0,
        start_time=0,
        duration=60,
        is_final=False,
        file=tmp_path / "segment_0.mkv",
        log_file=tmp_path / "segment_0.log"
    )


@pytest.fixture
def encoding_config(tmp_path):
    """テスト用のEncodingConfigを作成するフィクスチャ"""
    input_file = tmp_path / "input.mp4"
    input_file.touch()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    return EncodingConfig(
        input_file=input_file,
        workspace_dir=workspace_dir,
        parallel_jobs=4,
        segment_length=60,
        extra_args=['-crf', '30', '-preset', '6', '-g', '240', '-keyint_min', '240']
    )


@pytest.fixture
def mock_logger():
    """ロガーのモックを作成するフィクスチャ"""
    mock_logger = Mock()
    mock_logger.handlers = []
    return mock_logger


class TestSegmentInfo:
    """SegmentInfoデータクラスのテスト"""

    def test_segment_infoを作成(self, tmp_path):
        """SegmentInfoが正しく作成されることをテスト"""
        segment_info = SegmentInfo(
            index=1,
            start_time=60,
            duration=60,
            is_final=False,
            file=tmp_path / "segment_1.mkv",
            log_file=tmp_path / "segment_1.log"
        )

        assert segment_info.index == 1
        assert segment_info.start_time == 60
        assert segment_info.duration == 60
        assert segment_info.is_final is False
        assert segment_info.file == tmp_path / "segment_1.mkv"
        assert segment_info.log_file == tmp_path / "segment_1.log"


class TestFFmpegServiceのget_duration:
    """FFmpegServiceのget_durationメソッドのテスト"""

    def test_動画の長さを取得(self, ffmpeg_service, tmp_path):
        """ffprobeを使用して動画の長さを取得するテスト"""
        input_file = tmp_path / "input.mp4"

        # ffprobeの出力をモック
        mock_result = Mock()
        mock_result.stdout = '''{
    "format": {
        "filename": "xxx.mkv",
        "nb_streams": 2,
        "nb_programs": 0,
        "nb_stream_groups": 0,
        "format_name": "matroska,webm",
        "format_long_name": "Matroska / WebM",
        "start_time": "0.000000",
        "duration": "2112.857000",
        "size": "2601514857",
        "bit_rate": "9850225",
        "probe_score": 100,
        "tags": {
            "ENCODER": "Lavf61.7.100"
        }
    }
}'''

        with patch('av1_encoder.ffmpeg.subprocess.run', return_value=mock_result) as mock_run:
            duration = ffmpeg_service.get_duration(input_file)

            # 正しいコマンドで呼び出されたか確認
            mock_run.assert_called_once_with(
                [
                    'ffprobe', '-v', 'quiet', '-print_format', 'json',
                    '-show_format', str(input_file)
                ],
                capture_output=True,
                text=True,
                check=True
            )

            # 正しい長さが返されたか確認
            assert duration == 2112.857

    def test_動画の長さを取得_整数値(self, ffmpeg_service, tmp_path):
        """整数値の動画の長さを取得するテスト"""
        input_file = tmp_path / "input.mp4"

        mock_result = Mock()
        mock_result.stdout = '''{
    "format": {
        "filename": "xxx.mkv",
        "nb_streams": 2,
        "nb_programs": 0,
        "nb_stream_groups": 0,
        "format_name": "matroska,webm",
        "format_long_name": "Matroska / WebM",
        "start_time": "0.000000",
        "duration": "2112.000000",
        "size": "2601514857",
        "bit_rate": "9850225",
        "probe_score": 100,
        "tags": {
            "ENCODER": "Lavf61.7.100"
        }
    }
}'''

        with patch('av1_encoder.ffmpeg.subprocess.run', return_value=mock_result):
            duration = ffmpeg_service.get_duration(input_file)
            assert duration == 2112.0


class TestFFmpegServiceのencode_segment:
    """FFmpegServiceのencode_segmentメソッドのテスト"""

    def test_セグメントをエンコード_成功(self, ffmpeg_service, segment_info, encoding_config, tmp_path, mock_logger):
        """セグメントを正常にエンコードするテスト"""
        input_file = tmp_path / "input.mp4"

        # Popenのモック
        mock_process = Mock()
        mock_process.stdout = iter(["frame=100", "fps=30"])
        mock_process.wait.return_value = 0

        mock_handler = Mock()

        with patch('av1_encoder.ffmpeg.subprocess.Popen', return_value=mock_process) as mock_popen, \
             patch('av1_encoder.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # 成功を返すことを確認
            assert result is True

            # Popenが正しいコマンドで呼び出されたか確認
            called_cmd = mock_popen.call_args[0][0]
            assert called_cmd[0] == 'ffmpeg'
            assert '-ss' in called_cmd
            assert '0' in called_cmd  # start_time
            assert '-i' in called_cmd
            assert str(input_file) in called_cmd
            assert '-t' in called_cmd  # is_finalがFalseなので-tオプションがある
            assert '60' in called_cmd  # duration
            assert '-c:v' in called_cmd
            assert 'libsvtav1' in called_cmd
            assert '-crf' in called_cmd
            assert '30' in called_cmd
            assert '-preset' in called_cmd
            assert '6' in called_cmd
            assert '-g' in called_cmd
            assert '240' in called_cmd
            assert '-keyint_min' in called_cmd
            assert '-an' in called_cmd
            assert '-y' in called_cmd

    def test_セグメントをエンコード_最終セグメント(self, ffmpeg_service, tmp_path, encoding_config, mock_logger):
        """最終セグメントをエンコードするテスト（-tオプションなし）"""
        segment_info = SegmentInfo(
            index=5,
            start_time=300,
            duration=60,
            is_final=True,  # 最終セグメント
            file=tmp_path / "segment_5.mkv",
            log_file=tmp_path / "segment_5.log"
        )
        input_file = tmp_path / "input.mp4"

        mock_process = Mock()
        mock_process.stdout = iter([])
        mock_process.wait.return_value = 0

        mock_handler = Mock()

        with patch('av1_encoder.ffmpeg.subprocess.Popen', return_value=mock_process) as mock_popen, \
             patch('av1_encoder.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            assert result is True

            # -tオプションがないことを確認
            called_cmd = mock_popen.call_args[0][0]
            assert '-t' not in called_cmd

    def test_セグメントをエンコード_extra_argsなし(self, ffmpeg_service, segment_info, tmp_path, mock_logger):
        """extra_argsなしでセグメントをエンコードするテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            segment_length=60
        )

        mock_process = Mock()
        mock_process.stdout = iter([])
        mock_process.wait.return_value = 0

        mock_handler = Mock()

        with patch('av1_encoder.ffmpeg.subprocess.Popen', return_value=mock_process) as mock_popen, \
             patch('av1_encoder.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, config)

            assert result is True

            # extra_argsがないので追加オプションが含まれていないことを確認
            called_cmd = mock_popen.call_args[0][0]
            assert '-crf' not in called_cmd
            assert '-preset' not in called_cmd
            assert '-g' not in called_cmd
            assert '-keyint_min' not in called_cmd

    def test_セグメントをエンコード_カスタムextra_args(self, ffmpeg_service, segment_info, tmp_path, mock_logger):
        """カスタムextra_argsでセグメントをエンコードするテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            segment_length=60,
            extra_args=[
                '-pix_fmt', 'yuv420p10le',
                '-svtav1-params', 'tune=0:enable-qm=1:qm-min=0',
                '-crf', '25'
            ]
        )

        mock_process = Mock()
        mock_process.stdout = iter([])
        mock_process.wait.return_value = 0

        mock_handler = Mock()

        with patch('av1_encoder.ffmpeg.subprocess.Popen', return_value=mock_process) as mock_popen, \
             patch('av1_encoder.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, config)

            assert result is True

            # カスタムオプションが含まれていることを確認
            called_cmd = mock_popen.call_args[0][0]
            assert '-pix_fmt' in called_cmd
            assert 'yuv420p10le' in called_cmd
            assert '-svtav1-params' in called_cmd
            assert 'tune=0:enable-qm=1:qm-min=0' in called_cmd
            assert '-crf' in called_cmd
            assert '25' in called_cmd

    def test_セグメントをエンコード_失敗(self, ffmpeg_service, segment_info, encoding_config, tmp_path, mock_logger):
        """セグメントのエンコードが失敗するテスト"""
        input_file = tmp_path / "input.mp4"

        mock_process = Mock()
        mock_process.stdout = iter(["Error message"])
        mock_process.wait.return_value = 1  # エラーコード

        mock_handler = Mock()

        with patch('av1_encoder.ffmpeg.subprocess.Popen', return_value=mock_process), \
             patch('av1_encoder.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # 失敗を返すことを確認
            assert result is False

    def test_セグメントをエンコード_ロガーのクリーンアップ(self, ffmpeg_service, segment_info, encoding_config, tmp_path):
        """エンコード後にロガーのハンドラがクリーンアップされることをテスト"""
        input_file = tmp_path / "input.mp4"

        mock_process = Mock()
        mock_process.stdout = iter([])
        mock_process.wait.return_value = 0

        mock_handler = Mock()
        mock_logger = Mock()
        handlers_list = []
        mock_logger.handlers = handlers_list

        # addHandlerが呼ばれたときにリストに追加
        def add_handler(handler):
            handlers_list.append(handler)
        mock_logger.addHandler.side_effect = add_handler

        with patch('av1_encoder.ffmpeg.subprocess.Popen', return_value=mock_process), \
             patch('av1_encoder.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.ffmpeg.logging.getLogger', return_value=mock_logger):

            ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # ハンドラがクローズされ、削除されたことを確認
            mock_handler.close.assert_called_once()
            mock_logger.removeHandler.assert_called_once_with(mock_handler)


