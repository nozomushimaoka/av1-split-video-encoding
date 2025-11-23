from unittest.mock import Mock, patch
import pytest

from av1_encoder.core.ffmpeg import FFmpegService, SegmentInfo
from av1_encoder.core.config import EncodingConfig


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
        file=tmp_path / "segment_0.ivf",
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
        gop_size=240,
        segment_length=60,
        svtav1_args=['--crf', '30', '--preset', '6']
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
            file=tmp_path / "segment_1.ivf",
            log_file=tmp_path / "segment_1.log"
        )

        assert segment_info.index == 1
        assert segment_info.start_time == 60
        assert segment_info.duration == 60
        assert segment_info.is_final is False
        assert segment_info.file == tmp_path / "segment_1.ivf"
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

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result) as mock_run:
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

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result):
            duration = ffmpeg_service.get_duration(input_file)
            assert duration == 2112.0


class TestFFmpegServiceのget_fps:
    """FFmpegServiceのget_fpsメソッドのテスト"""

    def test_フレームレートを取得_分数形式(self, ffmpeg_service, tmp_path):
        """分数形式のフレームレート(24000/1001)を取得するテスト"""
        input_file = tmp_path / "input.mp4"

        mock_result = Mock()
        mock_result.stdout = '''{
    "streams": [
        {
            "index": 0,
            "codec_name": "h264",
            "codec_type": "video",
            "r_frame_rate": "24000/1001",
            "avg_frame_rate": "24000/1001"
        }
    ]
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result) as mock_run:
            fps = ffmpeg_service.get_fps(input_file)

            # 正しいコマンドで呼び出されたか確認
            mock_run.assert_called_once_with(
                [
                    'ffprobe', '-v', 'quiet', '-print_format', 'json',
                    '-show_streams', '-select_streams', 'v:0', str(input_file)
                ],
                capture_output=True,
                text=True,
                check=True
            )

            # 正しいフレームレートが返されたか確認 (23.976...)
            assert abs(fps - 23.976023976023978) < 0.0001

    def test_フレームレートを取得_整数形式(self, ffmpeg_service, tmp_path):
        """整数形式のフレームレート(30)を取得するテスト"""
        input_file = tmp_path / "input.mp4"

        mock_result = Mock()
        mock_result.stdout = '''{
    "streams": [
        {
            "index": 0,
            "codec_name": "h264",
            "codec_type": "video",
            "r_frame_rate": "30/1",
            "avg_frame_rate": "30/1"
        }
    ]
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result):
            fps = ffmpeg_service.get_fps(input_file)
            assert fps == 30.0

    def test_フレームレートを取得_60fps(self, ffmpeg_service, tmp_path):
        """60fpsのフレームレートを取得するテスト"""
        input_file = tmp_path / "input.mp4"

        mock_result = Mock()
        mock_result.stdout = '''{
    "streams": [
        {
            "index": 0,
            "codec_name": "h264",
            "codec_type": "video",
            "r_frame_rate": "60/1",
            "avg_frame_rate": "60/1"
        }
    ]
}'''

        with patch('av1_encoder.core.ffmpeg.subprocess.run', return_value=mock_result):
            fps = ffmpeg_service.get_fps(input_file)
            assert fps == 60.0


class TestFFmpegServiceのencode_segment:
    """FFmpegServiceのencode_segmentメソッドのテスト"""

    def test_セグメントをエンコード_成功(self, ffmpeg_service, segment_info, encoding_config, tmp_path, mock_logger):
        """セグメントを正常にエンコードするテスト"""
        input_file = tmp_path / "input.mp4"

        # FFmpegプロセスのモック
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = Mock()
        mock_ffmpeg_process.stderr.read.return_value = b""
        mock_ffmpeg_process.wait.return_value = 0

        # SvtAv1EncAppプロセスのモック
        mock_svtav1_process = Mock()
        mock_svtav1_process.stderr = iter(["Encoding frame 100", "Encoding complete"])
        mock_svtav1_process.wait.return_value = 0

        mock_handler = Mock()

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.core.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # 成功を返すことを確認
            assert result is True

            # Popenが2回呼び出されたことを確認（FFmpeg + SvtAv1EncApp）
            assert mock_popen.call_count == 2

            # FFmpegコマンドの確認
            ffmpeg_call = mock_popen.call_args_list[0]
            ffmpeg_cmd = ffmpeg_call[0][0]
            assert ffmpeg_cmd[0] == 'ffmpeg'
            assert '-ss' in ffmpeg_cmd
            assert '0' in ffmpeg_cmd  # start_time
            assert '-i' in ffmpeg_cmd
            assert str(input_file) in ffmpeg_cmd
            assert '-t' in ffmpeg_cmd  # is_finalがFalseなので-tオプションがある
            assert '60' in ffmpeg_cmd  # duration
            assert '-f' in ffmpeg_cmd
            assert 'yuv4mpegpipe' in ffmpeg_cmd
            assert '-pix_fmt' in ffmpeg_cmd
            assert 'yuv420p10le' in ffmpeg_cmd

            # SvtAv1EncAppコマンドの確認
            svtav1_call = mock_popen.call_args_list[1]
            svtav1_cmd = svtav1_call[0][0]
            assert svtav1_cmd[0] == 'SvtAv1EncApp'
            assert '-i' in svtav1_cmd
            assert 'stdin' in svtav1_cmd
            assert '--keyint' in svtav1_cmd
            assert '240' in svtav1_cmd
            assert '--crf' in svtav1_cmd
            assert '30' in svtav1_cmd
            assert '--preset' in svtav1_cmd
            assert '6' in svtav1_cmd
            assert '-b' in svtav1_cmd

    def test_セグメントをエンコード_最終セグメント(self, ffmpeg_service, tmp_path, encoding_config, mock_logger):
        """最終セグメントをエンコードするテスト（-tオプションなし）"""
        segment_info = SegmentInfo(
            index=5,
            start_time=300,
            duration=60,
            is_final=True,  # 最終セグメント
            file=tmp_path / "segment_5.ivf",
            log_file=tmp_path / "segment_5.log"
        )
        input_file = tmp_path / "input.mp4"

        # FFmpegプロセスのモック
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = Mock()
        mock_ffmpeg_process.stderr.read.return_value = b""
        mock_ffmpeg_process.wait.return_value = 0

        # SvtAv1EncAppプロセスのモック
        mock_svtav1_process = Mock()
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        mock_handler = Mock()

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.core.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            assert result is True

            # -tオプションがないことを確認（FFmpegコマンド）
            ffmpeg_call = mock_popen.call_args_list[0]
            ffmpeg_cmd = ffmpeg_call[0][0]
            assert '-t' not in ffmpeg_cmd

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
            gop_size=240,
            segment_length=60
        )

        # FFmpegプロセスのモック
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = Mock()
        mock_ffmpeg_process.stderr.read.return_value = b""
        mock_ffmpeg_process.wait.return_value = 0

        # SvtAv1EncAppプロセスのモック
        mock_svtav1_process = Mock()
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        mock_handler = Mock()

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.core.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, config)

            assert result is True

            # extra_argsがないので追加オプションが含まれていないことを確認（SvtAv1EncAppコマンド）
            svtav1_call = mock_popen.call_args_list[1]
            svtav1_cmd = svtav1_call[0][0]
            assert '--crf' not in svtav1_cmd
            assert '--preset' not in svtav1_cmd
            # --keyint は自動的に追加されるため含まれる
            assert '--keyint' in svtav1_cmd
            assert '240' in svtav1_cmd

    def test_セグメントをエンコード_カスタムsvtav1_args(self, ffmpeg_service, segment_info, tmp_path, mock_logger):
        """カスタムsvtav1_argsでセグメントをエンコードするテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=60,
            svtav1_args=[
                '--pix_fmt', 'yuv420p10le',
                '--svtav1-params', 'tune=0:enable-qm=1:qm-min=0',
                '--crf', '25'
            ]
        )

        # FFmpegプロセスのモック
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = Mock()
        mock_ffmpeg_process.stderr.read.return_value = b""
        mock_ffmpeg_process.wait.return_value = 0

        # SvtAv1EncAppプロセスのモック
        mock_svtav1_process = Mock()
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        mock_handler = Mock()

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect) as mock_popen, \
             patch('av1_encoder.core.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.core.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, config)

            assert result is True

            # カスタムオプションが含まれていることを確認（SvtAv1EncAppコマンド）
            svtav1_call = mock_popen.call_args_list[1]
            svtav1_cmd = svtav1_call[0][0]
            assert '--pix_fmt' in svtav1_cmd
            assert 'yuv420p10le' in svtav1_cmd
            assert '--svtav1-params' in svtav1_cmd
            assert 'tune=0:enable-qm=1:qm-min=0' in svtav1_cmd
            assert '--crf' in svtav1_cmd
            assert '25' in svtav1_cmd

    def test_セグメントをエンコード_失敗(self, ffmpeg_service, segment_info, encoding_config, tmp_path, mock_logger):
        """セグメントのエンコードが失敗するテスト"""
        input_file = tmp_path / "input.mp4"

        # FFmpegプロセスのモック（成功）
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = Mock()
        mock_ffmpeg_process.stderr.read.return_value = b""
        mock_ffmpeg_process.wait.return_value = 0

        # SvtAv1EncAppプロセスのモック（失敗）
        mock_svtav1_process = Mock()
        mock_svtav1_process.stderr = iter(["Error message"])
        mock_svtav1_process.wait.return_value = 1  # エラーコード

        mock_handler = Mock()

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect), \
             patch('av1_encoder.core.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.core.ffmpeg.logging.getLogger', return_value=mock_logger):

            result = ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # 失敗を返すことを確認
            assert result is False

    def test_セグメントをエンコード_ロガーのクリーンアップ(self, ffmpeg_service, segment_info, encoding_config, tmp_path):
        """エンコード後にロガーのハンドラがクリーンアップされることをテスト"""
        input_file = tmp_path / "input.mp4"

        # FFmpegプロセスのモック
        mock_ffmpeg_process = Mock()
        mock_ffmpeg_process.stdout = Mock()
        mock_ffmpeg_process.stderr = Mock()
        mock_ffmpeg_process.stderr.read.return_value = b""
        mock_ffmpeg_process.wait.return_value = 0

        # SvtAv1EncAppプロセスのモック
        mock_svtav1_process = Mock()
        mock_svtav1_process.stderr = iter([])
        mock_svtav1_process.wait.return_value = 0

        mock_handler = Mock()
        mock_logger = Mock()
        handlers_list = []
        mock_logger.handlers = handlers_list

        # addHandlerが呼ばれたときにリストに追加
        def add_handler(handler):
            handlers_list.append(handler)
        mock_logger.addHandler.side_effect = add_handler

        def popen_side_effect(cmd, **kwargs):
            if cmd[0] == 'ffmpeg':
                return mock_ffmpeg_process
            elif cmd[0] == 'SvtAv1EncApp':
                return mock_svtav1_process
            return Mock()

        with patch('av1_encoder.core.ffmpeg.subprocess.Popen', side_effect=popen_side_effect), \
             patch('av1_encoder.core.ffmpeg.logging.FileHandler', return_value=mock_handler), \
             patch('av1_encoder.core.ffmpeg.logging.getLogger', return_value=mock_logger):

            ffmpeg_service.encode_segment(segment_info, input_file, encoding_config)

            # ハンドラがクローズされ、削除されたことを確認
            mock_handler.close.assert_called_once()
            mock_logger.removeHandler.assert_called_once_with(mock_handler)


