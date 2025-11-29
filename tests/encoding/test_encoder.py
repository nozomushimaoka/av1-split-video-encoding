import logging
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from av1_encoder.core.config import EncodingConfig
from av1_encoder.core.ffmpeg import SegmentInfo
from av1_encoder.core.workspace import Workspace
from av1_encoder.encoding.encoder import EncodingOrchestrator, _worker_init


@pytest.fixture
def encoding_config(tmp_path):
    """テスト用のEncodingConfigを作成するフィクスチャ"""
    input_file = tmp_path / "input.mkv"
    input_file.touch()
    workspace_dir = tmp_path / "workspace"
    workspace_dir.mkdir()
    return EncodingConfig(
        input_file=input_file,
        workspace_dir=workspace_dir,
        parallel_jobs=2,
        gop_size=240,
        segment_length=60,
        svtav1_args=['--crf', '30', '--preset', '6']
    )


@pytest.fixture
def mock_workspace(tmp_path):
    """テスト用のWorkspaceモックを作成するフィクスチャ"""
    work_dir = tmp_path / "workspace"
    work_dir.mkdir(exist_ok=True)

    return Workspace(
        work_dir=work_dir,
        log_file=work_dir / "main.log",
        concat_file=work_dir / "concat.txt",
        output_file=work_dir / "output.mkv"
    )


@pytest.fixture
def mock_logger():
    """ロガーのモックを作成するフィクスチャ"""
    logger = Mock(spec=logging.Logger)
    return logger


class TestWorkerInit:
    """_worker_init関数のテスト"""

    def test_シグナルハンドラをデフォルトに戻す(self):
        """_worker_initがシグナルハンドラをデフォルトに戻すことをテスト"""
        import signal

        with patch('signal.signal') as mock_signal:
            _worker_init()

            # signal.signalが2回呼ばれたことを確認（SIGINT, SIGTERM）
            assert mock_signal.call_count == 2

            # SIGINT, SIGTERMに対してSIG_DFLが設定されたことを確認
            calls = mock_signal.call_args_list
            assert calls[0][0] == (signal.SIGINT, signal.SIG_DFL)
            assert calls[1][0] == (signal.SIGTERM, signal.SIG_DFL)


class TestEncodingOrchestrator初期化:
    """EncodingOrchestratorの初期化のテスト"""

    def test_初期化時に必要なコンポーネントが作成される(self, encoding_config, tmp_path):
        """EncodingOrchestratorが必要なコンポーネントで初期化されることをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path') as mock_make_workspace, \
             patch('av1_encoder.encoding.encoder.FFmpegService') as mock_ffmpeg_class, \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger') as mock_setup_logger:

            mock_workspace = Mock()
            mock_workspace.log_file = tmp_path / "test.log"
            mock_make_workspace.return_value = mock_workspace
            mock_logger = Mock()
            mock_setup_logger.return_value = mock_logger

            orchestrator = EncodingOrchestrator(encoding_config)

            # configが設定されていることを確認
            assert orchestrator.config == encoding_config

            # start_timeが設定されていることを確認
            assert isinstance(orchestrator.start_time, datetime)

            # workspaceが作成されていることを確認
            mock_make_workspace.assert_called_once_with(encoding_config.workspace_dir)
            assert orchestrator.workspace == mock_workspace

            # loggerが初期化されていることを確認
            mock_setup_logger.assert_called_once_with("av1_encoder", mock_workspace.log_file)
            assert orchestrator.logger == mock_logger

            # FFmpegServiceが作成されていることを確認
            mock_ffmpeg_class.assert_called_once()

    def test_start_timeが現在時刻に近い(self, encoding_config):
        """start_timeが現在時刻に設定されることをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            before = datetime.now()
            orchestrator = EncodingOrchestrator(encoding_config)
            after = datetime.now()

            # start_timeが初期化時の現在時刻であることを確認
            assert before <= orchestrator.start_time <= after


class TestEncodingOrchestratorのrun:
    """EncodingOrchestratorのrunメソッドのテスト"""

    def test_run処理が正しい順序で実行される(self, encoding_config):
        """runメソッドが正しい順序で各ステップを実行することをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'), \
             patch.object(EncodingOrchestrator, '_encode_segments') as mock_encode, \
             patch.object(EncodingOrchestrator, '_generate_concat_file') as mock_generate_concat, \
             patch.object(EncodingOrchestrator, '_print_completion') as mock_completion:

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.run()

            # 各メソッドが呼び出されたことを確認
            mock_encode.assert_called_once()
            mock_generate_concat.assert_called_once()
            mock_completion.assert_called_once()

    def test_runでエラーが発生した場合にログとraise(self, encoding_config):
        """runメソッドでエラーが発生した場合にログに記録し例外を再raiseすることをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            error = RuntimeError("テストエラー")

            with patch.object(orchestrator, '_encode_segments', side_effect=error):

                with pytest.raises(RuntimeError, match="テストエラー"):
                    orchestrator.run()

                # logger.exceptionが呼び出されたことを確認
                orchestrator.logger.exception.assert_called_once_with("エラー")

    def test_KeyboardInterrupt時の処理(self, encoding_config):
        """KeyboardInterrupt時に適切に処理されることをテスト"""
        import sys
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # エンコード中にKeyboardInterrupt
            with patch.object(orchestrator, '_encode_segments', side_effect=KeyboardInterrupt):
                with pytest.raises(SystemExit) as exc_info:
                    orchestrator.run()

                # 終了コード130で終了することを確認
                assert exc_info.value.code == 130

                # エラーログが出力されたことを確認
                orchestrator.logger.error.assert_called_once_with("処理が中断されました")


class TestEncodingOrchestratorのシグナルハンドラ:
    """EncodingOrchestratorのシグナルハンドラのテスト"""

    def test_メインプロセスでシグナル受信時にKeyboardInterruptを発生(self, encoding_config):
        """メインプロセスでシグナル受信時にKeyboardInterruptが発生することをテスト"""
        import os
        import signal

        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # メインプロセスのPIDと一致することを確認
            assert orchestrator._main_pid == os.getpid()

            # シグナルハンドラを直接呼び出し
            with pytest.raises(KeyboardInterrupt):
                orchestrator._signal_handler(signal.SIGINT, None)

            # ログが出力されたことを確認
            orchestrator.logger.warning.assert_called_once()
            assert "中断シグナル" in orchestrator.logger.warning.call_args[0][0]

    def test_ワーカープロセスではシグナルを無視(self, encoding_config):
        """ワーカープロセスではシグナルを無視することをテスト"""
        import signal

        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # 異なるPIDに変更（ワーカープロセスをシミュレート）
            original_pid = orchestrator._main_pid
            orchestrator._main_pid = original_pid + 1000

            # シグナルハンドラを呼び出しても例外は発生しない
            orchestrator._signal_handler(signal.SIGINT, None)

            # ログが出力されていないことを確認
            orchestrator.logger.warning.assert_not_called()

    def test_シグナルハンドラが設定される(self, encoding_config):
        """run実行時にシグナルハンドラが設定されることをテスト"""
        import signal

        with patch('av1_encoder.encoding.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'), \
             patch.object(EncodingOrchestrator, '_encode_segments'), \
             patch.object(EncodingOrchestrator, '_generate_concat_file'), \
             patch.object(EncodingOrchestrator, '_print_completion'), \
             patch('signal.signal') as mock_signal:

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.run()

            # signal.signalが2回呼ばれたことを確認（SIGINT, SIGTERM）
            assert mock_signal.call_count >= 2

            # SIGINT, SIGTERMに対してハンドラが設定されたことを確認
            signal_calls = [call[0] for call in mock_signal.call_args_list]
            assert any(signal.SIGINT in call for call in signal_calls)
            assert any(signal.SIGTERM in call for call in signal_calls)


class TestEncodingOrchestratorのprint_completion:
    """EncodingOrchestratorの_print_completionメソッドのテスト"""

    def test_完了情報をログに出力(self, encoding_config, mock_workspace):
        """_print_completionが処理時間をログに出力することをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.start_time = datetime.now() - timedelta(seconds=100)

            orchestrator._print_completion()

            # ログが出力されたことを確認
            assert orchestrator.logger.info.call_count == 1
            calls = [call[0][0] for call in orchestrator.logger.info.call_args_list]
            assert "終了" in calls[0]
            assert "処理時間" in calls[0]


class TestEncodingOrchestratorのlist_segments:
    """EncodingOrchestratorの_list_segmentsメソッドのテスト"""

    def test_セグメントリストを生成(self, encoding_config, mock_workspace):
        """_list_segmentsが正しいセグメントリストを生成することをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            # get_fps() と get_gop_size() のモックを設定
            orchestrator.ffmpeg.get_fps.return_value = 24.0
            # encoding_config に get_gop_size() メソッドがあるので、そのまま使用される（デフォルト240）

            with patch.object(orchestrator, '_calc_num_segments', return_value=3):
                segments = orchestrator._list_segments()

                assert len(segments) == 3

                # 最初のセグメント
                assert segments[0].index == 0
                assert segments[0].start_time == 0
                assert segments[0].duration == 60.0
                assert segments[0].is_final is False
                assert segments[0].file == mock_workspace.work_dir / "segment_0000.ivf"
                assert segments[0].log_file == mock_workspace.work_dir / "segment_0000.log"

                # 2番目のセグメント
                assert segments[1].index == 1
                assert segments[1].start_time == 60.0
                assert segments[1].is_final is False

                # 最終セグメント
                assert segments[2].index == 2
                assert segments[2].start_time == 120.0
                assert segments[2].is_final is True

    def test_単一セグメントの場合(self, encoding_config, mock_workspace):
        """動画が1セグメントしかない場合のテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            # get_fps() のモックを設定
            orchestrator.ffmpeg.get_fps.return_value = 24.0

            with patch.object(orchestrator, '_calc_num_segments', return_value=1):
                segments = orchestrator._list_segments()

                assert len(segments) == 1
                assert segments[0].index == 0
                assert segments[0].is_final is True


class TestEncodingOrchestratorのcalc_num_segments:
    """EncodingOrchestratorの_calc_num_segmentsメソッドのテスト"""

    def test_セグメント数を計算(self, encoding_config, mock_workspace):
        """_calc_num_segmentsが正しくセグメント数を計算することをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.ffmpeg = Mock()

            # 180秒の動画、セグメント長60秒の場合
            orchestrator.ffmpeg.get_duration.return_value = 180.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 3

            # 181秒の動画、セグメント長60秒の場合（切り上げで4セグメント）
            orchestrator.ffmpeg.get_duration.return_value = 181.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 4

            # 60秒の動画、セグメント長60秒の場合
            orchestrator.ffmpeg.get_duration.return_value = 60.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 1

    def test_端数があるセグメント数の計算(self, encoding_config, mock_workspace):
        """端数がある場合に切り上げられることをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.ffmpeg = Mock()

            # 100秒の動画、セグメント長60秒の場合（2セグメント）
            orchestrator.ffmpeg.get_duration.return_value = 100.0
            num_segments = orchestrator._calc_num_segments()
            assert num_segments == 2


class TestEncodingOrchestratorのencode_segments:
    """EncodingOrchestratorの_encode_segmentsメソッドのテスト"""

    def test_セグメントを並列エンコード(self, encoding_config, mock_workspace):
        """_encode_segmentsがセグメントを並列にエンコードすることをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            # 3つのセグメントを返すようにモック
            segments = [
                SegmentInfo(0, 0, 60, False, Path("seg0.ivf"), Path("seg0.log")),
                SegmentInfo(1, 60, 60, False, Path("seg1.ivf"), Path("seg1.log")),
                SegmentInfo(2, 120, 60, True, Path("seg2.ivf"), Path("seg2.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoding.encoder.ProcessPoolExecutor') as mock_executor_class:

                # 成功を返すモックfuture
                mock_futures = []
                for i in range(3):
                    mock_future = Mock()
                    mock_future.result.return_value = True
                    mock_futures.append(mock_future)

                # モックexecutorの設定
                mock_executor = MagicMock()
                mock_executor.submit.side_effect = mock_futures
                mock_executor.__enter__.return_value = mock_executor
                mock_executor.__exit__.return_value = False
                mock_executor_class.return_value = mock_executor

                # as_completedは引数として渡されたdictのキーを返す関数をモック
                def mock_as_completed(future_dict):
                    return list(future_dict.keys())

                with patch('av1_encoder.encoding.encoder.as_completed', side_effect=mock_as_completed):
                    orchestrator._encode_segments()

                # ProcessPoolExecutorが正しいmax_workersで作成されたことを確認
                assert mock_executor_class.call_count == 1
                call_kwargs = mock_executor_class.call_args[1]
                assert call_kwargs['max_workers'] == 2

                # 各セグメントに対してsubmitが呼び出されたことを確認
                assert mock_executor.submit.call_count == 3

    def test_エンコード失敗時にエラーを発生(self, encoding_config, mock_workspace):
        """エンコードが失敗した場合にRuntimeErrorが発生することをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            segments = [
                SegmentInfo(0, 0, 60, False, Path("seg0.ivf"), Path("seg0.log")),
                SegmentInfo(1, 60, 60, True, Path("seg1.ivf"), Path("seg1.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoding.encoder.ProcessPoolExecutor') as mock_executor_class:

                # 1つは成功、1つは失敗
                mock_future1 = Mock()
                mock_future1.result.return_value = True
                mock_future2 = Mock()
                mock_future2.result.return_value = False

                mock_futures = [mock_future1, mock_future2]

                # モックexecutorの設定
                mock_executor = MagicMock()
                mock_executor.submit.side_effect = mock_futures
                mock_executor.__enter__.return_value = mock_executor
                mock_executor.__exit__.return_value = False
                mock_executor_class.return_value = mock_executor

                # as_completedは引数として渡されたdictのキーを返す関数をモック
                def mock_as_completed(future_dict):
                    return list(future_dict.keys())

                with patch('av1_encoder.encoding.encoder.as_completed', side_effect=mock_as_completed):
                    with pytest.raises(RuntimeError, match="セグメント.*のエンコードに失敗"):
                        orchestrator._encode_segments()


class TestEncodingOrchestratorのgenerate_concat_file:
    """EncodingOrchestratorの_generate_concat_fileメソッドのテスト"""

    def test_concat_txtを生成(self, encoding_config, mock_workspace):
        """_generate_concat_fileがconcat.txtを生成することをテスト"""
        with patch('av1_encoder.encoding.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoding.encoder.FFmpegService'), \
             patch('av1_encoder.encoding.encoder.setup_file_and_console_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            # セグメントファイルを作成
            segment_files = [
                mock_workspace.work_dir / "segment_0000.ivf",
                mock_workspace.work_dir / "segment_0001.ivf",
                mock_workspace.work_dir / "segment_0002.ivf"
            ]

            for seg_file in segment_files:
                seg_file.touch()

            orchestrator._generate_concat_file()

            # concat.txtが生成されたことを確認
            assert mock_workspace.concat_file.exists()

            # concat.txtの内容を確認
            with open(mock_workspace.concat_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            assert len(lines) == 3
            for i, line in enumerate(lines):
                assert line == f"file '{segment_files[i].resolve()}'\n"


class TestEncodingConfig:
    """EncodingConfigデータクラスのテスト"""

    def test_configを作成(self, tmp_path):
        """EncodingConfigが正しく作成されることをテスト"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            segment_length=120,
            svtav1_args=['--crf', '30', '--preset', '6']
        )

        assert config.input_file == input_file
        assert config.workspace_dir == workspace_dir
        assert config.parallel_jobs == 4
        assert config.svtav1_args == ['--crf', '30', '--preset', '6']
        assert config.segment_length == 120

    def test_configのデフォルト値(self, tmp_path):
        """EncodingConfigのデフォルト値が正しいことをテスト"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240        )

        assert config.svtav1_args == []
        assert config.segment_length == 60  # デフォルト値

    def test_configのsvtav1_argsを空リストに設定(self, tmp_path):
        """svtav1_argsを明示的に空リストに設定できることをテスト"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            gop_size=240,
            svtav1_args=[]
        )

        assert config.svtav1_args == []
