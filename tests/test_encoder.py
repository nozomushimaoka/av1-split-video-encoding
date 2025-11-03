from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import logging
import pytest

from av1_encoder.encoder import EncodingOrchestrator
from av1_encoder.config import EncodingConfig
from av1_encoder.ffmpeg import SegmentInfo
from av1_encoder.workspace import Workspace


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
        parallel_jobs=2,
        segment_length=60,
        extra_args=['-crf', '30', '-preset', '6', '-g', '240', '-keyint_min', '240']
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


class TestEncodingOrchestrator初期化:
    """EncodingOrchestratorの初期化のテスト"""

    def test_初期化時に必要なコンポーネントが作成される(self, encoding_config, tmp_path):
        """EncodingOrchestratorが必要なコンポーネントで初期化されることをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path') as mock_make_workspace, \
             patch('av1_encoder.encoder.FFmpegService') as mock_ffmpeg_class, \
             patch.object(EncodingOrchestrator, '_init_logger') as mock_init_logger:

            mock_workspace = Mock()
            mock_workspace.log_file = tmp_path / "test.log"
            mock_make_workspace.return_value = mock_workspace
            mock_logger = Mock()
            mock_init_logger.return_value = mock_logger

            orchestrator = EncodingOrchestrator(encoding_config)

            # configが設定されていることを確認
            assert orchestrator.config == encoding_config

            # start_timeが設定されていることを確認
            assert isinstance(orchestrator.start_time, datetime)

            # workspaceが作成されていることを確認
            mock_make_workspace.assert_called_once_with(encoding_config.workspace_dir, encoding_config.input_file)
            assert orchestrator.workspace == mock_workspace

            # loggerが初期化されていることを確認
            mock_init_logger.assert_called_once_with(mock_workspace.log_file)
            assert orchestrator.logger == mock_logger

            # FFmpegServiceが作成されていることを確認
            mock_ffmpeg_class.assert_called_once()

    def test_start_timeが現在時刻に近い(self, encoding_config):
        """start_timeが現在時刻に設定されることをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            before = datetime.now()
            orchestrator = EncodingOrchestrator(encoding_config)
            after = datetime.now()

            # start_timeが初期化時の現在時刻であることを確認
            assert before <= orchestrator.start_time <= after


class TestEncodingOrchestratorのrun:
    """EncodingOrchestratorのrunメソッドのテスト"""

    def test_run処理が正しい順序で実行される(self, encoding_config):
        """runメソッドが正しい順序で各ステップを実行することをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'), \
             patch.object(EncodingOrchestrator, '_print_header') as mock_header, \
             patch.object(EncodingOrchestrator, '_encode_segments') as mock_encode, \
             patch.object(EncodingOrchestrator, '_concat_segments') as mock_concat, \
             patch.object(EncodingOrchestrator, '_print_completion') as mock_completion:

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.run()

            # 各メソッドが呼び出されたことを確認
            mock_header.assert_called_once()
            mock_encode.assert_called_once()
            mock_concat.assert_called_once()
            mock_completion.assert_called_once()

    def test_runでエラーが発生した場合にログとraise(self, encoding_config):
        """runメソッドでエラーが発生した場合にログに記録し例外を再raiseすることをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path'), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            error = RuntimeError("テストエラー")

            with patch.object(orchestrator, '_print_header'), \
                 patch.object(orchestrator, '_encode_segments', side_effect=error):

                with pytest.raises(RuntimeError, match="テストエラー"):
                    orchestrator.run()

                # logger.exceptionが呼び出されたことを確認
                orchestrator.logger.exception.assert_called_once_with("エラー")


class TestEncodingOrchestratorのprint_header:
    """EncodingOrchestratorの_print_headerメソッドのテスト"""

    def test_ヘッダー情報をログに出力(self, encoding_config, mock_workspace):
        """_print_headerが設定情報をログに出力することをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()

            orchestrator._print_header()

            # ログが出力されたことを確認
            assert orchestrator.logger.info.call_count >= 3
            calls = [str(call) for call in orchestrator.logger.info.call_args_list]
            assert any("作業ディレクトリ" in str(call) for call in calls)
            assert any("並列ジョブ数" in str(call) for call in calls)
            assert any("追加FFmpegオプション" in str(call) for call in calls)

    def test_ヘッダー情報_オプションなし(self, tmp_path, mock_workspace):
        """オプションがない場合はそれらをログに出力しないことをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir(exist_ok=True)
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=2,
            segment_length=60
        )

        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(config)
            orchestrator.logger = Mock()

            orchestrator._print_header()

            # オプションなしの情報が出力されないことを確認
            calls = [str(call) for call in orchestrator.logger.info.call_args_list]
            assert not any("追加FFmpegオプション" in str(call) for call in calls)


class TestEncodingOrchestratorのprint_completion:
    """EncodingOrchestratorの_print_completionメソッドのテスト"""

    def test_完了情報をログに出力(self, encoding_config, mock_workspace):
        """_print_completionが処理時間をログに出力することをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.start_time = datetime.now() - timedelta(seconds=100)

            orchestrator._print_completion()

            # ログが出力されたことを確認
            assert orchestrator.logger.info.call_count == 2
            calls = [call[0][0] for call in orchestrator.logger.info.call_args_list]
            assert "全処理完了" in calls[0]
            assert "処理時間" in calls[1]


class TestEncodingOrchestratorのlist_segments:
    """EncodingOrchestratorの_list_segmentsメソッドのテスト"""

    def test_セグメントリストを生成(self, encoding_config, mock_workspace):
        """_list_segmentsが正しいセグメントリストを生成することをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            with patch.object(orchestrator, '_calc_num_segments', return_value=3):
                segments = orchestrator._list_segments()

                assert len(segments) == 3

                # 最初のセグメント
                assert segments[0].index == 0
                assert segments[0].start_time == 0
                assert segments[0].duration == 60
                assert segments[0].is_final is False
                assert segments[0].file == mock_workspace.work_dir / "segment_0000.mp4"
                assert segments[0].log_file == mock_workspace.work_dir / "segment_0000.log"

                # 2番目のセグメント
                assert segments[1].index == 1
                assert segments[1].start_time == 60
                assert segments[1].is_final is False

                # 最終セグメント
                assert segments[2].index == 2
                assert segments[2].start_time == 120
                assert segments[2].is_final is True

    def test_単一セグメントの場合(self, encoding_config, mock_workspace):
        """動画が1セグメントしかない場合のテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)

            with patch.object(orchestrator, '_calc_num_segments', return_value=1):
                segments = orchestrator._list_segments()

                assert len(segments) == 1
                assert segments[0].index == 0
                assert segments[0].is_final is True


class TestEncodingOrchestratorのcalc_num_segments:
    """EncodingOrchestratorの_calc_num_segmentsメソッドのテスト"""

    def test_セグメント数を計算(self, encoding_config, mock_workspace):
        """_calc_num_segmentsが正しくセグメント数を計算することをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

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
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

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
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            # 3つのセグメントを返すようにモック
            segments = [
                SegmentInfo(0, 0, 60, False, Path("seg0.mp4"), Path("seg0.log")),
                SegmentInfo(1, 60, 60, False, Path("seg1.mp4"), Path("seg1.log")),
                SegmentInfo(2, 120, 60, True, Path("seg2.mp4"), Path("seg2.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoder.ProcessPoolExecutor') as mock_executor_class:

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

                with patch('av1_encoder.encoder.as_completed', side_effect=mock_as_completed):
                    orchestrator._encode_segments()

                # ProcessPoolExecutorが正しいmax_workersで作成されたことを確認
                assert mock_executor_class.call_count == 1
                call_kwargs = mock_executor_class.call_args[1]
                assert call_kwargs['max_workers'] == 2

                # 各セグメントに対してsubmitが呼び出されたことを確認
                assert mock_executor.submit.call_count == 3

    def test_エンコード失敗時にエラーを発生(self, encoding_config, mock_workspace):
        """エンコードが失敗した場合にRuntimeErrorが発生することをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            segments = [
                SegmentInfo(0, 0, 60, False, Path("seg0.mp4"), Path("seg0.log")),
                SegmentInfo(1, 60, 60, True, Path("seg1.mp4"), Path("seg1.log"))
            ]

            with patch.object(orchestrator, '_list_segments', return_value=segments), \
                 patch('av1_encoder.encoder.ProcessPoolExecutor') as mock_executor_class:

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

                with patch('av1_encoder.encoder.as_completed', side_effect=mock_as_completed):
                    with pytest.raises(RuntimeError, match="1個のセグメントでエラーが発生"):
                        orchestrator._encode_segments()


class TestEncodingOrchestratorのconcat_segments:
    """EncodingOrchestratorの_concat_segmentsメソッドのテスト"""

    def test_セグメントを結合(self, encoding_config, mock_workspace):
        """_concat_segmentsがセグメントファイルを結合することをテスト"""
        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch.object(EncodingOrchestrator, '_init_logger'):

            orchestrator = EncodingOrchestrator(encoding_config)
            orchestrator.logger = Mock()
            orchestrator.ffmpeg = Mock()

            # セグメントファイルを作成
            segment_files = [
                mock_workspace.work_dir / "segment_0000.mp4",
                mock_workspace.work_dir / "segment_0001.mp4",
                mock_workspace.work_dir / "segment_0002.mp4"
            ]

            for seg_file in segment_files:
                seg_file.touch()

            # セグメントファイルが存在することを確認
            for seg_file in segment_files:
                assert seg_file.exists()

            orchestrator._concat_segments()

            # ffmpeg.concat_segmentsが正しいパラメータで呼び出されたことを確認
            orchestrator.ffmpeg.concat_segments.assert_called_once()
            call_args = orchestrator.ffmpeg.concat_segments.call_args[0]
            assert len(call_args[0]) == 3  # segment_files
            assert call_args[1] == encoding_config.input_file  # audio_file
            assert call_args[2] == mock_workspace.concat_file  # concat_file
            assert call_args[3] == mock_workspace.output_file  # output_file

            # 結合後にセグメントファイルが削除されることを確認
            for seg_file in segment_files:
                assert not seg_file.exists()


class TestEncodingOrchestratorのinit_logger:
    """EncodingOrchestratorの_init_loggerメソッドのテスト"""

    def test_ロガーを初期化(self, encoding_config, tmp_path, mock_workspace):
        """_init_loggerがロガーを正しく初期化することをテスト"""
        log_file = tmp_path / "test.log"

        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch('av1_encoder.encoder.FFmpegService'):

            orchestrator = EncodingOrchestrator(encoding_config)
            logger = orchestrator._init_logger(log_file)

            # ロガーが返されることを確認
            assert isinstance(logger, logging.Logger)
            assert logger.name == "av1_encoder"
            assert logger.level == logging.INFO

            # ハンドラが2つあることを確認（ファイルとコンソール）
            assert len(logger.handlers) == 2

            # フォーマッターが設定されていることを確認
            for handler in logger.handlers:
                assert handler.formatter is not None

    def test_既存のハンドラがクリアされる(self, encoding_config, tmp_path, mock_workspace):
        """_init_loggerが既存のハンドラをクリアすることをテスト"""
        log_file = tmp_path / "test.log"

        with patch('av1_encoder.encoder.make_workspace_from_path', return_value=mock_workspace), \
             patch('av1_encoder.encoder.FFmpegService'), \
             patch('av1_encoder.encoder.FFmpegService'):

            orchestrator = EncodingOrchestrator(encoding_config)

            # 最初の呼び出し
            logger1 = orchestrator._init_logger(log_file)
            handler_count_1 = len(logger1.handlers)

            # 2回目の呼び出し
            logger2 = orchestrator._init_logger(log_file)
            handler_count_2 = len(logger2.handlers)

            # ハンドラ数が変わらないことを確認（クリアされて再追加される）
            assert handler_count_1 == handler_count_2
            assert logger1 is logger2  # 同じロガーインスタンス


class TestEncodingConfig:
    """EncodingConfigデータクラスのテスト"""

    def test_configを作成(self, tmp_path):
        """EncodingConfigが正しく作成されることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            segment_length=120,
            extra_args=['-crf', '30', '-preset', '6', '-g', '240']
        )

        assert config.input_file == input_file
        assert config.workspace_dir == workspace_dir
        assert config.parallel_jobs == 4
        assert config.extra_args == ['-crf', '30', '-preset', '6', '-g', '240']
        assert config.segment_length == 120

    def test_configのデフォルト値(self, tmp_path):
        """EncodingConfigのデフォルト値が正しいことをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4
        )

        assert config.extra_args == []
        assert config.segment_length == 60  # デフォルト値

    def test_configのextra_argsを空リストに設定(self, tmp_path):
        """extra_argsを明示的に空リストに設定できることをテスト"""
        input_file = tmp_path / "input.mp4"
        input_file.touch()
        workspace_dir = tmp_path / "workspace"
        workspace_dir.mkdir()
        config = EncodingConfig(
            input_file=input_file,
            workspace_dir=workspace_dir,
            parallel_jobs=4,
            extra_args=[]
        )

        assert config.extra_args == []
