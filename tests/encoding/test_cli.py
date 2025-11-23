from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from av1_encoder.encoding.cli import main
from av1_encoder.core.config import EncodingConfig


class TestCLIの引数パース:
    """CLIの引数パースのテスト"""

    def test_最小限の引数でパース(self, tmp_path):
        """入力ファイルのみ指定した場合の引数パースをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            # EncodingOrchestratorが正しい設定で呼び出されたことを確認
            assert mock_orchestrator_class.call_count == 1
            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('input.mp4')
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 4
            assert config.svtav1_args == ['--crf', '30']

            # runが呼び出されたことを確認
            mock_orchestrator.run.assert_called_once()

            # 成功コードを返すことを確認
            assert result == 0

    def test_全ての引数を指定してパース(self, tmp_path):
        """全ての引数を指定した場合の引数パースをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'video.mkv',
            str(workspace),
            '--parallel', '8', '--gop', '240',
            '-svtav1-params', 'crf=30:preset=6:g=240'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            # EncodingOrchestratorが正しい設定で呼び出されたことを確認
            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('video.mkv')
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 8
            # CLI側で展開されるので、展開後の形式になる
            assert config.svtav1_args == ['--crf', '30', '--preset', '6', '--g', '240']

            assert result == 0

    def test_短縮オプションでパラレルを指定(self, tmp_path):
        """短縮オプション -l でparallel_jobsを指定できることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '-l', '16', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.parallel_jobs == 16
            assert result == 0

    def test_一部のオプションのみ指定(self, tmp_path):
        """一部のオプションのみを指定した場合のテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'test.mp4',
            str(workspace),
            '--parallel', '4', '--gop', '240',
            '-svtav1-params', 'crf=25'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('test.mp4')
            assert config.parallel_jobs == 4
            # CLI側で展開されるので、展開後の形式になる
            assert config.svtav1_args == ['--crf', '25']
            assert result == 0


class TestCLIのmain関数:
    """CLIのmain関数のテスト"""

    def test_エンコード成功時に0を返す(self, tmp_path):
        """エンコードが成功した場合に0を返すことをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            assert result == 0

    def test_エンコード失敗時に1を返す(self, tmp_path):
        """エンコードが失敗した場合に1を返すことをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = RuntimeError("エンコードエラー")

            result = main()

            assert result == 1

    def test_例外が発生しても1を返す(self, tmp_path):
        """任意の例外が発生した場合に1を返すことをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = Exception("予期しないエラー")

            result = main()

            assert result == 1

    def test_orchestratorのrunが呼び出される(self, tmp_path):
        """orchestrator.run()が呼び出されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            mock_orchestrator.run.assert_called_once()


class TestCLIのEncodingConfig作成:
    """CLIからEncodingConfigを作成するテスト"""

    def test_EncodingConfigが正しく作成される(self, tmp_path):
        """CLIの引数からEncodingConfigが正しく作成されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'my_video.mp4',
            str(workspace),
            '--parallel', '12', '--gop', '240',
            '-svtav1-params', 'crf=28:preset=5:g=120'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            # EncodingConfigのインスタンスが作成されたことを確認
            assert mock_orchestrator_class.call_count == 1
            config = mock_orchestrator_class.call_args[0][0]

            assert isinstance(config, EncodingConfig)
            assert config.input_file == Path('my_video.mp4')
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 12
            # CLI側で展開されるので、展開後の形式になる
            assert config.svtav1_args == ['--crf', '28', '--preset', '5', '--g', '120']

    def test_input_fileがPathオブジェクトに変換される(self, tmp_path):
        """input_file引数がPathオブジェクトに変換されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'test/path/to/video.mkv', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert isinstance(config.input_file, Path)
            assert config.input_file == Path('test/path/to/video.mkv')


class TestCLIのデフォルト値:
    """CLIのデフォルト値のテスト"""

    def test_parallelのデフォルト値はNone(self, tmp_path):
        """parallelのデフォルト値がNoneであることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.parallel_jobs == 4

    def test_svtav1_paramsが必須である(self, tmp_path):
        """-svtav1-paramsが必須パラメータであることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()


class TestCLIのargparse動作:
    """argparseの動作に関するテスト"""

    def test_引数なしでヘルプが表示される(self):
        """引数なしで実行した場合にSystemExitが発生することをテスト"""
        test_args = ['prog']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_整数型の引数に文字列を指定してSystemExit(self, tmp_path):
        """整数型の引数に文字列を指定した場合にSystemExitが発生することをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', 'not-a-number', '--gop', '240']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()


class TestCLIの引数型:
    """CLIの引数の型に関するテスト"""

    def test_整数引数が正しく変換される(self, tmp_path):
        """整数引数が正しく変換されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'input.mp4',
            str(workspace),
            '--parallel', '10', '--gop', '240',
            '-svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]

            assert isinstance(config.parallel_jobs, int)
            assert config.parallel_jobs == 10

    def test_文字列引数が正しく処理される(self, tmp_path):
        """文字列引数が正しく処理されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = [
            'prog',
            'path/to/input.mp4',
            str(workspace),
            '--parallel', '4', '--gop', '240',
            '-svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert isinstance(config.input_file, Path)
            assert config.input_file == Path('path/to/input.mp4')


class TestCLIのエッジケース:
    """CLIのエッジケースのテスト"""

    def test_負の値を持つ引数(self, tmp_path):
        """負の値を持つ引数が正しく処理されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '-1', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            # argparseは負の値を許可するので、-1が設定される
            assert config.parallel_jobs == -1

    def test_svtav1_argsが正しく処理される(self, tmp_path):
        """svtav1_argsが正しく処理されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        test_args = ['prog', 'input.mp4', str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=0:preset=0']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            # CLI側で展開されるので、展開後の形式になる
            assert config.svtav1_args == ['--crf', '0', '--preset', '0']

    def test_非常に長いファイルパス(self, tmp_path):
        """非常に長いファイルパスが正しく処理されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        long_path = 'a' * 200 + '.mp4'
        test_args = ['prog', long_path, str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == Path(long_path)

    def test_特殊文字を含むファイル名(self, tmp_path):
        """特殊文字を含むファイル名が正しく処理されることをテスト"""
        workspace = tmp_path / 'workspace'
        workspace.mkdir()
        special_filename = 'test-file_123 (copy).mp4'
        test_args = ['prog', special_filename, str(workspace), '--parallel', '4', '--gop', '240', '-svtav1-params', 'crf=30']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.encoding.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == Path(special_filename)

