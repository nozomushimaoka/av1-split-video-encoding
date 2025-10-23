from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from av1_encoder.cli import main
from av1_encoder.config import EncodingConfig


class TestCLIの引数パース:
    """CLIの引数パースのテスト"""

    def test_最小限の引数でパース(self):
        """入力ファイルのみ指定した場合の引数パースをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            # EncodingOrchestratorが正しい設定で呼び出されたことを確認
            assert mock_orchestrator_class.call_count == 1
            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('input.mp4')
            assert config.parallel_jobs == 4  # デフォルト値
            assert config.crf is None
            assert config.preset is None
            assert config.keyint is None
            assert config.s3_bucket == 'xxx'  # デフォルト値

            # runが呼び出されたことを確認
            mock_orchestrator.run.assert_called_once()

            # 成功コードを返すことを確認
            assert result == 0

    def test_全ての引数を指定してパース(self):
        """全ての引数を指定した場合の引数パースをテスト"""
        test_args = [
            'prog',
            'video.mkv',
            '--parallel', '8',
            '--crf', '30',
            '--preset', '6',
            '--keyint', '240',
            '--bucket', 'my-bucket'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            # EncodingOrchestratorが正しい設定で呼び出されたことを確認
            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('video.mkv')
            assert config.parallel_jobs == 8
            assert config.crf == 30
            assert config.preset == 6
            assert config.keyint == 240
            assert config.s3_bucket == 'my-bucket'

            assert result == 0

    def test_短縮オプションでパラレルを指定(self):
        """短縮オプション -l でparallel_jobsを指定できることをテスト"""
        test_args = ['prog', 'input.mp4', '-l', '16']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.parallel_jobs == 16
            assert result == 0

    def test_一部のオプションのみ指定(self):
        """一部のオプションのみを指定した場合のテスト"""
        test_args = [
            'prog',
            'test.mp4',
            '--crf', '25',
            '--bucket', 'test-bucket'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            config = mock_orchestrator_class.call_args[0][0]

            assert config.input_file == Path('test.mp4')
            assert config.parallel_jobs == 4  # デフォルト
            assert config.crf == 25
            assert config.preset is None
            assert config.keyint is None
            assert config.s3_bucket == 'test-bucket'
            assert result == 0


class TestCLIのmain関数:
    """CLIのmain関数のテスト"""

    def test_エンコード成功時に0を返す(self):
        """エンコードが成功した場合に0を返すことをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.return_value = None

            result = main()

            assert result == 0

    def test_エンコード失敗時に1を返す(self):
        """エンコードが失敗した場合に1を返すことをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = RuntimeError("エンコードエラー")

            result = main()

            assert result == 1

    def test_例外が発生しても1を返す(self):
        """任意の例外が発生した場合に1を返すことをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = Exception("予期しないエラー")

            result = main()

            assert result == 1

    def test_orchestratorのrunが呼び出される(self):
        """orchestrator.run()が呼び出されることをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            mock_orchestrator.run.assert_called_once()


class TestCLIのEncodingConfig作成:
    """CLIからEncodingConfigを作成するテスト"""

    def test_EncodingConfigが正しく作成される(self):
        """CLIの引数からEncodingConfigが正しく作成されることをテスト"""
        test_args = [
            'prog',
            'my_video.mp4',
            '--parallel', '12',
            '--crf', '28',
            '--preset', '5',
            '--keyint', '120',
            '--bucket', 'custom-bucket'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            # EncodingConfigのインスタンスが作成されたことを確認
            assert mock_orchestrator_class.call_count == 1
            config = mock_orchestrator_class.call_args[0][0]

            assert isinstance(config, EncodingConfig)
            assert config.input_file == Path('my_video.mp4')
            assert config.parallel_jobs == 12
            assert config.crf == 28
            assert config.preset == 5
            assert config.keyint == 120
            assert config.s3_bucket == 'custom-bucket'

    def test_input_fileがPathオブジェクトに変換される(self):
        """input_file引数がPathオブジェクトに変換されることをテスト"""
        test_args = ['prog', 'test/path/to/video.mkv']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert isinstance(config.input_file, Path)
            assert config.input_file == Path('test/path/to/video.mkv')


class TestCLIのデフォルト値:
    """CLIのデフォルト値のテスト"""

    def test_parallelのデフォルト値は4(self):
        """parallelのデフォルト値が4であることをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.parallel_jobs == 4

    def test_bucketのデフォルト値(self):
        """bucketのデフォルト値が正しいことをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.s3_bucket == 'xxx'

    def test_オプション引数のデフォルト値はNone(self):
        """オプション引数（crf, preset, keyint）のデフォルト値がNoneであることをテスト"""
        test_args = ['prog', 'input.mp4']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.crf is None
            assert config.preset is None
            assert config.keyint is None


class TestCLIのargparse動作:
    """argparseの動作に関するテスト"""

    def test_引数なしでヘルプが表示される(self):
        """引数なしで実行した場合にSystemExitが発生することをテスト"""
        test_args = ['prog']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_不正な引数でSystemExit(self):
        """不正な引数を指定した場合にSystemExitが発生することをテスト"""
        test_args = ['prog', 'input.mp4', '--invalid-option', 'value']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_整数型の引数に文字列を指定してSystemExit(self):
        """整数型の引数に文字列を指定した場合にSystemExitが発生することをテスト"""
        test_args = ['prog', 'input.mp4', '--parallel', 'not-a-number']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()


class TestCLIの引数型:
    """CLIの引数の型に関するテスト"""

    def test_整数引数が正しく変換される(self):
        """整数引数が正しく変換されることをテスト"""
        test_args = [
            'prog',
            'input.mp4',
            '--parallel', '10',
            '--crf', '35',
            '--preset', '8',
            '--keyint', '300'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]

            assert isinstance(config.parallel_jobs, int)
            assert isinstance(config.crf, int)
            assert isinstance(config.preset, int)
            assert isinstance(config.keyint, int)

            assert config.parallel_jobs == 10
            assert config.crf == 35
            assert config.preset == 8
            assert config.keyint == 300

    def test_文字列引数が正しく処理される(self):
        """文字列引数が正しく処理されることをテスト"""
        test_args = [
            'prog',
            'path/to/input.mp4',
            '--bucket', 'my-custom-bucket-name'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]

            assert isinstance(config.s3_bucket, str)
            assert config.s3_bucket == 'my-custom-bucket-name'


class TestCLIのエッジケース:
    """CLIのエッジケースのテスト"""

    def test_負の値を持つ引数(self):
        """負の値を持つ引数が正しく処理されることをテスト"""
        test_args = ['prog', 'input.mp4', '--parallel', '-1']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            # argparseは負の値を許可するので、-1が設定される
            assert config.parallel_jobs == -1

    def test_ゼロを持つ引数(self):
        """0を持つ引数が正しく処理されることをテスト"""
        test_args = ['prog', 'input.mp4', '--crf', '0', '--preset', '0']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.crf == 0
            assert config.preset == 0

    def test_非常に長いファイルパス(self):
        """非常に長いファイルパスが正しく処理されることをテスト"""
        long_path = 'a' * 200 + '.mp4'
        test_args = ['prog', long_path]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == Path(long_path)

    def test_特殊文字を含むファイル名(self):
        """特殊文字を含むファイル名が正しく処理されることをテスト"""
        special_filename = 'test-file_123 (copy).mp4'
        test_args = ['prog', special_filename]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == Path(special_filename)

    def test_空文字列のバケット名(self):
        """空文字列のバケット名が正しく処理されることをテスト"""
        test_args = ['prog', 'input.mp4', '--bucket', '']

        with patch('sys.argv', test_args), \
             patch('av1_encoder.cli.EncodingOrchestrator') as mock_orchestrator_class:

            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            main()

            config = mock_orchestrator_class.call_args[0][0]
            assert config.s3_bucket == ''
