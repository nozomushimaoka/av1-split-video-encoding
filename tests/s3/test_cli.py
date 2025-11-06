"""S3 CLI のテスト"""

from unittest.mock import patch, Mock
import os
import pytest

from av1_encoder.s3.cli import main, setup_logging


class TestSetupLogging:
    """setup_logging関数のテスト"""

    def test_ロギングが設定される(self):
        """ロギングが正しく設定されることをテスト"""
        import logging

        # ロガーをリセット
        s3_logger = logging.getLogger('av1_encoder.s3')
        s3_logger.handlers.clear()

        setup_logging()

        # S3ロガーにハンドラーが追加されたことを確認
        assert len(s3_logger.handlers) == 1

        # ハンドラーがStreamHandlerであることを確認
        handler = s3_logger.handlers[0]
        assert isinstance(handler, logging.StreamHandler)

        # ログレベルがINFOに設定されていることを確認
        assert s3_logger.level == logging.INFO

        # 親ロガーへの伝播が無効化されていることを確認
        assert s3_logger.propagate is False


class TestMainのコマンドライン引数:
    """CLIのコマンドライン引数のテスト"""

    def test_すべての引数を指定(self):
        """すべての引数を指定した場合のテスト"""
        test_args = [
            'prog',
            '--bucket', 'my-bucket',
            '--parallel', '8',
            '--',
            '-crf', '30',
            '-preset', '5'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # run_batch_encodingが正しい引数で呼ばれたことを確認
            mock_run.assert_called_once_with(
                bucket='my-bucket',
                parallel=8,
                extra_args=['-crf', '30', '-preset', '5']
            )
            assert result == 0

    def test_並列数のみ指定(self):
        """並列数のみを指定した場合のテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '10'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # extra_argsが空のリストで呼ばれたことを確認
            mock_run.assert_called_once_with(
                bucket='test-bucket',
                parallel=10,
                extra_args=[]
            )
            assert result == 0

    def test_環境変数からバケット名を取得(self):
        """環境変数S3_BUCKETからバケット名を取得することをテスト"""
        test_args = ['prog', '--parallel', '5']

        with patch('sys.argv', test_args), \
             patch.dict(os.environ, {'S3_BUCKET': 'env-bucket'}), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # 環境変数のバケット名が使用されたことを確認
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['bucket'] == 'env-bucket'
            assert result == 0

    def test_コマンドライン引数が環境変数より優先される(self):
        """コマンドライン引数が環境変数より優先されることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'cli-bucket',
            '--parallel', '5'
        ]

        with patch('sys.argv', test_args), \
             patch.dict(os.environ, {'S3_BUCKET': 'env-bucket'}), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # コマンドライン引数のバケット名が使用されたことを確認
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['bucket'] == 'cli-bucket'
            assert result == 0

    def test_バケット名が指定されていない場合はエラー(self):
        """バケット名が指定されていない場合はエラーを返すことをテスト"""
        test_args = ['prog', '--parallel', '5']

        with patch('sys.argv', test_args), \
             patch.dict(os.environ, {}, clear=True):
            result = main()

            # エラーコードを返すことを確認
            assert result == 1

    def test_並列数が指定されていない場合はエラー(self):
        """並列数が指定されていない場合はSystemExitが発生することをテスト"""
        test_args = ['prog', '--bucket', 'test-bucket']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()


class TestMainの実行:
    """main関数の実行のテスト"""

    def test_処理成功時に0を返す(self):
        """処理が成功した場合に0を返すことをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '5'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            assert result == 0

    def test_処理失敗時に1を返す(self):
        """処理が失敗した場合に1を返すことをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '5'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 1

            result = main()

            assert result == 1

    def test_setup_loggingが呼ばれる(self):
        """setup_loggingが呼ばれることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '5'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.setup_logging') as mock_setup_logging, \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            mock_setup_logging.assert_called_once()


class TestMainの引数型:
    """main関数の引数の型のテスト"""

    def test_整数引数が正しく変換される(self):
        """整数引数が正しく変換されることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '12'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # 整数型で呼ばれたことを確認
            call_kwargs = mock_run.call_args[1]
            assert isinstance(call_kwargs['parallel'], int)
            assert call_kwargs['parallel'] == 12

    def test_不正な整数値でSystemExit(self):
        """不正な整数値を指定した場合にSystemExitが発生することをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', 'invalid'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_extra_argsが正しくリストとして渡される(self):
        """extra_argsが正しくリストとして渡されることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '5',
            '--',
            '-crf', '30',
            '-preset', '6'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # extra_argsがリストで渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert isinstance(call_kwargs['extra_args'], list)
            assert call_kwargs['extra_args'] == ['-crf', '30', '-preset', '6']


class TestMainのエッジケース:
    """main関数のエッジケースのテスト"""

    def test_負の値を持つ引数(self):
        """負の値を持つ引数が正しく処理されることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '-1'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # 負の値が渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['parallel'] == -1

    def test_非常に大きな値を持つ引数(self):
        """非常に大きな値を持つ引数が正しく処理されることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--parallel', '1000000'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # 大きな値が渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['parallel'] == 1000000

    def test_特殊文字を含むバケット名(self):
        """特殊文字を含むバケット名が正しく処理されることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket-123_456',
            '--parallel', '5'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # 特殊文字を含むバケット名が渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['bucket'] == 'test-bucket-123_456'

    def test_ショートオプションlが使用できる(self):
        """ショートオプション-lが使用できることをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '-l', '8'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # ショートオプションが正しく処理されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['parallel'] == 8


class TestMainのargparse動作:
    """argparseの動作に関するテスト"""

    def test_ヘルプオプション(self):
        """ヘルプオプションでSystemExitが発生することをテスト"""
        test_args = ['prog', '--help']

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit) as exc_info:
                main()

            # ヘルプは正常終了（コード0）
            assert exc_info.value.code == 0

    def test_不明なオプションでSystemExit(self):
        """不明なオプションでSystemExitが発生することをテスト"""
        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--unknown-option', 'value'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()
