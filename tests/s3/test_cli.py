"""S3 CLI のテスト"""

import logging
import os
from unittest.mock import Mock, patch

import pytest

from av1_encoder.core.logging_config import setup_console_logger
from av1_encoder.s3.cli import main


class TestSetupConsoleLogger:
    """setup_console_logger関数のテスト（ログ設定モジュールに移動済み）"""

    def test_ロギングが設定される(self):
        """ロギングが正しく設定されることをテスト"""
        # ロガーをリセット
        s3_logger = logging.getLogger('av1_encoder.s3')
        s3_logger.handlers.clear()

        setup_console_logger('av1_encoder.s3')

        # S3ロガーにハンドラーが追加されたことを確認
        assert len(s3_logger.handlers) == 1

        # ハンドラーがStreamHandlerであることを確認
        handler = s3_logger.handlers[0]
        assert isinstance(handler, logging.StreamHandler)

        # ログレベルがINFOに設定されていることを確認
        assert s3_logger.level == logging.INFO

        # 親ロガーへの伝播が無効化されていることを確認
        assert s3_logger.propagate is False

    def test_既存ハンドラがある場合はスキップ(self):
        """既存のハンドラがある場合はスキップされることをテスト"""
        # ロガーをリセット
        s3_logger = logging.getLogger('av1_encoder.s3')
        s3_logger.handlers.clear()

        # 最初の呼び出し
        setup_console_logger('av1_encoder.s3')
        assert len(s3_logger.handlers) == 1

        # 2回目の呼び出し（ハンドラは追加されない）
        setup_console_logger('av1_encoder.s3')
        assert len(s3_logger.handlers) == 1  # 変わらず1つ


class TestMainのコマンドライン引数:
    """CLIのコマンドライン引数のテスト"""

    def test_すべての引数を指定(self, tmp_path):
        """すべての引数を指定した場合のテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'my-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '8', '--gop', '240',
            '--svtav1-params', 'crf=30,preset=5'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # run_batch_encodingが正しい引数で呼ばれたことを確認
            mock_run.assert_called_once_with(
                bucket='my-bucket',
                pending_files_path=pending_files_path,
                parallel=8,
                gop_size=240,
                # CLI側で展開されるので、展開後の形式になる
                svtav1_args=['--crf', '30', '--preset', '5'],
                ffmpeg_args=[],
                audio_args=[]
            )
            assert result == 0

    def test_音声パラメータを指定(self, tmp_path):
        """音声パラメータを指定した場合のテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'my-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '8', '--gop', '240',
            '--svtav1-params', 'crf=30,preset=5',
            '--audio-params', 'c:a=aac,b:a=128k'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # audio_argsが展開されたことを確認
            mock_run.assert_called_once_with(
                bucket='my-bucket',
                pending_files_path=pending_files_path,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5'],
                ffmpeg_args=[],
                audio_args=['-c:a', 'aac', '-b:a', '128k']
            )
            assert result == 0

    def test_並列数のみ指定(self, tmp_path):
        """並列数のみを指定した場合のテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '10', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            # svtav1_argsが展開されたことを確認
            mock_run.assert_called_once_with(
                bucket='test-bucket',
                pending_files_path=pending_files_path,
                parallel=10,
                gop_size=240,
                svtav1_args=['--crf', '30'],
                ffmpeg_args=[],
                audio_args=[]
            )
            assert result == 0

    def test_環境変数からバケット名を取得(self, tmp_path):
        """環境変数S3_BUCKETからバケット名を取得することをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

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

    def test_コマンドライン引数が環境変数より優先される(self, tmp_path):
        """コマンドライン引数が環境変数より優先されることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'cli-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
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

    def test_バケット名が指定されていない場合はエラー(self, tmp_path):
        """バケット名が指定されていない場合はエラーを返すことをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

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

    def test_処理成功時に0を返す(self, tmp_path):
        """処理が成功した場合に0を返すことをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            result = main()

            assert result == 0

    def test_処理失敗時に1を返す(self, tmp_path):
        """処理が失敗した場合に1を返すことをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 1

            result = main()

            assert result == 1

    def test_setup_console_loggerが呼ばれる(self, tmp_path):
        """setup_console_loggerが呼ばれることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.setup_console_logger') as mock_setup_logging, \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            mock_setup_logging.assert_called_once_with('av1_encoder.s3')


class TestMainの引数型:
    """main関数の引数の型のテスト"""

    def test_整数引数が正しく変換される(self, tmp_path):
        """整数引数が正しく変換されることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '12', '--gop', '240',
            '--svtav1-params', 'crf=30'
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
            '--parallel', 'invalid', '--gop', '240'
        ]

        with patch('sys.argv', test_args):
            with pytest.raises(SystemExit):
                main()

    def test_svtav1_argsが正しくリストとして渡される(self, tmp_path):
        """svtav1_argsが正しくリストとして渡されることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30,preset=6'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # svtav1_argsがリストで渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert isinstance(call_kwargs['svtav1_args'], list)
            # CLI側で展開されるので、展開後の形式になる
            assert call_kwargs['svtav1_args'] == ['--crf', '30', '--preset', '6']


class TestMainのエッジケース:
    """main関数のエッジケースのテスト"""

    def test_負の値を持つ引数(self, tmp_path):
        """負の値を持つ引数が正しく処理されることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '-1', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # 負の値が渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['parallel'] == -1

    def test_非常に大きな値を持つ引数(self, tmp_path):
        """非常に大きな値を持つ引数が正しく処理されることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '--parallel', '1000000', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # 大きな値が渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['parallel'] == 1000000

    def test_特殊文字を含むバケット名(self, tmp_path):
        """特殊文字を含むバケット名が正しく処理されることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket-123_456',
            '--pending-files', str(pending_files_path),
            '--parallel', '5', '--gop', '240',
            '--svtav1-params', 'crf=30'
        ]

        with patch('sys.argv', test_args), \
             patch('av1_encoder.s3.cli.run_batch_encoding') as mock_run:
            mock_run.return_value = 0

            main()

            # 特殊文字を含むバケット名が渡されたことを確認
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs['bucket'] == 'test-bucket-123_456'

    def test_ショートオプションlが使用できる(self, tmp_path):
        """ショートオプション-lが使用できることをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        test_args = [
            'prog',
            '--bucket', 'test-bucket',
            '--pending-files', str(pending_files_path),
            '-l', '8',
            '--gop', '240',
            '--svtav1-params', 'crf=30'
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
