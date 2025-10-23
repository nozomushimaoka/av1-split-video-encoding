from pathlib import Path
from unittest.mock import MagicMock, Mock, patch
import pytest

from av1_encoder.storage import S3Service


@pytest.fixture
def s3_service():
    """モックされたboto3クライアントを持つS3Serviceインスタンスを作成するフィクスチャ"""
    with patch('av1_encoder.storage.boto3.client') as mock_boto3_client:
        mock_s3_client = Mock()
        mock_boto3_client.return_value = mock_s3_client

        service = S3Service()
        service.s3_client = mock_s3_client

        yield service


@pytest.fixture
def temp_dir(tmp_path):
    """テストファイルを含む一時ディレクトリを作成するフィクスチャ"""
    # テストファイルを作成
    test_file1 = tmp_path / "file1.txt"
    test_file1.write_text("content1")

    test_file2 = tmp_path / "file2.txt"
    test_file2.write_text("content2")

    # ネストされたディレクトリを作成
    nested_dir = tmp_path / "subdir"
    nested_dir.mkdir()
    test_file3 = nested_dir / "file3.txt"
    test_file3.write_text("content3")

    return tmp_path


class TestS3Service初期化:
    """S3Serviceの初期化のテスト"""

    def test_初期化時にs3クライアントが作成される(self):
        """S3Serviceがboto3クライアントで初期化されることをテスト"""
        with patch('av1_encoder.storage.boto3.client') as mock_boto3_client:
            mock_s3_client = Mock()
            mock_boto3_client.return_value = mock_s3_client

            service = S3Service()

            mock_boto3_client.assert_called_once_with('s3')
            assert service.s3_client == mock_s3_client


class TestS3Serviceダウンロード:
    """S3Serviceのdownloadメソッドのテスト"""

    def test_downloadが正しくs3クライアントを呼び出す(self, s3_service, tmp_path):
        """downloadがs3_client.download_fileを正しいパラメータで呼び出すことをテスト"""
        bucket = "test-bucket"
        key = "test-key"
        local_file = tmp_path / "downloaded_file.txt"

        s3_service.download(bucket, key, local_file)

        s3_service.s3_client.download_file.assert_called_once_with(
            bucket, key, str(local_file)
        )


class TestS3Serviceディレクトリアップロード:
    """S3Serviceのupload_directoryメソッドのテスト"""

    def test_単一ファイルのディレクトリをアップロード(self, s3_service, tmp_path):
        """単一ファイルを含むディレクトリをアップロードするテスト"""
        test_file = tmp_path / "single_file.txt"
        test_file.write_text("content")

        bucket = "test-bucket"
        key_prefix = "prefix"

        s3_service.upload_directory(tmp_path, bucket, key_prefix)

        # upload_fileが1回呼び出される
        assert s3_service.s3_client.upload_file.call_count == 1

        # 呼び出し引数を確認
        call_args = s3_service.s3_client.upload_file.call_args
        assert call_args[0][0] == str(test_file)
        assert call_args[0][1] == bucket
        assert call_args[0][2] == "prefix/single_file.txt"

    def test_複数ファイルのディレクトリをアップロード(self, s3_service, temp_dir):
        """複数ファイルを含むディレクトリをアップロードするテスト"""
        bucket = "test-bucket"
        key_prefix = "prefix"

        s3_service.upload_directory(temp_dir, bucket, key_prefix)

        # upload_fileが3回呼び出される（file1.txt、file2.txt、subdir/file3.txt）
        assert s3_service.s3_client.upload_file.call_count == 3

    def test_ネストされたサブディレクトリをアップロード(self, s3_service, temp_dir):
        """ネストされたサブディレクトリを含むディレクトリをアップロードするテスト"""
        bucket = "test-bucket"
        key_prefix = "prefix"

        s3_service.upload_directory(temp_dir, bucket, key_prefix)

        # アップロードされたすべてのS3キーを収集
        uploaded_keys = [
            call_args[0][2]
            for call_args in s3_service.s3_client.upload_file.call_args_list
        ]

        # ネストされたファイルが正しいS3キーを持つことを確認
        assert any("prefix/subdir/file3.txt" in key for key in uploaded_keys)

    def test_プレフィックスなしでディレクトリをアップロード(self, s3_service, tmp_path):
        """キープレフィックスなしでディレクトリをアップロードするテスト"""
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")

        bucket = "test-bucket"

        s3_service.upload_directory(tmp_path, bucket, "")

        # S3キーにプレフィックスがないことを確認
        call_args = s3_service.s3_client.upload_file.call_args
        assert call_args[0][2] == "file.txt"

    def test_空のディレクトリをアップロード(self, s3_service, tmp_path):
        """空のディレクトリをアップロードするテスト"""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        bucket = "test-bucket"
        key_prefix = "prefix"

        s3_service.upload_directory(empty_dir, bucket, key_prefix)

        # upload_fileが呼び出されない
        assert s3_service.s3_client.upload_file.call_count == 0

    def test_スレッドプールを使用してアップロード(self, s3_service, temp_dir):
        """upload_directoryがThreadPoolExecutorを使用することをテスト"""
        bucket = "test-bucket"
        key_prefix = "prefix"

        with patch('av1_encoder.storage.ThreadPoolExecutor') as mock_executor_class, \
             patch('av1_encoder.storage.as_completed') as mock_as_completed:
            mock_executor = MagicMock()
            mock_executor_class.return_value.__enter__.return_value = mock_executor

            # submitが完了したフューチャーを返すようにモック
            mock_future1 = Mock()
            mock_future1.result.return_value = None
            mock_future2 = Mock()
            mock_future2.result.return_value = None
            mock_future3 = Mock()
            mock_future3.result.return_value = None

            futures = [mock_future1, mock_future2, mock_future3]
            mock_executor.submit.side_effect = futures

            # as_completedがフューチャーを返すようにモック
            mock_as_completed.return_value = futures

            s3_service.upload_directory(temp_dir, bucket, key_prefix)

            # ThreadPoolExecutorが正しいmax_workersで作成されたことを確認
            mock_executor_class.assert_called_once_with(
                max_workers=S3Service.NUM_PARALLEL_UPLOAD
            )

            # 各ファイルに対してsubmitが呼び出されたことを確認
            assert mock_executor.submit.call_count == 3


class TestS3Service単一ファイルアップロード:
    """S3Serviceの_upload_single_fileメソッドのテスト"""

    def test_単一ファイルをアップロード(self, s3_service, tmp_path):
        """_upload_single_fileが正しいS3キーを構築してアップロードすることをテスト"""
        test_file = tmp_path / "test_file.txt"
        test_file.write_text("content")

        bucket = "test-bucket"
        key_prefix = "prefix"

        s3_service._upload_single_file(test_file, bucket, tmp_path, key_prefix)

        # upload_fileが正しいパラメータで呼び出されたことを確認
        s3_service.s3_client.upload_file.assert_called_once_with(
            str(test_file), bucket, "prefix/test_file.txt"
        )


class TestS3ServiceのS3キー構築:
    """S3Serviceの_build_s3_keyメソッドのテスト"""

    def test_プレフィックスなしでS3キーを構築(self, s3_service, tmp_path):
        """プレフィックスなしでS3キーを構築するテスト"""
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")

        result = s3_service._build_s3_key(test_file, tmp_path, "")

        assert result == "file.txt"

    def test_プレフィックスありでS3キーを構築(self, s3_service, tmp_path):
        """プレフィックスありでS3キーを構築するテスト"""
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")

        result = s3_service._build_s3_key(test_file, tmp_path, "prefix")

        assert result == "prefix/file.txt"

    def test_ネストされたパスでS3キーを構築(self, s3_service, tmp_path):
        """ネストされたパスでS3キーを構築するテスト"""
        nested_dir = tmp_path / "subdir1" / "subdir2"
        nested_dir.mkdir(parents=True)
        test_file = nested_dir / "file.txt"
        test_file.write_text("content")

        result = s3_service._build_s3_key(test_file, tmp_path, "prefix")

        assert result == "prefix/subdir1/subdir2/file.txt"

    def test_バックスラッシュをスラッシュに変換(self, s3_service, tmp_path):
        """Windowsのパス区切り文字がスラッシュに変換されることをテスト"""
        # バックスラッシュを持つWindowsパスをシミュレート
        nested_dir = tmp_path / "subdir"
        nested_dir.mkdir()
        test_file = nested_dir / "file.txt"
        test_file.write_text("content")

        with patch.object(Path, 'relative_to', return_value=Path('subdir\\file.txt')):
            result = s3_service._build_s3_key(test_file, tmp_path, "prefix")

            # バックスラッシュではなくスラッシュを持つべき
            assert result == "prefix/subdir/file.txt"
            assert "\\" not in result


class TestS3Serviceファイルアップロード:
    """S3Serviceの_upload_fileメソッドのテスト"""

    def test_ファイルをアップロード(self, s3_service, tmp_path):
        """_upload_fileがs3_client.upload_fileを正しく呼び出すことをテスト"""
        test_file = tmp_path / "file.txt"
        test_file.write_text("content")

        bucket = "test-bucket"
        key = "test-key"

        s3_service._upload_file(test_file, bucket, key)

        s3_service.s3_client.upload_file.assert_called_once_with(
            str(test_file), bucket, key
        )
