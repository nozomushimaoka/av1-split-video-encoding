"""S3パイプラインのテスト"""

from unittest.mock import Mock, patch
import pytest
from botocore.exceptions import ClientError

from av1_encoder.s3.pipeline import S3Pipeline


@pytest.fixture
def mock_s3_client():
    """S3クライアントのモックを作成"""
    client = Mock()
    return client


@pytest.fixture
def s3_pipeline(mock_s3_client):
    """S3Pipelineのフィクスチャ"""
    with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
        pipeline = S3Pipeline('test-bucket')
        return pipeline


class TestS3Pipeline初期化:
    """S3Pipelineの初期化のテスト"""

    def test_バケット名が設定される(self):
        """バケット名が正しく設定されることをテスト"""
        with patch('av1_encoder.s3.pipeline.boto3.client'):
            pipeline = S3Pipeline('my-bucket')
            assert pipeline.bucket_name == 'my-bucket'

    def test_s3クライアントが初期化される(self):
        """S3クライアントが初期化されることをテスト"""
        with patch('av1_encoder.s3.pipeline.boto3.client') as mock_boto_client:
            pipeline = S3Pipeline('test-bucket')
            mock_boto_client.assert_called_once_with('s3')
            assert pipeline.s3_client is not None


class TestS3Pipelineのdownload_file:
    """download_fileメソッドのテスト"""

    def test_ファイルをダウンロード(self, s3_pipeline, mock_s3_client, tmp_path):
        """S3からファイルをダウンロードすることをテスト"""
        local_path = tmp_path / "test.mkv"

        mock_s3_client.head_object.return_value = {'ContentLength': 1024}

        s3_pipeline.download_file('test.mkv', local_path, show_progress=False)

        mock_s3_client.download_file.assert_called_once_with(
            Bucket='test-bucket',
            Key='input/test.mkv',
            Filename=str(local_path)
        )

    def test_既存ファイルはスキップ(self, s3_pipeline, mock_s3_client, tmp_path):
        """既に存在するファイルはダウンロードをスキップすることをテスト"""
        local_path = tmp_path / "test.mkv"
        local_path.touch()  # ファイルを作成

        s3_pipeline.download_file('test.mkv', local_path)

        mock_s3_client.download_file.assert_not_called()

    def test_ダウンロード失敗時に例外を発生(self, s3_pipeline, mock_s3_client, tmp_path):
        """ダウンロードに失敗した場合に例外を発生することをテスト"""
        local_path = tmp_path / "test.mkv"

        mock_s3_client.head_object.return_value = {'ContentLength': 1024}
        mock_s3_client.download_file.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey', 'Message': 'The key does not exist'}},
            'GetObject'
        )

        with pytest.raises(ClientError):
            s3_pipeline.download_file('test.mkv', local_path, show_progress=False)


class TestS3Pipelineのdownload_file_async:
    """download_file_asyncメソッドのテスト"""

    def test_バックグラウンドダウンロードを開始(self, s3_pipeline, tmp_path):
        """バックグラウンドでダウンロードを開始することをテスト"""
        local_path = tmp_path / "test.mkv"

        with patch.object(s3_pipeline, 'download_file') as mock_download:
            future = s3_pipeline.download_file_async('test.mkv', local_path)

            # Futureオブジェクトが返されることを確認
            assert future is not None

            # Futureの結果を待つとdownload_fileが呼ばれることを確認
            future.result()
            mock_download.assert_called_once_with('test.mkv', local_path, show_progress=False)


class TestS3Pipelineのupload_file:
    """upload_fileメソッドのテスト"""

    def test_ファイルをアップロード(self, s3_pipeline, mock_s3_client, tmp_path):
        """S3へファイルをアップロードすることをテスト"""
        local_path = tmp_path / "output.mkv"
        local_path.write_text("test content")

        s3_pipeline.upload_file(local_path, 'video1', show_progress=False)

        mock_s3_client.upload_file.assert_called_once_with(
            Filename=str(local_path),
            Bucket='test-bucket',
            Key='output/video1'
        )

    def test_アップロード失敗時に例外を発生(self, s3_pipeline, mock_s3_client, tmp_path):
        """アップロードに失敗した場合に例外を発生することをテスト"""
        local_path = tmp_path / "output.mkv"
        local_path.write_text("test content")

        mock_s3_client.upload_file.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            'PutObject'
        )

        with pytest.raises(ClientError):
            s3_pipeline.upload_file(local_path, 'video1', show_progress=False)


class TestS3Pipelineのupload_file_async:
    """upload_file_asyncメソッドのテスト"""

    def test_バックグラウンドアップロードを開始(self, s3_pipeline, tmp_path):
        """バックグラウンドでアップロードを開始することをテスト"""
        local_path = tmp_path / "output.mkv"
        local_path.write_text("test content")

        with patch.object(s3_pipeline, 'upload_file') as mock_upload:
            future = s3_pipeline.upload_file_async(local_path, 'video1')

            # Futureオブジェクトが返されることを確認
            assert future is not None

            # Futureの結果を待つとupload_fileが呼ばれることを確認
            future.result()
            mock_upload.assert_called_once_with(local_path, 'video1', show_progress=False)


class TestS3Pipelineのshutdown:
    """shutdownメソッドのテスト"""

    def test_エグゼキュータをシャットダウン(self, s3_pipeline):
        """ThreadPoolExecutorをシャットダウンすることをテスト"""
        with patch.object(s3_pipeline.executor, 'shutdown') as mock_shutdown:
            s3_pipeline.shutdown()
            mock_shutdown.assert_called_once_with(wait=True)
