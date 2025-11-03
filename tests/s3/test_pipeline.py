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


class TestS3Pipelineのlist_input_files:
    """list_input_filesメソッドのテスト"""

    def test_入力ファイル一覧を取得(self, s3_pipeline, mock_s3_client):
        """S3から.mkvファイル一覧を取得することをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/video1.mkv'},
                {'Key': 'input/video2.mkv'},
                {'Key': 'input/video3.mkv'},
                {'Key': 'input/other.txt'},  # .mkv以外は除外
            ]
        }

        files = s3_pipeline.list_input_files()

        assert len(files) == 3
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        assert 'video3.mkv' in files
        assert 'other.txt' not in files

        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='input/'
        )

    def test_ファイルがない場合は空セットを返す(self, s3_pipeline, mock_s3_client):
        """ファイルがない場合は空セットを返すことをテスト"""
        mock_s3_client.list_objects_v2.return_value = {}

        files = s3_pipeline.list_input_files()

        assert files == set()

    def test_S3エラー時に例外を発生(self, s3_pipeline, mock_s3_client):
        """S3からのファイル取得に失敗した場合に例外を発生することをテスト"""
        mock_s3_client.list_objects_v2.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchBucket', 'Message': 'The bucket does not exist'}},
            'ListObjectsV2'
        )

        with pytest.raises(ClientError):
            s3_pipeline.list_input_files()


class TestS3Pipelineのlist_output_files:
    """list_output_filesメソッドのテスト"""

    def test_出力ファイル一覧を取得(self, s3_pipeline, mock_s3_client):
        """S3から出力ファイル一覧を取得することをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'output/video1'},
                {'Key': 'output/video2'},
            ]
        }

        files = s3_pipeline.list_output_files()

        assert len(files) == 2
        assert 'video1' in files
        assert 'video2' in files

        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='output/'
        )

    def test_出力ファイルがない場合は空セットを返す(self, s3_pipeline, mock_s3_client):
        """出力ファイルがない場合は空セットを返すことをテスト"""
        mock_s3_client.list_objects_v2.return_value = {}

        files = s3_pipeline.list_output_files()

        assert files == set()


class TestS3Pipelineのcalculate_pending_files:
    """calculate_pending_filesメソッドのテスト"""

    def test_未処理ファイルを計算(self, s3_pipeline):
        """未処理ファイルを正しく計算することをテスト"""
        with patch.object(s3_pipeline, 'list_input_files') as mock_list_input, \
             patch.object(s3_pipeline, 'list_output_files') as mock_list_output:

            mock_list_input.return_value = {
                'video1.mkv',
                'video2.mkv',
                'video3.mkv',
                'video4.mkv'
            }
            mock_list_output.return_value = {
                'video1',  # 既に処理済み
                'video2'   # 既に処理済み
            }

            pending = s3_pipeline.calculate_pending_files()

            assert len(pending) == 2
            assert 'video3.mkv' in pending
            assert 'video4.mkv' in pending
            # ソートされていることを確認
            assert pending == sorted(pending)

    def test_入力ファイルがない場合はエラー(self, s3_pipeline):
        """入力ファイルがない場合はValueErrorを発生することをテスト"""
        with patch.object(s3_pipeline, 'list_input_files', return_value=set()):
            with pytest.raises(ValueError, match="に.mkvファイルが見つかりません"):
                s3_pipeline.calculate_pending_files()

    def test_すべて処理済みの場合は空リスト(self, s3_pipeline):
        """すべて処理済みの場合は空リストを返すことをテスト"""
        with patch.object(s3_pipeline, 'list_input_files') as mock_list_input, \
             patch.object(s3_pipeline, 'list_output_files') as mock_list_output:

            mock_list_input.return_value = {
                'video1.mkv',
                'video2.mkv'
            }
            mock_list_output.return_value = {
                'video1',
                'video2'
            }

            pending = s3_pipeline.calculate_pending_files()

            assert pending == []


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
