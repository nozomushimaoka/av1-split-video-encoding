"""S3パイプラインのテスト"""

from unittest.mock import Mock, patch
import pytest
from botocore.exceptions import ClientError

from av1_encoder.s3.pipeline import S3Pipeline, ProgressCallback


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


class TestProgressCallback:
    """ProgressCallbackクラスのテスト"""

    def test_初期化(self):
        """ProgressCallbackが正しく初期化されることをテスト"""
        callback = ProgressCallback("test.mkv", 5 * 1024 * 1024 * 1024)
        assert callback.filename == "test.mkv"
        assert callback.total_size == 5 * 1024 * 1024 * 1024
        assert callback.update_interval == 1024 * 1024 * 1024
        assert callback.accumulated == 0
        assert callback.transferred == 0

    def test_カスタム更新間隔で初期化(self):
        """カスタム更新間隔で初期化できることをテスト"""
        callback = ProgressCallback("test.mkv", 1000, update_interval=100)
        assert callback.update_interval == 100

    def test_1GB未満の転送ではログ出力されない(self, caplog):
        """1GB未満の転送ではログが出力されないことをテスト"""
        import logging
        caplog.set_level(logging.INFO)

        callback = ProgressCallback("test.mkv", 2 * 1024 * 1024 * 1024)

        # 500MB転送（ログは出ない）
        callback(500 * 1024 * 1024)

        # プログレスログが出力されていないことを確認
        progress_logs = [record for record in caplog.records if "GB" in record.message]
        assert len(progress_logs) == 0

        # 内部状態の確認
        assert callback.accumulated == 500 * 1024 * 1024
        assert callback.transferred == 500 * 1024 * 1024

    def test_1GB以上の転送でログ出力(self):
        """1GB以上の転送でログが出力されることをテスト"""
        import logging
        from io import StringIO

        # StringIOハンドラを作成してロガーに追加
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 5 * 1024 * 1024 * 1024)

            # 1.5GB転送（1回ログ出力）
            callback(1.5 * 1024 * 1024 * 1024)

            # ログ内容を取得
            log_output = string_io.getvalue()

            # ログが出力されたことを確認
            assert "test.mkv" in log_output
            assert "GB" in log_output
            assert "%" in log_output

            # accumulatedは1GB分減算されている
            assert callback.accumulated == int(0.5 * 1024 * 1024 * 1024)
            assert callback.transferred == int(1.5 * 1024 * 1024 * 1024)
        finally:
            logger.removeHandler(handler)

    def test_複数回の転送で累積処理(self):
        """複数回の転送で累積が正しく処理されることをテスト"""
        import logging
        from io import StringIO

        # StringIOハンドラを作成してロガーに追加
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 10 * 1024 * 1024 * 1024)

            # 600MB転送（ログなし）
            bytes_600mb = 600 * 1024 * 1024
            callback(bytes_600mb)
            log_output_1 = string_io.getvalue()
            assert "GB" not in log_output_1

            # さらに600MB転送（合計1.2GB、ログ1回）
            callback(bytes_600mb)
            log_output_2 = string_io.getvalue()
            assert "test.mkv" in log_output_2
            assert "GB" in log_output_2

            # 内部状態確認
            assert callback.transferred == bytes_600mb * 2
            # accumulated は 1.2GB - 1GB = 0.2GB
            expected_accumulated = (bytes_600mb * 2) - (1024 * 1024 * 1024)
            assert callback.accumulated == expected_accumulated
        finally:
            logger.removeHandler(handler)

    def test_flushで最終進捗を出力(self):
        """flush時に最終進捗がログ出力されることをテスト"""
        import logging
        from io import StringIO

        # StringIOハンドラを作成してロガーに追加
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 3 * 1024 * 1024 * 1024)

            # 500MB転送
            callback(500 * 1024 * 1024)

            # flush呼び出し
            callback.flush()

            # ログ内容を取得
            log_output = string_io.getvalue()

            # ログが出力されたことを確認
            assert "test.mkv" in log_output
            assert "GB" in log_output
            # 内容確認（0.47GB / 2.79GB程度）
            assert "0.4" in log_output or "0.5" in log_output
        finally:
            logger.removeHandler(handler)

    def test_flush時に転送が0の場合(self, caplog):
        """flush時に転送が0の場合でもログが出力されないことをテスト"""
        import logging
        caplog.set_level(logging.INFO)

        callback = ProgressCallback("test.mkv", 1000)

        # 転送なしでflush
        callback.flush()

        # ログが出力されていないことを確認
        progress_logs = [r for r in caplog.records if "test.mkv" in r.message]
        assert len(progress_logs) == 0

    def test_パーセンテージ計算(self):
        """パーセンテージが正しく計算されることをテスト"""
        import logging
        from io import StringIO

        # StringIOハンドラを作成してロガーに追加
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 10 * 1024 * 1024 * 1024)

            # 5GB転送（50%）
            callback(5 * 1024 * 1024 * 1024)

            # ログ内容を取得
            log_output = string_io.getvalue()

            # パーセンテージが含まれることを確認
            assert "test.mkv" in log_output
            assert "%" in log_output
        finally:
            logger.removeHandler(handler)

    def test_total_sizeが0の場合のパーセンテージ(self):
        """total_sizeが0の場合でもエラーが発生しないことをテスト"""
        callback = ProgressCallback("test.mkv", 0)

        # 例外が発生しないことを確認
        callback(100)
        callback.flush()


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
            mock_download.assert_called_once_with('test.mkv', local_path, show_progress=True)


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
            mock_upload.assert_called_once_with(local_path, 'video1', show_progress=True)


class TestS3Pipelineのshutdown:
    """shutdownメソッドのテスト"""

    def test_エグゼキュータをシャットダウン(self, s3_pipeline):
        """ThreadPoolExecutorをシャットダウンすることをテスト"""
        with patch.object(s3_pipeline.executor, 'shutdown') as mock_shutdown:
            s3_pipeline.shutdown()
            mock_shutdown.assert_called_once_with(wait=True)


class TestS3Pipelineのコンテキストマネージャ:
    """コンテキストマネージャとしての動作テスト"""

    def test_コンテキストマネージャでshutdownが呼ばれる(self, mock_s3_client):
        """withブロック終了時にshutdownが呼ばれることをテスト"""
        with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
            with patch.object(S3Pipeline, 'shutdown') as mock_shutdown:
                with S3Pipeline('test-bucket') as pipeline:
                    assert pipeline.bucket_name == 'test-bucket'

                # withブロック終了後にshutdownが呼ばれたことを確認
                mock_shutdown.assert_called_once()

    def test_コンテキストマネージャで例外発生時もshutdownが呼ばれる(self, mock_s3_client):
        """例外発生時もshutdownが呼ばれることをテスト"""
        with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
            with patch.object(S3Pipeline, 'shutdown') as mock_shutdown:
                try:
                    with S3Pipeline('test-bucket') as pipeline:
                        raise ValueError("テスト例外")
                except ValueError:
                    pass

                # 例外発生後もshutdownが呼ばれたことを確認
                mock_shutdown.assert_called_once()

    def test_コンテキストマネージャが自身を返す(self, mock_s3_client):
        """__enter__が自身を返すことをテスト"""
        with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
            pipeline = S3Pipeline('test-bucket')
            result = pipeline.__enter__()
            assert result is pipeline
            pipeline.shutdown()
