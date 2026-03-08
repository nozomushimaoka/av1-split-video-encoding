"""Tests for S3 pipeline"""

from unittest.mock import Mock, patch
import pytest
from botocore.exceptions import ClientError

from av1_encoder.s3.pipeline import S3Pipeline, ProgressCallback


@pytest.fixture
def mock_s3_client():
    """Create a mock S3 client"""
    client = Mock()
    return client


@pytest.fixture
def s3_pipeline(mock_s3_client):
    """Fixture for S3Pipeline"""
    with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
        pipeline = S3Pipeline()
        return pipeline


class TestProgressCallback:
    """Tests for the ProgressCallback class"""

    def test_initialization(self):
        """Test that ProgressCallback is initialized correctly"""
        callback = ProgressCallback("test.mkv", 5 * 1024 * 1024 * 1024)
        assert callback.filename == "test.mkv"
        assert callback.total_size == 5 * 1024 * 1024 * 1024
        assert callback.update_interval == 1024 * 1024 * 1024
        assert callback.accumulated == 0
        assert callback.transferred == 0

    def test_initialize_with_custom_update_interval(self):
        """Test that initialization with a custom update interval works"""
        callback = ProgressCallback("test.mkv", 1000, update_interval=100)
        assert callback.update_interval == 100

    def test_no_log_output_for_transfers_under_1gb(self, caplog):
        """Test that no log is output for transfers under 1GB"""
        import logging
        caplog.set_level(logging.INFO)

        callback = ProgressCallback("test.mkv", 2 * 1024 * 1024 * 1024)

        # Transfer 500MB (no log output)
        callback(500 * 1024 * 1024)

        # Verify no progress log was output
        progress_logs = [record for record in caplog.records if "GB" in record.message]
        assert len(progress_logs) == 0

        # Check internal state
        assert callback.accumulated == 500 * 1024 * 1024
        assert callback.transferred == 500 * 1024 * 1024

    def test_log_output_for_transfers_over_1gb(self):
        """Test that a log is output for transfers over 1GB"""
        import logging
        from io import StringIO

        # Create StringIO handler and add it to the logger
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 5 * 1024 * 1024 * 1024)

            # Transfer 1.5GB (1 log output)
            callback(1.5 * 1024 * 1024 * 1024)

            # Get log output
            log_output = string_io.getvalue()

            # Verify log was output
            assert "test.mkv" in log_output
            assert "GB" in log_output
            assert "%" in log_output

            # accumulated is reduced by 1GB
            assert callback.accumulated == int(0.5 * 1024 * 1024 * 1024)
            assert callback.transferred == int(1.5 * 1024 * 1024 * 1024)
        finally:
            logger.removeHandler(handler)

    def test_accumulated_processing_over_multiple_transfers(self):
        """Test that accumulation is handled correctly over multiple transfers"""
        import logging
        from io import StringIO

        # Create StringIO handler and add it to the logger
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 10 * 1024 * 1024 * 1024)

            # Transfer 600MB (no log)
            bytes_600mb = 600 * 1024 * 1024
            callback(bytes_600mb)
            log_output_1 = string_io.getvalue()
            assert "GB" not in log_output_1

            # Transfer another 600MB (total 1.2GB, 1 log output)
            callback(bytes_600mb)
            log_output_2 = string_io.getvalue()
            assert "test.mkv" in log_output_2
            assert "GB" in log_output_2

            # Check internal state
            assert callback.transferred == bytes_600mb * 2
            # accumulated is 1.2GB - 1GB = 0.2GB
            expected_accumulated = (bytes_600mb * 2) - (1024 * 1024 * 1024)
            assert callback.accumulated == expected_accumulated
        finally:
            logger.removeHandler(handler)

    def test_flush_outputs_final_progress(self):
        """Test that final progress is logged when flush is called"""
        import logging
        from io import StringIO

        # Create StringIO handler and add it to the logger
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 3 * 1024 * 1024 * 1024)

            # Transfer 500MB
            callback(500 * 1024 * 1024)

            # Call flush
            callback.flush()

            # Get log output
            log_output = string_io.getvalue()

            # Verify log was output
            assert "test.mkv" in log_output
            assert "GB" in log_output
            # Verify content (approximately 0.47GB / 2.79GB)
            assert "0.4" in log_output or "0.5" in log_output
        finally:
            logger.removeHandler(handler)

    def test_no_log_on_flush_when_transferred_is_zero(self, caplog):
        """Test that no log is output when flush is called with zero transferred"""
        import logging
        caplog.set_level(logging.INFO)

        callback = ProgressCallback("test.mkv", 1000)

        # Flush with no transfers
        callback.flush()

        # Verify no log was output
        progress_logs = [r for r in caplog.records if "test.mkv" in r.message]
        assert len(progress_logs) == 0

    def test_percentage_calculation(self):
        """Test that percentage is calculated correctly"""
        import logging
        from io import StringIO

        # Create StringIO handler and add it to the logger
        logger = logging.getLogger('av1_encoder.s3.pipeline')
        logger.setLevel(logging.INFO)
        string_io = StringIO()
        handler = logging.StreamHandler(string_io)
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)

        try:
            callback = ProgressCallback("test.mkv", 10 * 1024 * 1024 * 1024)

            # Transfer 5GB (50%)
            callback(5 * 1024 * 1024 * 1024)

            # Get log output
            log_output = string_io.getvalue()

            # Verify percentage is included
            assert "test.mkv" in log_output
            assert "%" in log_output
        finally:
            logger.removeHandler(handler)

    def test_no_error_when_total_size_is_zero(self):
        """Test that no error occurs when total_size is zero"""
        callback = ProgressCallback("test.mkv", 0)

        # Verify no exception is raised
        callback(100)
        callback.flush()


class TestS3PipelineInitialization:
    """Tests for S3Pipeline initialization"""

    def test_s3_client_is_initialized(self):
        """Test that the S3 client is initialized"""
        with patch('av1_encoder.s3.pipeline.boto3.client') as mock_boto_client:
            pipeline = S3Pipeline()
            mock_boto_client.assert_called_once_with('s3')
            assert pipeline.s3_client is not None


class TestS3PipelineDownloadFile:
    """Tests for the download_file method"""

    def test_download_file(self, s3_pipeline, mock_s3_client, tmp_path):
        """Test downloading a file from S3"""
        local_path = tmp_path / "test.mkv"

        mock_s3_client.head_object.return_value = {'ContentLength': 1024}

        s3_pipeline.download_file('test-bucket', 'input/test.mkv', local_path, show_progress=False)

        mock_s3_client.download_file.assert_called_once_with(
            Bucket='test-bucket',
            Key='input/test.mkv',
            Filename=str(local_path)
        )

    def test_skip_existing_file(self, s3_pipeline, mock_s3_client, tmp_path):
        """Test that download is skipped for files that already exist"""
        local_path = tmp_path / "test.mkv"
        local_path.touch()  # Create the file

        s3_pipeline.download_file('test-bucket', 'input/test.mkv', local_path)

        mock_s3_client.download_file.assert_not_called()

    def test_raises_exception_on_download_failure(self, s3_pipeline, mock_s3_client, tmp_path):
        """Test that an exception is raised when download fails"""
        local_path = tmp_path / "test.mkv"

        mock_s3_client.head_object.return_value = {'ContentLength': 1024}
        mock_s3_client.download_file.side_effect = ClientError(
            {'Error': {'Code': 'NoSuchKey', 'Message': 'The key does not exist'}},
            'GetObject'
        )

        with pytest.raises(ClientError):
            s3_pipeline.download_file('test-bucket', 'input/test.mkv', local_path, show_progress=False)


class TestS3PipelineDownloadFileAsync:
    """Tests for the download_file_async method"""

    def test_start_background_download(self, s3_pipeline, tmp_path):
        """Test that a background download is started"""
        local_path = tmp_path / "test.mkv"

        with patch.object(s3_pipeline, 'download_file') as mock_download:
            future = s3_pipeline.download_file_async('test-bucket', 'input/test.mkv', local_path)

            # Verify a Future object is returned
            assert future is not None

            # Verify download_file is called when waiting for the future's result
            future.result()
            mock_download.assert_called_once_with('test-bucket', 'input/test.mkv', local_path, show_progress=True)


class TestS3PipelineUploadFile:
    """Tests for the upload_file method"""

    def test_upload_file(self, s3_pipeline, mock_s3_client, tmp_path):
        """Test uploading a file to S3"""
        local_path = tmp_path / "output.mkv"
        local_path.write_text("test content")

        s3_pipeline.upload_file(local_path, 'test-bucket', 'output/video1.mkv', show_progress=False)

        mock_s3_client.upload_file.assert_called_once_with(
            Filename=str(local_path),
            Bucket='test-bucket',
            Key='output/video1.mkv'
        )

    def test_raises_exception_on_upload_failure(self, s3_pipeline, mock_s3_client, tmp_path):
        """Test that an exception is raised when upload fails"""
        local_path = tmp_path / "output.mkv"
        local_path.write_text("test content")

        mock_s3_client.upload_file.side_effect = ClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
            'PutObject'
        )

        with pytest.raises(ClientError):
            s3_pipeline.upload_file(local_path, 'test-bucket', 'output/video1.mkv', show_progress=False)


class TestS3PipelineUploadFileAsync:
    """Tests for the upload_file_async method"""

    def test_start_background_upload(self, s3_pipeline, tmp_path):
        """Test that a background upload is started"""
        local_path = tmp_path / "output.mkv"
        local_path.write_text("test content")

        with patch.object(s3_pipeline, 'upload_file') as mock_upload:
            future = s3_pipeline.upload_file_async(local_path, 'test-bucket', 'output/video1.mkv')

            # Verify a Future object is returned
            assert future is not None

            # Verify upload_file is called when waiting for the future's result
            future.result()
            mock_upload.assert_called_once_with(local_path, 'test-bucket', 'output/video1.mkv', show_progress=True)


class TestS3PipelineShutdown:
    """Tests for the shutdown method"""

    def test_shutdown_executor(self, s3_pipeline):
        """Test that the ThreadPoolExecutor is shut down"""
        with patch.object(s3_pipeline.executor, 'shutdown') as mock_shutdown:
            s3_pipeline.shutdown()
            mock_shutdown.assert_called_once_with(wait=True)


class TestS3PipelineContextManager:
    """Tests for context manager behavior"""

    def test_shutdown_called_on_context_exit(self, mock_s3_client):
        """Test that shutdown is called when the with block exits"""
        with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
            with patch.object(S3Pipeline, 'shutdown') as mock_shutdown:
                with S3Pipeline() as pipeline:
                    assert pipeline.s3_client is not None

                # Verify shutdown was called after the with block
                mock_shutdown.assert_called_once()

    def test_shutdown_called_even_on_exception(self, mock_s3_client):
        """Test that shutdown is called even when an exception occurs"""
        with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
            with patch.object(S3Pipeline, 'shutdown') as mock_shutdown:
                try:
                    with S3Pipeline() as pipeline:
                        raise ValueError("test exception")
                except ValueError:
                    pass

                # Verify shutdown was called after the exception
                mock_shutdown.assert_called_once()

    def test_context_manager_returns_self(self, mock_s3_client):
        """Test that __enter__ returns itself"""
        with patch('av1_encoder.s3.pipeline.boto3.client', return_value=mock_s3_client):
            pipeline = S3Pipeline()
            result = pipeline.__enter__()
            assert result is pipeline
            pipeline.shutdown()
