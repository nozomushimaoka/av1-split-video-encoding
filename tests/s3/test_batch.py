"""Tests for S3 batch encoding processing"""

from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from av1_encoder.s3.batch_orchestrator import run_batch_encoding
from av1_encoder.s3.file_processor import encode_video, process_single_file
from av1_encoder.s3.video_merger import merge_video_with_audio


@pytest.fixture
def mock_workspace(tmp_path):
    """Create a workspace for testing"""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return workspace


@pytest.fixture
def mock_s3_pipeline():
    """Create a mock S3Pipeline"""
    pipeline = Mock()
    return pipeline


class TestMergeVideoWithAudio:
    """Tests for the merge_video_with_audio function"""

    def test_merge_video_and_audio(self, mock_workspace, tmp_path):
        """Test that video and audio are merged successfully"""
        # Create concat.txt
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\nfile 'segment_1.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)

            merge_video_with_audio(mock_workspace, input_file, output_file)

            # Verify the correct command was called
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0] == 'ffmpeg'
            assert '-f' in cmd
            assert 'concat' in cmd
            assert '-i' in cmd
            assert str(concat_file) in cmd
            assert str(input_file) in cmd
            assert '-map' in cmd
            assert '0:v:0' in cmd
            assert '1:a?' in cmd
            assert '-c:v' in cmd
            assert 'copy' in cmd
            assert '-c:a' in cmd
            assert str(output_file) in cmd

    def test_error_when_concat_txt_missing(self, mock_workspace, tmp_path):
        """Test that FileNotFoundError is raised when concat.txt does not exist"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with pytest.raises(FileNotFoundError, match="concat.txt not found"):
            merge_video_with_audio(mock_workspace, input_file, output_file)

    def test_raises_exception_on_merge_failure(self, mock_workspace, tmp_path):
        """Test that an exception is raised when merging fails"""
        # Create concat.txt
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.side_effect = Exception("merge error")

            with pytest.raises(Exception, match="merge error"):
                merge_video_with_audio(mock_workspace, input_file, output_file)

    def test_merge_with_audio_args(self, mock_workspace, tmp_path):
        """Test merging video and audio with specified audio arguments"""
        # Create concat.txt
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\nfile 'segment_1.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)

            merge_video_with_audio(mock_workspace, input_file, output_file, audio_args=['-c:a', 'aac', '-b:a', '128k'])

            # Verify the correct command was called
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0] == 'ffmpeg'
            # Verify audio args were added
            assert '-c:a' in cmd
            assert 'aac' in cmd
            assert '-b:a' in cmd
            assert '128k' in cmd
            # copy should not be present
            copy_indices = [i for i, x in enumerate(cmd) if x == 'copy']
            # only the -c:v copy's copy should exist
            assert len(copy_indices) == 1

    def test_use_copy_when_no_audio_args(self, mock_workspace, tmp_path):
        """Test that copy is used by default when no audio args are provided"""
        # Create concat.txt
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)

            merge_video_with_audio(mock_workspace, input_file, output_file, audio_args=None)

            # Verify the correct command was called
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert '-c:a' in cmd
            # copy is used by default
            ca_index = cmd.index('-c:a')
            assert cmd[ca_index + 1] == 'copy'


class TestEncodeVideo:
    """Tests for the encode_video function"""

    def test_run_encoding(self, tmp_path):
        """Test that encoding runs successfully"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # Patch via the encoding.encoder module path where it's imported
        with patch('av1_encoder.encoding.encoder.EncodingOrchestrator') as mock_orchestrator_class:
            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            encode_video(input_file, workspace, parallel=8, gop_size=240, svtav1_args=['--crf', '30', '--preset', '5'])

            # Verify EncodingOrchestrator was called correctly
            mock_orchestrator_class.assert_called_once()
            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == input_file
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 8
            assert config.svtav1_args == ['--crf', '30', '--preset', '5']
            assert config.segment_length == 60

            # Verify run was called
            mock_orchestrator.run.assert_called_once()

    def test_raises_exception_on_encode_failure(self, tmp_path):
        """Test that an exception is raised when encoding fails"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # Patch via the encoding.encoder module path where it's imported
        with patch('av1_encoder.encoding.encoder.EncodingOrchestrator') as mock_orchestrator_class:
            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = RuntimeError("encoding error")

            with pytest.raises(RuntimeError, match="encoding error"):
                encode_video(input_file, workspace, parallel=8, gop_size=240, svtav1_args=['--crf', '30', '--preset', '5'])

    def test_run_encoding_with_audio_args(self, tmp_path):
        """Test that encoding with audio arguments runs successfully"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # Patch via the encoding.encoder module path where it's imported
        with patch('av1_encoder.encoding.encoder.EncodingOrchestrator') as mock_orchestrator_class:
            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            encode_video(
                input_file, workspace, parallel=8, gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5'],
                audio_args=['-c:a', 'aac', '-b:a', '128k']
            )

            # Verify EncodingOrchestrator was called correctly
            mock_orchestrator_class.assert_called_once()
            config = mock_orchestrator_class.call_args[0][0]
            assert config.audio_args == ['-c:a', 'aac', '-b:a', '128k']

            # Verify run was called
            mock_orchestrator.run.assert_called_once()


class TestProcessSingleFile:
    """Tests for the process_single_file function"""

    def test_process_s3_file(self, mock_s3_pipeline, tmp_path):
        """Test processing an S3 file"""
        # Create workspace and add temporary files
        workspace = None

        def mock_encode_impl(input_f, ws, parallel, gop_size, svtav1_args, ffmpeg_args=None, audio_args=None,
                             hardware_decode=None, hardware_decode_device=None):
            nonlocal workspace
            workspace = ws
            # Create temporary files (concat.txt, segment files, logs)
            (ws / "concat.txt").touch()
            (ws / "segment_0000.ivf").touch()
            (ws / "segment_0001.ivf").touch()
            (ws / "main.log").touch()

        def mock_merge_impl(ws, input_f, output_f, audio_args=None):
            # Create output.mkv
            output_f.touch()

        # Mock download_file to create the file when called
        def mock_download(bucket, key, local_path, show_progress=True):
            local_path.touch()

        mock_s3_pipeline.download_file.side_effect = mock_download

        with patch('av1_encoder.s3.file_processor.encode_video', side_effect=mock_encode_impl) as mock_encode, \
             patch('av1_encoder.s3.file_processor.merge_video_with_audio', side_effect=mock_merge_impl) as mock_merge:

            mock_future = Mock()
            mock_s3_pipeline.upload_file_async.return_value = mock_future

            result = process_single_file(
                input_file_path='s3://test-bucket/input/test.mkv',
                output_dir='s3://test-bucket/output/',
                workspace_base=tmp_path,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5'],
                s3=mock_s3_pipeline
            )

            # Verify encode and merge were called
            assert mock_encode.call_count == 1
            assert mock_merge.call_count == 1

            # Verify download was called
            mock_s3_pipeline.download_file.assert_called_once()

            # Verify upload was called
            assert mock_s3_pipeline.upload_file_async.call_count == 1

            # Return value is None
            assert result is None

            # Verify upload completion wait was called
            mock_future.result.assert_called_once()

    def test_process_local_file(self, tmp_path):
        """Test processing a local file"""
        # Create input file
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        input_file = input_dir / "test.mkv"
        input_file.touch()

        output_dir = tmp_path / "output"
        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        # Create workspace and add temporary files
        workspace = None

        def mock_encode_impl(input_f, ws, parallel, gop_size, svtav1_args, ffmpeg_args=None, audio_args=None,
                             hardware_decode=None, hardware_decode_device=None):
            nonlocal workspace
            workspace = ws
            # Create temporary files
            (ws / "concat.txt").touch()
            (ws / "segment_0000.ivf").touch()
            (ws / "main.log").touch()

        def mock_merge_impl(ws, input_f, output_f, audio_args=None):
            # Create output.mkv
            output_f.touch()

        with patch('av1_encoder.s3.file_processor.encode_video', side_effect=mock_encode_impl) as mock_encode, \
             patch('av1_encoder.s3.file_processor.merge_video_with_audio', side_effect=mock_merge_impl) as mock_merge:

            result = process_single_file(
                input_file_path=str(input_file),
                output_dir=str(output_dir),
                workspace_base=workspace_base,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5'],
                s3=None  # No S3 pipeline needed for local files
            )

            # Verify encode and merge were called
            assert mock_encode.call_count == 1
            assert mock_merge.call_count == 1

            # Return value is None
            assert result is None

            # Verify local file is not deleted
            assert input_file.exists()

            # Verify file was copied to output directory
            assert output_dir.exists()
            assert (output_dir / "test.mkv").exists()

    def test_wait_for_previous_download(self, mock_s3_pipeline, tmp_path):
        """Test that the previous download is waited for"""
        # Mock download_file to create the file when called
        def mock_download(bucket, key, local_path, show_progress=True):
            local_path.touch()

        mock_s3_pipeline.download_file.side_effect = mock_download

        download_future = Mock()

        with patch('av1_encoder.s3.file_processor.encode_video'), \
             patch('av1_encoder.s3.file_processor.merge_video_with_audio'):

            mock_upload_future = Mock()
            mock_s3_pipeline.upload_file_async.return_value = mock_upload_future

            process_single_file(
                input_file_path='s3://test-bucket/input/test.mkv',
                output_dir='s3://test-bucket/output/',
                workspace_base=tmp_path,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5'],
                s3=mock_s3_pipeline,
                download_future=download_future
            )

            # Verify result was called on the previous download future
            download_future.result.assert_called_once()

    def test_raises_exception_on_error(self, mock_s3_pipeline, tmp_path):
        """Test that an exception is raised when an error occurs during processing"""
        # Mock download_file to create the file when called
        def mock_download(bucket, key, local_path, show_progress=True):
            local_path.touch()

        mock_s3_pipeline.download_file.side_effect = mock_download

        with patch('av1_encoder.s3.file_processor.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("encoding error")

            with pytest.raises(RuntimeError, match="encoding error"):
                process_single_file(
                    input_file_path='s3://test-bucket/input/test.mkv',
                    output_dir='s3://test-bucket/output/',
                    workspace_base=tmp_path,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5'],
                    s3=mock_s3_pipeline
                )

    def test_delete_input_file_on_s3_error(self, mock_s3_pipeline, tmp_path):
        """Test that the input file is deleted when an error occurs with an S3 file"""
        input_file = tmp_path / "test.mkv"

        # Mock download_file to create the file when called
        def mock_download(bucket, key, local_path, show_progress=True):
            local_path.touch()

        mock_s3_pipeline.download_file.side_effect = mock_download

        with patch('av1_encoder.s3.file_processor.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("encoding error")

            with pytest.raises(RuntimeError, match="encoding error"):
                process_single_file(
                    input_file_path='s3://test-bucket/input/test.mkv',
                    output_dir='s3://test-bucket/output/',
                    workspace_base=tmp_path,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5'],
                    s3=mock_s3_pipeline
                )

            # Verify input file was deleted after error
            assert not input_file.exists()

    def test_do_not_delete_local_file_on_error(self, tmp_path):
        """Test that the input file is not deleted when an error occurs with a local file"""
        # Create input file
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        input_file = input_dir / "test.mkv"
        input_file.touch()

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.file_processor.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("encoding error")

            with pytest.raises(RuntimeError, match="encoding error"):
                process_single_file(
                    input_file_path=str(input_file),
                    output_dir=str(tmp_path / "output"),
                    workspace_base=workspace_base,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5'],
                    s3=None
                )

            # Verify local file is not deleted after error
            assert input_file.exists()


class TestRunBatchEncoding:
    """Tests for the run_batch_encoding function"""

    def test_run_s3_batch_encoding(self, mock_s3_pipeline, tmp_path):
        """Test that S3 batch encoding runs successfully"""
        # Create pending files file (with S3 URIs)
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("s3://test-bucket/input/video1.mkv\ns3://test-bucket/input/video2.mkv\n")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.batch_orchestrator.S3Pipeline', return_value=mock_s3_pipeline) as mock_pipeline_class:
            with patch('av1_encoder.s3.batch_orchestrator.process_single_file') as mock_process:
                # process_single_file returns None
                mock_process.return_value = None

                result = run_batch_encoding(
                    pending_files_path=pending_files_path,
                    output_dir='s3://test-bucket/output/',
                    workspace_base=workspace_base,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5']
                )

                # Verify S3Pipeline was initialized
                mock_pipeline_class.assert_called_once()

                # Verify each file was processed
                assert mock_process.call_count == 2

                # Verify shutdown was called
                mock_s3_pipeline.shutdown.assert_called_once()

                # Verify success code is returned
                assert result == 0

    def test_run_local_batch_encoding(self, tmp_path):
        """Test that local file batch encoding runs successfully"""
        # Create pending files file (with local paths)
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "video1.mkv").touch()
        (input_dir / "video2.mkv").touch()

        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text(f"{input_dir}/video1.mkv\n{input_dir}/video2.mkv\n")

        output_dir = tmp_path / "output"
        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.batch_orchestrator.process_single_file') as mock_process:
            # process_single_file returns None
            mock_process.return_value = None

            result = run_batch_encoding(
                pending_files_path=pending_files_path,
                output_dir=str(output_dir),
                workspace_base=workspace_base,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # Verify each file was processed
            assert mock_process.call_count == 2

            # Verify success code is returned
            assert result == 0

    def test_exit_when_no_files_to_process(self, tmp_path):
        """Test that processing exits when there are no files to process"""
        # Create an empty pending files file
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        result = run_batch_encoding(
            pending_files_path=pending_files_path,
            output_dir=str(tmp_path / "output"),
            workspace_base=workspace_base,
            parallel=8,
            gop_size=240,
            svtav1_args=['--crf', '30', '--preset', '5']
        )

        # Verify success code is returned
        assert result == 0

    def test_return_error_code_on_s3_pipeline_init_failure(self, tmp_path):
        """Test that an error code is returned when S3 pipeline initialization fails"""
        # Create pending files file (with S3 URIs)
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("s3://test-bucket/input/video1.mkv\n")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.batch_orchestrator.S3Pipeline') as mock_pipeline_class:
            mock_pipeline_class.side_effect = Exception("init error")

            result = run_batch_encoding(
                pending_files_path=pending_files_path,
                output_dir='s3://test-bucket/output/',
                workspace_base=workspace_base,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # Verify error code is returned
            assert result == 1

    def test_return_error_code_when_file_list_not_found(self, tmp_path):
        """Test that an error code is returned when the file list is not found"""
        # Use a nonexistent pending files path
        pending_files_path = tmp_path / "nonexistent.txt"

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        result = run_batch_encoding(
            pending_files_path=pending_files_path,
            output_dir=str(tmp_path / "output"),
            workspace_base=workspace_base,
            parallel=8,
            gop_size=240,
            svtav1_args=['--crf', '30', '--preset', '5']
        )

        # Verify error code is returned
        assert result == 1

    def test_start_next_s3_file_download_in_background(self, mock_s3_pipeline, tmp_path):
        """Test that the next S3 file download is started in the background"""
        # Create pending files file (with S3 URIs)
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text(
            "s3://test-bucket/input/video1.mkv\n"
            "s3://test-bucket/input/video2.mkv\n"
            "s3://test-bucket/input/video3.mkv\n"
        )

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.batch_orchestrator.S3Pipeline', return_value=mock_s3_pipeline):
            mock_download_future1 = Mock()
            mock_download_future2 = Mock()
            mock_s3_pipeline.download_file_async.side_effect = [mock_download_future1, mock_download_future2]

            with patch('av1_encoder.s3.batch_orchestrator.process_single_file') as mock_process:
                mock_process.return_value = None

                result = run_batch_encoding(
                    pending_files_path=pending_files_path,
                    output_dir='s3://test-bucket/output/',
                    workspace_base=workspace_base,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5']
                )

                # Verify download was started twice (for the 2nd and 3rd files)
                assert mock_s3_pipeline.download_file_async.call_count == 2

                assert result == 0
