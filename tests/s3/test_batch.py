"""S3バッチエンコード処理のテスト"""

from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from av1_encoder.s3.batch_orchestrator import run_batch_encoding
from av1_encoder.s3.file_processor import encode_video, process_single_file
from av1_encoder.s3.video_merger import merge_video_with_audio


@pytest.fixture
def mock_workspace(tmp_path):
    """テスト用のワークスペースを作成"""
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    return workspace


@pytest.fixture
def mock_s3_pipeline():
    """S3Pipelineのモックを作成"""
    pipeline = Mock()
    return pipeline


class TestMergeVideoWithAudio:
    """merge_video_with_audio関数のテスト"""

    def test_動画と音声を結合(self, mock_workspace, tmp_path):
        """動画と音声を正常に結合することをテスト"""
        # concat.txtを作成
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\nfile 'segment_1.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)

            merge_video_with_audio(mock_workspace, input_file, output_file)

            # 正しいコマンドが呼ばれたことを確認
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

    def test_concat_txtが存在しない場合はエラー(self, mock_workspace, tmp_path):
        """concat.txtが存在しない場合にFileNotFoundErrorを発生することをテスト"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with pytest.raises(FileNotFoundError, match="concat.txtが見つかりません"):
            merge_video_with_audio(mock_workspace, input_file, output_file)

    def test_結合に失敗した場合は例外を発生(self, mock_workspace, tmp_path):
        """結合に失敗した場合に例外を発生することをテスト"""
        # concat.txtを作成
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.side_effect = Exception("結合エラー")

            with pytest.raises(Exception, match="結合エラー"):
                merge_video_with_audio(mock_workspace, input_file, output_file)

    def test_音声引数を指定して結合(self, mock_workspace, tmp_path):
        """音声引数を指定して動画と音声を結合することをテスト"""
        # concat.txtを作成
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\nfile 'segment_1.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)

            merge_video_with_audio(mock_workspace, input_file, output_file, audio_args=['-c:a', 'aac', '-b:a', '128k'])

            # 正しいコマンドが呼ばれたことを確認
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0] == 'ffmpeg'
            # 音声引数が追加されていることを確認
            assert '-c:a' in cmd
            assert 'aac' in cmd
            assert '-b:a' in cmd
            assert '128k' in cmd
            # copyは含まれない
            copy_indices = [i for i, x in enumerate(cmd) if x == 'copy']
            # -c:v copyのcopyのみ存在
            assert len(copy_indices) == 1

    def test_音声引数なしの場合はcopyを使用(self, mock_workspace, tmp_path):
        """音声引数がない場合はデフォルトでcopyを使用することをテスト"""
        # concat.txtを作成
        concat_file = mock_workspace / "concat.txt"
        concat_file.write_text("file 'segment_0.ivf'\n")

        input_file = tmp_path / "input.mkv"
        input_file.touch()
        output_file = tmp_path / "output.mkv"

        with patch('av1_encoder.s3.video_merger.subprocess.run') as mock_run:
            mock_run.return_value = Mock(returncode=0)

            merge_video_with_audio(mock_workspace, input_file, output_file, audio_args=None)

            # 正しいコマンドが呼ばれたことを確認
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert '-c:a' in cmd
            # デフォルトでcopyが使用される
            ca_index = cmd.index('-c:a')
            assert cmd[ca_index + 1] == 'copy'


class TestEncodeVideo:
    """encode_video関数のテスト"""

    def test_エンコード処理を実行(self, tmp_path):
        """エンコード処理を正常に実行することをテスト"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # encoding.encoderモジュールからインポートされるのでそのパスをパッチ
        with patch('av1_encoder.encoding.encoder.EncodingOrchestrator') as mock_orchestrator_class:
            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            encode_video(input_file, workspace, parallel=8, gop_size=240, svtav1_args=['--crf', '30', '--preset', '5'])

            # EncodingOrchestratorが正しく呼ばれたことを確認
            mock_orchestrator_class.assert_called_once()
            config = mock_orchestrator_class.call_args[0][0]
            assert config.input_file == input_file
            assert config.workspace_dir == workspace
            assert config.parallel_jobs == 8
            assert config.svtav1_args == ['--crf', '30', '--preset', '5']
            assert config.segment_length == 60

            # runが呼ばれたことを確認
            mock_orchestrator.run.assert_called_once()

    def test_エンコード失敗時に例外を発生(self, tmp_path):
        """エンコードが失敗した場合に例外を発生することをテスト"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # encoding.encoderモジュールからインポートされるのでそのパスをパッチ
        with patch('av1_encoder.encoding.encoder.EncodingOrchestrator') as mock_orchestrator_class:
            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator
            mock_orchestrator.run.side_effect = RuntimeError("エンコードエラー")

            with pytest.raises(RuntimeError, match="エンコードエラー"):
                encode_video(input_file, workspace, parallel=8, gop_size=240, svtav1_args=['--crf', '30', '--preset', '5'])

    def test_音声引数を含むエンコード処理(self, tmp_path):
        """音声引数を含むエンコード処理を実行することをテスト"""
        input_file = tmp_path / "input.mkv"
        input_file.touch()
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # encoding.encoderモジュールからインポートされるのでそのパスをパッチ
        with patch('av1_encoder.encoding.encoder.EncodingOrchestrator') as mock_orchestrator_class:
            mock_orchestrator = Mock()
            mock_orchestrator_class.return_value = mock_orchestrator

            encode_video(
                input_file, workspace, parallel=8, gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5'],
                audio_args=['-c:a', 'aac', '-b:a', '128k']
            )

            # EncodingOrchestratorが正しく呼ばれたことを確認
            mock_orchestrator_class.assert_called_once()
            config = mock_orchestrator_class.call_args[0][0]
            assert config.audio_args == ['-c:a', 'aac', '-b:a', '128k']

            # runが呼ばれたことを確認
            mock_orchestrator.run.assert_called_once()


class TestProcessSingleFile:
    """process_single_file関数のテスト"""

    def test_S3ファイルの処理(self, mock_s3_pipeline, tmp_path):
        """S3ファイルを処理することをテスト"""
        # ワークスペースを作成して一時ファイルを追加
        workspace = None

        def mock_encode_impl(input_f, ws, parallel, gop_size, svtav1_args, ffmpeg_args=None, audio_args=None,
                             hardware_decode=None, hardware_decode_device=None):
            nonlocal workspace
            workspace = ws
            # 一時ファイルを作成（concat.txt, segment files, logs）
            (ws / "concat.txt").touch()
            (ws / "segment_0000.ivf").touch()
            (ws / "segment_0001.ivf").touch()
            (ws / "main.log").touch()

        def mock_merge_impl(ws, input_f, output_f, audio_args=None):
            # output.mkvを作成
            output_f.touch()

        # download_fileが呼ばれたときにファイルを作成するようにモック
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

            # エンコードと結合が呼ばれたことを確認
            assert mock_encode.call_count == 1
            assert mock_merge.call_count == 1

            # ダウンロードが呼ばれたことを確認
            mock_s3_pipeline.download_file.assert_called_once()

            # アップロードが呼ばれたことを確認
            assert mock_s3_pipeline.upload_file_async.call_count == 1

            # 戻り値はNone
            assert result is None

            # アップロード完了待機が呼ばれたことを確認
            mock_future.result.assert_called_once()

    def test_ローカルファイルの処理(self, tmp_path):
        """ローカルファイルを処理することをテスト"""
        # 入力ファイルを作成
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        input_file = input_dir / "test.mkv"
        input_file.touch()

        output_dir = tmp_path / "output"
        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        # ワークスペースを作成して一時ファイルを追加
        workspace = None

        def mock_encode_impl(input_f, ws, parallel, gop_size, svtav1_args, ffmpeg_args=None, audio_args=None,
                             hardware_decode=None, hardware_decode_device=None):
            nonlocal workspace
            workspace = ws
            # 一時ファイルを作成
            (ws / "concat.txt").touch()
            (ws / "segment_0000.ivf").touch()
            (ws / "main.log").touch()

        def mock_merge_impl(ws, input_f, output_f, audio_args=None):
            # output.mkvを作成
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
                s3=None  # ローカルファイルなのでS3パイプラインは不要
            )

            # エンコードと結合が呼ばれたことを確認
            assert mock_encode.call_count == 1
            assert mock_merge.call_count == 1

            # 戻り値はNone
            assert result is None

            # ローカルファイルは削除されないことを確認
            assert input_file.exists()

            # 出力ディレクトリにファイルがコピーされたことを確認
            assert output_dir.exists()
            assert (output_dir / "test.mkv").exists()

    def test_前のダウンロードを待機(self, mock_s3_pipeline, tmp_path):
        """前のダウンロードを待機することをテスト"""
        # download_fileが呼ばれたときにファイルを作成するようにモック
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

            # 前のダウンロードのresultが呼ばれたことを確認
            download_future.result.assert_called_once()

    def test_エラー時に例外を発生(self, mock_s3_pipeline, tmp_path):
        """処理中にエラーが発生した場合に例外を発生することをテスト"""
        # download_fileが呼ばれたときにファイルを作成するようにモック
        def mock_download(bucket, key, local_path, show_progress=True):
            local_path.touch()

        mock_s3_pipeline.download_file.side_effect = mock_download

        with patch('av1_encoder.s3.file_processor.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("エンコードエラー")

            with pytest.raises(RuntimeError, match="エンコードエラー"):
                process_single_file(
                    input_file_path='s3://test-bucket/input/test.mkv',
                    output_dir='s3://test-bucket/output/',
                    workspace_base=tmp_path,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5'],
                    s3=mock_s3_pipeline
                )

    def test_S3ファイルのエラー時に入力ファイルを削除(self, mock_s3_pipeline, tmp_path):
        """S3ファイルでエラー発生時に入力ファイルが削除されることをテスト"""
        input_file = tmp_path / "test.mkv"

        # download_fileが呼ばれたときにファイルを作成するようにモック
        def mock_download(bucket, key, local_path, show_progress=True):
            local_path.touch()

        mock_s3_pipeline.download_file.side_effect = mock_download

        with patch('av1_encoder.s3.file_processor.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("エンコードエラー")

            with pytest.raises(RuntimeError, match="エンコードエラー"):
                process_single_file(
                    input_file_path='s3://test-bucket/input/test.mkv',
                    output_dir='s3://test-bucket/output/',
                    workspace_base=tmp_path,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5'],
                    s3=mock_s3_pipeline
                )

            # エラー後に入力ファイルが削除されたことを確認
            assert not input_file.exists()

    def test_ローカルファイルのエラー時は入力ファイルを削除しない(self, tmp_path):
        """ローカルファイルでエラー発生時に入力ファイルが削除されないことをテスト"""
        # 入力ファイルを作成
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        input_file = input_dir / "test.mkv"
        input_file.touch()

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.file_processor.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("エンコードエラー")

            with pytest.raises(RuntimeError, match="エンコードエラー"):
                process_single_file(
                    input_file_path=str(input_file),
                    output_dir=str(tmp_path / "output"),
                    workspace_base=workspace_base,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5'],
                    s3=None
                )

            # ローカルファイルはエラー後も削除されないことを確認
            assert input_file.exists()


class TestRunBatchEncoding:
    """run_batch_encoding関数のテスト"""

    def test_S3バッチエンコード処理を実行(self, mock_s3_pipeline, tmp_path):
        """S3バッチエンコード処理を正常に実行することをテスト"""
        # pending filesファイルを作成（S3 URIを含む）
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("s3://test-bucket/input/video1.mkv\ns3://test-bucket/input/video2.mkv\n")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.batch_orchestrator.S3Pipeline', return_value=mock_s3_pipeline) as mock_pipeline_class:
            with patch('av1_encoder.s3.batch_orchestrator.process_single_file') as mock_process:
                # process_single_fileはNoneを返す
                mock_process.return_value = None

                result = run_batch_encoding(
                    pending_files_path=pending_files_path,
                    output_dir='s3://test-bucket/output/',
                    workspace_base=workspace_base,
                    parallel=8,
                    gop_size=240,
                    svtav1_args=['--crf', '30', '--preset', '5']
                )

                # S3Pipelineが初期化されたことを確認
                mock_pipeline_class.assert_called_once()

                # 各ファイルが処理されたことを確認
                assert mock_process.call_count == 2

                # shutdownが呼ばれたことを確認
                mock_s3_pipeline.shutdown.assert_called_once()

                # 成功コードを返すことを確認
                assert result == 0

    def test_ローカルバッチエンコード処理を実行(self, tmp_path):
        """ローカルファイルのバッチエンコード処理を正常に実行することをテスト"""
        # pending filesファイルを作成（ローカルパスを含む）
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
            # process_single_fileはNoneを返す
            mock_process.return_value = None

            result = run_batch_encoding(
                pending_files_path=pending_files_path,
                output_dir=str(output_dir),
                workspace_base=workspace_base,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # 各ファイルが処理されたことを確認
            assert mock_process.call_count == 2

            # 成功コードを返すことを確認
            assert result == 0

    def test_処理対象ファイルがない場合は終了(self, tmp_path):
        """処理対象ファイルがない場合は終了することをテスト"""
        # 空のpending filesファイルを作成
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

        # 成功コードを返すことを確認
        assert result == 0

    def test_S3パイプライン初期化失敗時はエラーコードを返す(self, tmp_path):
        """S3パイプラインの初期化に失敗した場合はエラーコードを返すことをテスト"""
        # pending filesファイルを作成（S3 URIを含む）
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("s3://test-bucket/input/video1.mkv\n")

        workspace_base = tmp_path / "workspace"
        workspace_base.mkdir()

        with patch('av1_encoder.s3.batch_orchestrator.S3Pipeline') as mock_pipeline_class:
            mock_pipeline_class.side_effect = Exception("初期化エラー")

            result = run_batch_encoding(
                pending_files_path=pending_files_path,
                output_dir='s3://test-bucket/output/',
                workspace_base=workspace_base,
                parallel=8,
                gop_size=240,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # エラーコードを返すことを確認
            assert result == 1

    def test_ファイルリストが見つからない場合はエラーコードを返す(self, tmp_path):
        """ファイルリストが見つからない場合はエラーコードを返すことをテスト"""
        # 存在しないpending filesパスを使用
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

        # エラーコードを返すことを確認
        assert result == 1

    def test_次のS3ファイルのダウンロードをバックグラウンドで開始(self, mock_s3_pipeline, tmp_path):
        """次のS3ファイルのダウンロードをバックグラウンドで開始することをテスト"""
        # pending filesファイルを作成（S3 URIを含む）
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

                # 2回ダウンロードが開始されたことを確認（2番目と3番目のファイル）
                assert mock_s3_pipeline.download_file_async.call_count == 2

                assert result == 0
