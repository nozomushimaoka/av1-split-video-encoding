"""S3バッチエンコード処理のテスト"""

from pathlib import Path
from unittest.mock import Mock, patch, call
import pytest

from av1_encoder.s3.batch import (
    merge_video_with_audio,
    encode_video,
    process_single_file,
    run_batch_encoding
)


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

        with patch('av1_encoder.s3.batch.subprocess.run') as mock_run:
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

        with patch('av1_encoder.s3.batch.subprocess.run') as mock_run:
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

        with patch('av1_encoder.s3.batch.subprocess.run') as mock_run:
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

        with patch('av1_encoder.s3.batch.subprocess.run') as mock_run:
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

    def test_単一ファイルの処理_ダウンロードなし(self, mock_s3_pipeline, tmp_path, monkeypatch):
        """既に存在するファイルを処理することをテスト"""
        # 実際のファイルを作成
        input_file = tmp_path / "test.mkv"
        input_file.touch()

        # カレントディレクトリをtmp_pathに変更
        monkeypatch.chdir(tmp_path)

        # ワークスペースを作成して一時ファイルを追加
        workspace = None

        def mock_encode_impl(input_f, ws, parallel, gop_size, svtav1_args, ffmpeg_args=None, audio_args=None):
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

        with patch('av1_encoder.s3.batch.encode_video', side_effect=mock_encode_impl) as mock_encode, \
             patch('av1_encoder.s3.batch.merge_video_with_audio', side_effect=mock_merge_impl) as mock_merge:

            mock_future = Mock()
            mock_s3_pipeline.upload_file_async.return_value = mock_future

            result = process_single_file(
                mock_s3_pipeline,
                'test.mkv',
                'test',
                gop_size=240,
                parallel=8,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # エンコードと結合が呼ばれたことを確認
            assert mock_encode.call_count == 1
            assert mock_merge.call_count == 1

            # アップロードが呼ばれたことを確認
            assert mock_s3_pipeline.upload_file_async.call_count == 1

            # アップロード引数を確認（拡張子付きでアップロードされる）
            upload_call_args = mock_s3_pipeline.upload_file_async.call_args[0]
            assert upload_call_args[1] == 'test.mkv'  # base_name + .mkv

            # 戻り値はNone（変更後）
            assert result is None

            # アップロード完了待機が呼ばれたことを確認
            mock_future.result.assert_called_once()

            # 入力ファイルが削除されたことを確認
            assert not input_file.exists()

            # セグメントファイルが削除され、concat.txt, ログファイルが残っていることを確認
            assert workspace is not None
            assert workspace.exists()
            output_file = workspace / "output.mkv"
            # セグメントファイルが削除されていることを確認
            remaining_files = list(workspace.iterdir())
            remaining_names = {f.name for f in remaining_files}
            # output.mkvはアップロード後に削除される（変更後）
            assert "output.mkv" not in remaining_names
            assert "concat.txt" in remaining_names
            assert "main.log" in remaining_names
            # セグメントファイルが削除されていることを確認
            assert not any(f.name.startswith("segment_") for f in remaining_files)

    def test_単一ファイルの処理_前のダウンロードを待機(self, mock_s3_pipeline, tmp_path, monkeypatch):
        """前のダウンロードを待機することをテスト"""
        input_file = tmp_path / "test.mkv"
        input_file.touch()

        # カレントディレクトリをtmp_pathに変更
        monkeypatch.chdir(tmp_path)

        download_future = Mock()

        with patch('av1_encoder.s3.batch.encode_video'), \
             patch('av1_encoder.s3.batch.merge_video_with_audio'):

            mock_upload_future = Mock()
            mock_s3_pipeline.upload_file_async.return_value = mock_upload_future

            process_single_file(
                mock_s3_pipeline,
                'test.mkv',
                'test',
                gop_size=240,
                parallel=8,
                svtav1_args=['--crf', '30', '--preset', '5'],
                download_future=download_future
            )

            # 前のダウンロードのresultが呼ばれたことを確認
            download_future.result.assert_called_once()

    def test_単一ファイルの処理_ファイルが存在しない場合はダウンロード(self, mock_s3_pipeline, tmp_path, monkeypatch):
        """ファイルが存在しない場合はダウンロードすることをテスト"""
        # ファイルは作成しない（存在しない状態）

        # カレントディレクトリをtmp_pathに変更
        monkeypatch.chdir(tmp_path)

        # download_fileが呼ばれたときにファイルを作成するようにモック
        def mock_download(remote, local):
            local.touch()

        mock_s3_pipeline.download_file.side_effect = mock_download

        with patch('av1_encoder.s3.batch.encode_video'), \
             patch('av1_encoder.s3.batch.merge_video_with_audio'):

            mock_upload_future = Mock()
            mock_s3_pipeline.upload_file_async.return_value = mock_upload_future

            process_single_file(
                mock_s3_pipeline,
                'test.mkv',
                'test',
                gop_size=240,
                parallel=8,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # ダウンロードが呼ばれたことを確認
            mock_s3_pipeline.download_file.assert_called_once()
            call_args = mock_s3_pipeline.download_file.call_args[0]
            assert call_args[0] == 'test.mkv'
            assert call_args[1] == Path('test.mkv')

    def test_単一ファイルの処理_エラー時に例外を発生(self, mock_s3_pipeline, tmp_path, monkeypatch):
        """処理中にエラーが発生した場合に例外を発生することをテスト"""
        input_file = tmp_path / "test.mkv"
        input_file.touch()

        # カレントディレクトリをtmp_pathに変更
        monkeypatch.chdir(tmp_path)

        with patch('av1_encoder.s3.batch.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("エンコードエラー")

            with pytest.raises(RuntimeError, match="エンコードエラー"):
                process_single_file(
                    mock_s3_pipeline,
                    'test.mkv',
                    'test',
                    gop_size=240,
                parallel=8,
                    svtav1_args=['--crf', '30', '--preset', '5']
                )

    def test_エラー時に入力ファイルを削除(self, mock_s3_pipeline, tmp_path, monkeypatch):
        """エラー発生時に入力ファイルが削除されることをテスト"""
        input_file = tmp_path / "test.mkv"
        input_file.touch()

        # カレントディレクトリをtmp_pathに変更
        monkeypatch.chdir(tmp_path)

        with patch('av1_encoder.s3.batch.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("エンコードエラー")

            # ファイルが存在することを確認
            assert input_file.exists()

            with pytest.raises(RuntimeError, match="エンコードエラー"):
                process_single_file(
                    mock_s3_pipeline,
                    'test.mkv',
                    'test',
                    gop_size=240,
                    parallel=8,
                    svtav1_args=['--crf', '30', '--preset', '5']
                )

            # エラー後に入力ファイルが削除されたことを確認
            assert not input_file.exists()

    def test_エラー時の入力ファイル削除が失敗してもワーニング(self, mock_s3_pipeline, tmp_path, monkeypatch, caplog):
        """エラー時の入力ファイル削除が失敗してもワーニングログを出すことをテスト"""
        import logging
        from pathlib import Path
        caplog.set_level(logging.WARNING)

        input_file = tmp_path / "test.mkv"
        input_file.touch()

        # カレントディレクトリをtmp_pathに変更
        monkeypatch.chdir(tmp_path)

        with patch('av1_encoder.s3.batch.encode_video') as mock_encode:
            mock_encode.side_effect = RuntimeError("エンコードエラー")

            # Path.unlinkをパッチして失敗させる
            original_unlink = Path.unlink

            def failing_unlink(self):
                if str(self).endswith('test.mkv'):
                    raise PermissionError("削除権限がありません")
                else:
                    return original_unlink(self)

            with patch('pathlib.Path.unlink', new=failing_unlink):
                with pytest.raises(RuntimeError, match="エンコードエラー"):
                    process_single_file(
                        mock_s3_pipeline,
                        'test.mkv',
                        'test',
                        gop_size=240,
                        parallel=8,
                        svtav1_args=['--crf', '30', '--preset', '5']
                    )

                # ワーニングログが出力されたことを確認
                warning_logs = [r for r in caplog.records if r.levelname == 'WARNING']
                assert len(warning_logs) > 0
                assert any("削除に失敗" in r.message for r in warning_logs)


class TestRunBatchEncoding:
    """run_batch_encoding関数のテスト"""

    def test_バッチエンコード処理を実行(self, mock_s3_pipeline, tmp_path):
        """バッチエンコード処理を正常に実行することをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\nvideo2.mkv\n")

        with patch('av1_encoder.s3.batch.S3Pipeline', return_value=mock_s3_pipeline) as mock_pipeline_class:
            with patch('av1_encoder.s3.batch.process_single_file') as mock_process:
                # process_single_fileはNoneを返す（変更後）
                mock_process.return_value = None

                result = run_batch_encoding(
                    bucket='test-bucket',
                    pending_files_path=pending_files_path,
                    gop_size=240,
                    parallel=8,
                    svtav1_args=['--crf', '30', '--preset', '5']
                )

                # S3Pipelineが初期化されたことを確認
                mock_pipeline_class.assert_called_once_with('test-bucket')

                # 各ファイルが処理されたことを確認
                assert mock_process.call_count == 2

                # shutdownが呼ばれたことを確認
                mock_s3_pipeline.shutdown.assert_called_once()

                # 成功コードを返すことを確認
                assert result == 0

    def test_処理対象ファイルがない場合は終了(self, mock_s3_pipeline, tmp_path):
        """処理対象ファイルがない場合は終了することをテスト"""
        # 空のpending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("")

        with patch('av1_encoder.s3.batch.S3Pipeline', return_value=mock_s3_pipeline):
            result = run_batch_encoding(
                bucket='test-bucket',
                pending_files_path=pending_files_path,
                gop_size=240,
                parallel=8,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # shutdownが呼ばれたことを確認
            mock_s3_pipeline.shutdown.assert_called_once()

            # 成功コードを返すことを確認
            assert result == 0

    def test_S3パイプライン初期化失敗時はエラーコードを返す(self, tmp_path):
        """S3パイプラインの初期化に失敗した場合はエラーコードを返すことをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\n")

        with patch('av1_encoder.s3.batch.S3Pipeline') as mock_pipeline_class:
            mock_pipeline_class.side_effect = Exception("初期化エラー")

            result = run_batch_encoding(
                bucket='test-bucket',
                pending_files_path=pending_files_path,
                gop_size=240,
                parallel=8,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # エラーコードを返すことを確認
            assert result == 1

    def test_処理中にエラーが発生した場合はエラーコードを返す(self, mock_s3_pipeline, tmp_path):
        """処理中にエラーが発生した場合はエラーコードを返すことをテスト"""
        # 存在しないpending filesパスを使用してエラーを発生させる
        pending_files_path = tmp_path / "nonexistent.txt"

        with patch('av1_encoder.s3.batch.S3Pipeline', return_value=mock_s3_pipeline):
            result = run_batch_encoding(
                bucket='test-bucket',
                pending_files_path=pending_files_path,
                gop_size=240,
                parallel=8,
                svtav1_args=['--crf', '30', '--preset', '5']
            )

            # shutdownが呼ばれたことを確認
            mock_s3_pipeline.shutdown.assert_called_once()

            # エラーコードを返すことを確認
            assert result == 1

    def test_次のファイルのダウンロードをバックグラウンドで開始(self, mock_s3_pipeline, tmp_path, monkeypatch):
        """次のファイルのダウンロードをバックグラウンドで開始することをテスト"""
        # pending filesファイルを作成
        pending_files_path = tmp_path / "pending.txt"
        pending_files_path.write_text("video1.mkv\nvideo2.mkv\nvideo3.mkv\n")

        # カレントディレクトリをtmp_pathに変更
        monkeypatch.chdir(tmp_path)

        # 実際のファイルを作成（process_single_fileが実際に動作するように）
        (tmp_path / "video1.mkv").touch()
        (tmp_path / "video2.mkv").touch()
        (tmp_path / "video3.mkv").touch()

        with patch('av1_encoder.s3.batch.S3Pipeline', return_value=mock_s3_pipeline):
            mock_download_future1 = Mock()
            mock_download_future2 = Mock()
            mock_s3_pipeline.download_file_async.side_effect = [mock_download_future1, mock_download_future2]

            mock_upload_future = Mock()
            mock_s3_pipeline.upload_file_async.return_value = mock_upload_future

            with patch('av1_encoder.s3.batch.encode_video'), \
                 patch('av1_encoder.s3.batch.merge_video_with_audio'):

                result = run_batch_encoding(
                    bucket='test-bucket',
                    pending_files_path=pending_files_path,
                    gop_size=240,
                    parallel=8,
                    svtav1_args=['--crf', '30', '--preset', '5']
                )

                # 2回ダウンロードが開始されたことを確認（2番目と3番目のファイル）
                assert mock_s3_pipeline.download_file_async.call_count == 2

                # 最後のダウンロードを待機したことを確認
                # video2.mkvの処理時にdownload_future1（video3.mkvのダウンロード）を待つ
                # video3.mkvの処理時にdownload_future2は次がないのでNone
                mock_download_future2.result.assert_called_once()

                # アップロードは各ファイルで3回呼ばれる（変更後: process_single_file内でresult()を呼ぶ）
                assert mock_upload_future.result.call_count == 3

                assert result == 0
