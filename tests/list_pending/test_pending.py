"""未処理ファイル一覧取得機能のテスト"""

from pathlib import Path
from unittest.mock import Mock
import pytest

from av1_encoder.list_pending.pending import (
    list_s3_objects,
    list_local_files,
    calculate_pending_files
)


@pytest.fixture
def mock_s3_client():
    """S3クライアントのモックを作成"""
    client = Mock()
    return client


class Testのlist_s3_objects:
    """list_s3_objects関数のテスト"""

    def test_オブジェクト一覧を取得(self, mock_s3_client):
        """S3からオブジェクト一覧を取得することをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/video1.mkv'},
                {'Key': 'input/video2.mkv'},
                {'Key': 'input/video3.mkv'},
            ],
            'IsTruncated': False
        }

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        assert len(files) == 3
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        assert 'video3.mkv' in files

        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='input/'
        )

    def test_相対パス全体を保持(self, mock_s3_client):
        """prefixを除いた相対パス全体を保持することをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/subdir/video1.mkv'},
                {'Key': 'input/another/subdir/video2.mkv'},
            ],
            'IsTruncated': False
        }

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        assert len(files) == 2
        # 相対パス全体が含まれる
        assert 'subdir/video1.mkv' in files
        assert 'another/subdir/video2.mkv' in files
        # ファイル名のみは含まれない
        assert 'video1.mkv' not in files
        assert 'video2.mkv' not in files

    def test_オブジェクトがない場合は空セットを返す(self, mock_s3_client):
        """オブジェクトがない場合は空セットを返すことをテスト"""
        mock_s3_client.list_objects_v2.return_value = {}

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        assert files == set()

    def test_Contentsがない場合は空セットを返す(self, mock_s3_client):
        """Contentsキーがない場合は空セットを返すことをテスト"""
        mock_s3_client.list_objects_v2.return_value = {'IsTruncated': False}

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        assert files == set()

    def test_同名ファイルが異なるフォルダにある場合(self, mock_s3_client):
        """同じファイル名が異なるフォルダにある場合、別ファイルとして扱うことをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/dir1/video.mkv'},
                {'Key': 'input/dir2/video.mkv'},
            ],
            'IsTruncated': False
        }

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        # 相対パスが異なるので2つのファイルとして扱われる
        assert len(files) == 2
        assert 'dir1/video.mkv' in files
        assert 'dir2/video.mkv' in files

    def test_ページネーション対応(self, mock_s3_client):
        """複数ページの結果を正しく取得することをテスト"""
        # 1ページ目（続きあり）
        # 2ページ目（続きなし）
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                ],
                'IsTruncated': True,
                'NextContinuationToken': 'token123'
            },
            {
                'Contents': [
                    {'Key': 'input/video3.mkv'},
                    {'Key': 'input/video4.mkv'},
                ],
                'IsTruncated': False
            }
        ]

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        # すべてのページからファイルを取得
        assert len(files) == 4
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        assert 'video3.mkv' in files
        assert 'video4.mkv' in files

        # 2回呼ばれることを確認
        assert mock_s3_client.list_objects_v2.call_count == 2
        # 2回目の呼び出しでContinuationTokenが渡されることを確認
        second_call_kwargs = mock_s3_client.list_objects_v2.call_args_list[1][1]
        assert second_call_kwargs['ContinuationToken'] == 'token123'

    def test_ディレクトリエントリを除外(self, mock_s3_client):
        """末尾が/のディレクトリエントリを除外することをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/'},  # ディレクトリエントリ
                {'Key': 'input/subdir/'},  # ディレクトリエントリ
                {'Key': 'input/video1.mkv'},
                {'Key': 'input/subdir/video2.mkv'},
            ],
            'IsTruncated': False
        }

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        # ディレクトリエントリは含まれない
        assert len(files) == 2
        assert 'video1.mkv' in files
        assert 'subdir/video2.mkv' in files
        # ディレクトリエントリは含まれない
        assert '' not in files
        assert 'subdir/' not in files


class Testのlist_local_files:
    """list_local_files関数のテスト"""

    def test_ファイル一覧を取得(self, tmp_path):
        """ローカルディレクトリからファイル一覧を取得することをテスト"""
        # テスト用ファイルを作成
        (tmp_path / "video1.mkv").touch()
        (tmp_path / "video2.mkv").touch()
        (tmp_path / "video3.mkv").touch()

        files = list_local_files(tmp_path)

        assert len(files) == 3
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        assert 'video3.mkv' in files

    def test_サブディレクトリ内のファイルも取得(self, tmp_path):
        """サブディレクトリ内のファイルも取得することをテスト"""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "video1.mkv").touch()
        another = tmp_path / "another" / "subdir"
        another.mkdir(parents=True)
        (another / "video2.mkv").touch()

        files = list_local_files(tmp_path)

        assert len(files) == 2
        assert 'subdir/video1.mkv' in files
        assert 'another/subdir/video2.mkv' in files

    def test_ディレクトリが存在しない場合は空セット(self, tmp_path):
        """存在しないディレクトリの場合は空セットを返すことをテスト"""
        nonexistent = tmp_path / "nonexistent"

        files = list_local_files(nonexistent)

        assert files == set()

    def test_空のディレクトリは空セット(self, tmp_path):
        """空のディレクトリの場合は空セットを返すことをテスト"""
        files = list_local_files(tmp_path)

        assert files == set()


class Testのcalculate_pending_files:
    """calculate_pending_files関数のテスト"""

    def test_S3の未処理ファイルを計算(self, mock_s3_client):
        """S3の未処理ファイルを正しく計算することをテスト"""
        # 1回目の呼び出し(input/)
        # 2回目の呼び出し(output/)
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                    {'Key': 'input/video3.mkv'},
                    {'Key': 'input/video4.mkv'},
                ],
                'IsTruncated': False
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},
                    {'Key': 'output/video2.mkv'},
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        assert len(pending) == 2
        # 絶対パス（S3 URI）で返される
        assert 's3://test-bucket/input/video3.mkv' in pending
        assert 's3://test-bucket/input/video4.mkv' in pending
        # ソートされていることを確認
        assert pending == sorted(pending)

        # list_objects_v2が2回呼ばれることを確認
        assert mock_s3_client.list_objects_v2.call_count == 2

    def test_すべて処理済みの場合は空リスト(self, mock_s3_client):
        """すべて処理済みの場合は空リストを返すことをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                ],
                'IsTruncated': False
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},
                    {'Key': 'output/video2.mkv'},
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        assert pending == []

    def test_出力ファイルがない場合は全て未処理(self, mock_s3_client):
        """出力ファイルがない場合は全て未処理として返すことをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                ],
                'IsTruncated': False
            },
            {}  # 出力ファイルなし
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        assert len(pending) == 2
        assert 's3://test-bucket/input/video1.mkv' in pending
        assert 's3://test-bucket/input/video2.mkv' in pending

    def test_mkv拡張子を正しく処理(self, mock_s3_client):
        """.mkv拡張子を正しく処理することをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                ],
                'IsTruncated': False
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},  # 拡張子付き
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # video1はベース名で比較されるため処理済みと判定される
        assert len(pending) == 1
        assert 's3://test-bucket/input/video2.mkv' in pending

    def test_結果がソートされる(self, mock_s3_client):
        """結果がアルファベット順にソートされることをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/zebra.mkv'},
                    {'Key': 'input/apple.mkv'},
                    {'Key': 'input/moon.mkv'},
                ],
                'IsTruncated': False
            },
            {}
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        assert pending == [
            's3://test-bucket/input/apple.mkv',
            's3://test-bucket/input/moon.mkv',
            's3://test-bucket/input/zebra.mkv'
        ]

    def test_入力ファイルがない場合(self, mock_s3_client):
        """入力ファイルがない場合のテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {},  # 入力ファイルなし
            {}
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # 入力ファイルがない場合は空リストを返す
        assert pending == []

    def test_ベース名で比較する(self, mock_s3_client):
        """ベース名(.mkv除去後)で比較することをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/movie_name.mkv'},
                ],
                'IsTruncated': False
            },
            {
                'Contents': [
                    {'Key': 'output/movie_name.mkv'},
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # ベース名が同じなので処理済みと判定される
        assert pending == []

    def test_出力ファイルに余分なファイルがある(self, mock_s3_client):
        """出力に入力にないファイルがある場合をテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                ],
                'IsTruncated': False
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},
                    {'Key': 'output/video2.mkv'},  # 入力にない
                    {'Key': 'output/extra.mkv'},   # 入力にない
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # video1は処理済み、余分な出力ファイルは無視される
        assert pending == []

    def test_サブフォルダ内の同名ファイルを別ファイルとして扱う(self, mock_s3_client):
        """サブフォルダ内の同名ファイルを別ファイルとして正しく扱うことをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/folder1/video.mkv'},
                    {'Key': 'input/folder2/video.mkv'},
                    {'Key': 'input/folder3/video.mkv'},
                ],
                'IsTruncated': False
            },
            {
                'Contents': [
                    {'Key': 'output/folder1/video.mkv'},  # 処理済み
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # folder1/video.mkvは処理済み、残り2つは未処理
        assert len(pending) == 2
        assert 's3://test-bucket/input/folder2/video.mkv' in pending
        assert 's3://test-bucket/input/folder3/video.mkv' in pending
        # folder1/video.mkvは含まれない
        assert 's3://test-bucket/input/folder1/video.mkv' not in pending

    def test_深いサブフォルダ構造(self, mock_s3_client):
        """深いサブフォルダ構造を正しく処理することをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/a/b/c/video1.mkv'},
                    {'Key': 'input/x/y/z/video2.mkv'},
                ],
                'IsTruncated': False
            },
            {
                'Contents': [
                    {'Key': 'output/a/b/c/video1.mkv'},
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # a/b/c/video1.mkvは処理済み、x/y/z/video2.mkvは未処理
        assert len(pending) == 1
        assert 's3://test-bucket/input/x/y/z/video2.mkv' in pending

    def test_ローカルファイルの未処理ファイルを計算(self, tmp_path):
        """ローカルファイルの未処理ファイルを計算することをテスト"""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # 入力ファイルを作成
        (input_dir / "video1.mkv").touch()
        (input_dir / "video2.mkv").touch()
        (input_dir / "video3.mkv").touch()

        # 出力ファイルを作成（一部処理済み）
        (output_dir / "video1.mkv").touch()

        pending = calculate_pending_files(str(input_dir), str(output_dir))

        assert len(pending) == 2
        # 絶対パスで返される
        assert str(input_dir.resolve() / "video2.mkv") in pending
        assert str(input_dir.resolve() / "video3.mkv") in pending

    def test_ローカルサブフォルダ構造(self, tmp_path):
        """ローカルファイルのサブフォルダ構造を正しく処理することをテスト"""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # サブフォルダを含む入力ファイルを作成
        (input_dir / "folder1").mkdir()
        (input_dir / "folder1" / "video.mkv").touch()
        (input_dir / "folder2").mkdir()
        (input_dir / "folder2" / "video.mkv").touch()

        # folder1は処理済み
        (output_dir / "folder1").mkdir()
        (output_dir / "folder1" / "video.mkv").touch()

        pending = calculate_pending_files(str(input_dir), str(output_dir))

        assert len(pending) == 1
        assert str(input_dir.resolve() / "folder2" / "video.mkv") in pending
