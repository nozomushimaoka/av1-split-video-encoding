"""未処理ファイル一覧取得機能のテスト"""

from unittest.mock import Mock
import pytest

from av1_encoder.list_pending.pending import list_objects, calculate_pending_files


@pytest.fixture
def mock_s3_client():
    """S3クライアントのモックを作成"""
    client = Mock()
    return client


class Testのlist_objects:
    """list_objects関数のテスト"""

    def test_オブジェクト一覧を取得(self, mock_s3_client):
        """S3からオブジェクト一覧を取得することをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/video1.mkv'},
                {'Key': 'input/video2.mkv'},
                {'Key': 'input/video3.mkv'},
            ]
        }

        files = list_objects(mock_s3_client, 'test-bucket', 'input/')

        assert len(files) == 3
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        assert 'video3.mkv' in files

        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket='test-bucket',
            Prefix='input/'
        )

    def test_ファイル名のみを抽出(self, mock_s3_client):
        """パスからファイル名のみを抽出することをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/subdir/video1.mkv'},
                {'Key': 'input/another/subdir/video2.mkv'},
            ]
        }

        files = list_objects(mock_s3_client, 'test-bucket', 'input/')

        assert len(files) == 2
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        # パスは含まれない
        assert 'input/subdir/video1.mkv' not in files

    def test_オブジェクトがない場合は空セットを返す(self, mock_s3_client):
        """オブジェクトがない場合は空セットを返すことをテスト"""
        mock_s3_client.list_objects_v2.return_value = {}

        files = list_objects(mock_s3_client, 'test-bucket', 'input/')

        assert files == set()

    def test_Contentsがない場合は空セットを返す(self, mock_s3_client):
        """Contentsキーがない場合は空セットを返すことをテスト"""
        mock_s3_client.list_objects_v2.return_value = {'IsTruncated': False}

        files = list_objects(mock_s3_client, 'test-bucket', 'input/')

        assert files == set()

    def test_重複ファイル名は一意になる(self, mock_s3_client):
        """同じファイル名が複数ある場合はsetで一意になることをテスト"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/dir1/video.mkv'},
                {'Key': 'input/dir2/video.mkv'},
            ]
        }

        files = list_objects(mock_s3_client, 'test-bucket', 'input/')

        # 同じファイル名なのでsetにより1つになる
        assert len(files) == 1
        assert 'video.mkv' in files


class Testのcalculate_pending_files:
    """calculate_pending_files関数のテスト"""

    def test_未処理ファイルを計算(self, mock_s3_client):
        """未処理ファイルを正しく計算することをテスト"""
        # 1回目の呼び出し(input/)
        # 2回目の呼び出し(output/)
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                    {'Key': 'input/video3.mkv'},
                    {'Key': 'input/video4.mkv'},
                ]
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},
                    {'Key': 'output/video2.mkv'},
                ]
            }
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        assert len(pending) == 2
        assert 'video3.mkv' in pending
        assert 'video4.mkv' in pending
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
                ]
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},
                    {'Key': 'output/video2.mkv'},
                ]
            }
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        assert pending == []

    def test_出力ファイルがない場合は全て未処理(self, mock_s3_client):
        """出力ファイルがない場合は全て未処理として返すことをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                ]
            },
            {}  # 出力ファイルなし
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        assert len(pending) == 2
        assert 'video1.mkv' in pending
        assert 'video2.mkv' in pending

    def test_mkv拡張子を正しく処理(self, mock_s3_client):
        """.mkv拡張子を正しく処理することをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                ]
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},  # 拡張子付き
                ]
            }
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        # video1はベース名で比較されるため処理済みと判定される
        assert len(pending) == 1
        assert 'video2.mkv' in pending

    def test_結果がソートされる(self, mock_s3_client):
        """結果がアルファベット順にソートされることをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/zebra.mkv'},
                    {'Key': 'input/apple.mkv'},
                    {'Key': 'input/moon.mkv'},
                ]
            },
            {}
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        assert pending == ['apple.mkv', 'moon.mkv', 'zebra.mkv']

    def test_入力ファイルがない場合(self, mock_s3_client):
        """入力ファイルがない場合のテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {},  # 入力ファイルなし
            {}
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        # 入力ファイルがない場合は空リストを返す
        assert pending == []

    def test_ベース名で比較する(self, mock_s3_client):
        """ベース名(.mkv除去後)で比較することをテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/movie_name.mkv'},
                ]
            },
            {
                'Contents': [
                    {'Key': 'output/movie_name.mkv'},
                ]
            }
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        # ベース名が同じなので処理済みと判定される
        assert pending == []

    def test_出力ファイルに余分なファイルがある(self, mock_s3_client):
        """出力に入力にないファイルがある場合をテスト"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                ]
            },
            {
                'Contents': [
                    {'Key': 'output/video1.mkv'},
                    {'Key': 'output/video2.mkv'},  # 入力にない
                    {'Key': 'output/extra.mkv'},   # 入力にない
                ]
            }
        ]

        pending = calculate_pending_files(mock_s3_client, 'test-bucket')

        # video1は処理済み、余分な出力ファイルは無視される
        assert pending == []
