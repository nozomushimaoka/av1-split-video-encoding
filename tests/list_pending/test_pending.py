"""Tests for the pending file listing feature"""

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
    """Create a mock S3 client"""
    client = Mock()
    return client


class TestListS3Objects:
    """Tests for the list_s3_objects function"""

    def test_get_object_list(self, mock_s3_client):
        """Test getting a list of objects from S3"""
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

    def test_preserve_full_relative_path(self, mock_s3_client):
        """Test that the full relative path (excluding prefix) is preserved"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/subdir/video1.mkv'},
                {'Key': 'input/another/subdir/video2.mkv'},
            ],
            'IsTruncated': False
        }

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        assert len(files) == 2
        # Full relative path is included
        assert 'subdir/video1.mkv' in files
        assert 'another/subdir/video2.mkv' in files
        # Filename only is not included
        assert 'video1.mkv' not in files
        assert 'video2.mkv' not in files

    def test_return_empty_set_when_no_objects(self, mock_s3_client):
        """Test that an empty set is returned when there are no objects"""
        mock_s3_client.list_objects_v2.return_value = {}

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        assert files == set()

    def test_return_empty_set_when_no_contents_key(self, mock_s3_client):
        """Test that an empty set is returned when the Contents key is missing"""
        mock_s3_client.list_objects_v2.return_value = {'IsTruncated': False}

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        assert files == set()

    def test_same_name_files_in_different_folders_are_distinct(self, mock_s3_client):
        """Test that files with the same name in different folders are treated as separate files"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/dir1/video.mkv'},
                {'Key': 'input/dir2/video.mkv'},
            ],
            'IsTruncated': False
        }

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        # Different relative paths, so treated as 2 files
        assert len(files) == 2
        assert 'dir1/video.mkv' in files
        assert 'dir2/video.mkv' in files

    def test_pagination_support(self, mock_s3_client):
        """Test that multiple pages of results are retrieved correctly"""
        # Page 1 (has more)
        # Page 2 (no more)
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

        # Retrieve files from all pages
        assert len(files) == 4
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        assert 'video3.mkv' in files
        assert 'video4.mkv' in files

        # Verify called twice
        assert mock_s3_client.list_objects_v2.call_count == 2
        # Verify ContinuationToken was passed in the second call
        second_call_kwargs = mock_s3_client.list_objects_v2.call_args_list[1][1]
        assert second_call_kwargs['ContinuationToken'] == 'token123'

    def test_exclude_directory_entries(self, mock_s3_client):
        """Test that directory entries (ending with /) are excluded"""
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'input/'},  # Directory entry
                {'Key': 'input/subdir/'},  # Directory entry
                {'Key': 'input/video1.mkv'},
                {'Key': 'input/subdir/video2.mkv'},
            ],
            'IsTruncated': False
        }

        files = list_s3_objects(mock_s3_client, 'test-bucket', 'input/')

        # Directory entries should not be included
        assert len(files) == 2
        assert 'video1.mkv' in files
        assert 'subdir/video2.mkv' in files
        # Directory entries should not be included
        assert '' not in files
        assert 'subdir/' not in files


class TestListLocalFiles:
    """Tests for the list_local_files function"""

    def test_get_file_list(self, tmp_path):
        """Test getting a list of files from a local directory"""
        # Create test files
        (tmp_path / "video1.mkv").touch()
        (tmp_path / "video2.mkv").touch()
        (tmp_path / "video3.mkv").touch()

        files = list_local_files(tmp_path)

        assert len(files) == 3
        assert 'video1.mkv' in files
        assert 'video2.mkv' in files
        assert 'video3.mkv' in files

    def test_get_files_in_subdirectories(self, tmp_path):
        """Test that files in subdirectories are also retrieved"""
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

    def test_return_empty_set_for_nonexistent_directory(self, tmp_path):
        """Test that an empty set is returned for a directory that does not exist"""
        nonexistent = tmp_path / "nonexistent"

        files = list_local_files(nonexistent)

        assert files == set()

    def test_empty_directory_returns_empty_set(self, tmp_path):
        """Test that an empty directory returns an empty set"""
        files = list_local_files(tmp_path)

        assert files == set()


class TestCalculatePendingFiles:
    """Tests for the calculate_pending_files function"""

    def test_calculate_s3_pending_files(self, mock_s3_client):
        """Test that S3 pending files are calculated correctly"""
        # First call (input/)
        # Second call (output/)
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
        # Returned as absolute paths (S3 URIs)
        assert 's3://test-bucket/input/video3.mkv' in pending
        assert 's3://test-bucket/input/video4.mkv' in pending
        # Verify sorted
        assert pending == sorted(pending)

        # Verify list_objects_v2 was called twice
        assert mock_s3_client.list_objects_v2.call_count == 2

    def test_return_empty_list_when_all_processed(self, mock_s3_client):
        """Test that an empty list is returned when all files have been processed"""
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

    def test_return_all_as_pending_when_no_output_files(self, mock_s3_client):
        """Test that all files are returned as pending when there are no output files"""
        mock_s3_client.list_objects_v2.side_effect = [
            {
                'Contents': [
                    {'Key': 'input/video1.mkv'},
                    {'Key': 'input/video2.mkv'},
                ],
                'IsTruncated': False
            },
            {}  # No output files
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        assert len(pending) == 2
        assert 's3://test-bucket/input/video1.mkv' in pending
        assert 's3://test-bucket/input/video2.mkv' in pending

    def test_handle_mkv_extension_correctly(self, mock_s3_client):
        """Test that .mkv extensions are handled correctly"""
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
                    {'Key': 'output/video1.mkv'},  # With extension
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # video1 is compared by base name, so it is treated as processed
        assert len(pending) == 1
        assert 's3://test-bucket/input/video2.mkv' in pending

    def test_results_are_sorted(self, mock_s3_client):
        """Test that results are sorted alphabetically"""
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

    def test_no_input_files(self, mock_s3_client):
        """Test when there are no input files"""
        mock_s3_client.list_objects_v2.side_effect = [
            {},  # No input files
            {}
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # Return empty list when there are no input files
        assert pending == []

    def test_compare_by_base_name(self, mock_s3_client):
        """Test that comparison is done by base name (after removing .mkv)"""
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

        # Same base name, so treated as processed
        assert pending == []

    def test_extra_output_files_are_ignored(self, mock_s3_client):
        """Test that extra output files not in input are ignored"""
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
                    {'Key': 'output/video2.mkv'},  # Not in input
                    {'Key': 'output/extra.mkv'},   # Not in input
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # video1 is processed, extra output files are ignored
        assert pending == []

    def test_same_name_files_in_subfolders_are_treated_as_distinct(self, mock_s3_client):
        """Test that files with the same name in subfolders are treated as separate files"""
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
                    {'Key': 'output/folder1/video.mkv'},  # Processed
                ],
                'IsTruncated': False
            }
        ]

        pending = calculate_pending_files(
            's3://test-bucket/input/',
            's3://test-bucket/output/',
            mock_s3_client
        )

        # folder1/video.mkv is processed, the other 2 are pending
        assert len(pending) == 2
        assert 's3://test-bucket/input/folder2/video.mkv' in pending
        assert 's3://test-bucket/input/folder3/video.mkv' in pending
        # folder1/video.mkv should not be included
        assert 's3://test-bucket/input/folder1/video.mkv' not in pending

    def test_deep_subfolder_structure(self, mock_s3_client):
        """Test that deep subfolder structures are handled correctly"""
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

        # a/b/c/video1.mkv is processed, x/y/z/video2.mkv is pending
        assert len(pending) == 1
        assert 's3://test-bucket/input/x/y/z/video2.mkv' in pending

    def test_calculate_local_pending_files(self, tmp_path):
        """Test that local file pending files are calculated correctly"""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create input files
        (input_dir / "video1.mkv").touch()
        (input_dir / "video2.mkv").touch()
        (input_dir / "video3.mkv").touch()

        # Create output files (some already processed)
        (output_dir / "video1.mkv").touch()

        pending = calculate_pending_files(str(input_dir), str(output_dir))

        assert len(pending) == 2
        # Returned as absolute paths
        assert str(input_dir.resolve() / "video2.mkv") in pending
        assert str(input_dir.resolve() / "video3.mkv") in pending

    def test_local_subfolder_structure(self, tmp_path):
        """Test that local file subfolder structures are handled correctly"""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        input_dir.mkdir()
        output_dir.mkdir()

        # Create input files including subfolders
        (input_dir / "folder1").mkdir()
        (input_dir / "folder1" / "video.mkv").touch()
        (input_dir / "folder2").mkdir()
        (input_dir / "folder2" / "video.mkv").touch()

        # folder1 is processed
        (output_dir / "folder1").mkdir()
        (output_dir / "folder1" / "video.mkv").touch()

        pending = calculate_pending_files(str(input_dir), str(output_dir))

        assert len(pending) == 1
        assert str(input_dir.resolve() / "folder2" / "video.mkv") in pending
