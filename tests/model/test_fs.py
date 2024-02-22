import unittest.mock as mock
import os
import pytest

from scoutlib.model.fs import Directory, File


@pytest.fixture
def mock_directory():
    with mock.patch("scoutlib.model.fs.Directory", autospec=True) as mock_dir:
        # Setup mock to return a Directory instance w predefined attrs
        mock_dir.return_value = mock_dir
        yield mock_dir


@pytest.fixture
def mock_directory_from_path():
    with mock.patch("scoutlib.model.fs.Directory.from_path", autospec=True) as mock_dir:
        # Setup mock to return a Directory instance w predefined attrs
        mock_dir.return_value = mock_dir
        yield mock_dir


# Test section for standard constructor for Directory
def test_directory_init():
    # Arrange
    name = "test"
    path = "/test/a"
    # Act
    dir = Directory(name=name, path=path)
    # Assert
    assert dir.name == name
    assert dir.path == path


# Test section for Directory.from_path
def test_directory_from_path():
    # Arrange
    path = "/test/a"
    # Act
    dir = Directory.from_path(path)
    # Assert
    assert dir.name == "a"
    assert dir.path == path


# Test section for standard constructor for File
@pytest.fixture
def mock_file():
    with mock.patch("scoutlib.model.fs.File", autospec=True) as mock_file:
        # Setup mock to return a File instance w predefined attrs
        mock_file.return_value = mock_file
        yield mock_file


@pytest.mark.parametrize(
    "dirpath, filename, id",
    [
        ("/", "test", None),
        ("/test", "basic", 42),
        ("/test/basic", "small", 69),
        ("/test/random", "test", (2**31) - 1),
    ],
)
def test_file_init(mock_directory, dirpath, filename, id):
    # Arrange
    parent = mock_directory.from_path(dirpath)
    # Act
    file = File(name=filename, parent=parent, id=id)
    # Assert
    assert file.name == filename
    assert file.parent == parent
    assert file.id == id
