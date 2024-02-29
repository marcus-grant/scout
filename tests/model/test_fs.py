# TODO: Refactor tests organized into testclasses for each class
# TODO: Need test cases asserting different factory methods result in same
import unittest.mock as mock
import os
import pytest
from pathlib import Path, PurePath

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
    testpath = "/test/a/b/c"
    dir = Directory(path=testpath)
    assert dir.path == PurePath(testpath), f"Expected '/test/a', got {dir.path}"
    assert dir.id is None, f"Expected None, got {dir.id}"
    assert dir.name == "c", f"Expected 'c', got {dir.name}"

    testpath = "/a/b"
    dir = Directory(path=testpath, id=42)
    assert dir.path == PurePath("/a/b"), f"Expected '/a/b', got {dir.path}"
    assert dir.id == 42, f"Expected 42, got {dir.id}"
    assert dir.name == "b", f"Expected 'b', got {dir.name}"

    # Assert exception when no path is given
    with pytest.raises(TypeError):
        Directory()  # type: ignore


# Test section for Directory.from_path
def test_directory_from_path():
    # Arrange
    testpath = "/test/a"
    # Act
    dir = Directory.from_path(testpath)
    # Assert
    assert dir.name == "a"
    assert dir.path == PurePath(testpath)


def test_directory_eq():
    assert Directory.from_path("/test/a") == Directory.from_path("/test/a")
    assert Directory.from_path("/test/b", id=42) == Directory.from_path(
        "/test/b", id=42
    )
    assert Directory.from_path("/test/c") != Directory.from_path("/test/d")
    assert Directory.from_path("/test/e", id=42) != Directory.from_path("/test/e")


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


# @pytest.mark.parametrize(
#     "path",
#     [("/"), ("/home/user/Documents"), ("relative/path")],
# )
# def test_dirtreenode_mkroot(path):
#     # Arrange
#     rootdir = Directory.from_path(path)
#     # Act
#     root = DirTreeNode.mkroot(rootdir)
#     # Assert
#     assert root.dir.path == path
#     assert root.dir == rootdir
#     assert root.parent is None
#     assert root.subdirs == []
#     assert root.depth == 0
#
#
# def test_dirtreenode_init_defaults():
#     dir = Directory.from_path("/")
#     node_all_defaults = DirTreeNode(dir=dir)
#     assert node_all_defaults.dir == dir
#     assert node_all_defaults.dir.path == "/"
#     assert node_all_defaults.parent is None
#     assert node_all_defaults.subdirs == []
#     assert node_all_defaults.depth == 0
#
#
# # def test_dirtreenode_mkroot
