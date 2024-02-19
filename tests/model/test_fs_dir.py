from datetime import datetime
import os
import pathlib
from pyfakefs.fake_filesystem_unittest import Patcher
import pytest
import random

from scoutlib.model.fs_dir import FsDir


@pytest.fixture(autouse=True)
def setup_fakefs():
    with Patcher() as patcher:
        # Create the fake FS directory paths
        dir_paths = [
            "/testroot/basic/small",  # Basic <1MiB test files
            "/testroot/basic/big",  # Basic >= 1MiB test files
            "/testroot/random",  # Random generated files with known seed
            "/testroot/random/small/prime",  # <1MiB prime byte-count
            "/testroot/random/big/prime",  # >=1MiB prime byte-count
            "/testroot/special",  # (encoding, hidden, unsafe chars, etc)
            "/testroot/special/utf8/name",  # UTF-8 high-codepoint file names
            "/testroot/special/utf8/content",  # UTF-8 high-codepoint contents
        ]
        file_paths = [
            "/testroot/basic/small/empty.txt",
            "/testroot/basic/small/hello.txt",
        ]
        file_contents = [
            "",
            "Hello, World!\n",
        ]

        # Automatically create a common directory structure or files needed by all tests
        for path in dir_paths:
            patcher.fs.create_dir(path)  # type: ignore pyright cant find create_dir method
        for path, contents in zip(file_paths, file_contents):
            patcher.fs.create_file(path, contents=contents)  # type: ignore
        yield patcher  # Yielding patcher for tests that might want to use it directly


#
#
# @pytest.fixture
# def create_dir(setup_fs):
#     """Fixture to create and return an FsDir object from a given path."""
#
#     def _create_fs_dir(path: str):
#         # Ensure the directory exists in the fake filesystem
#         setup_fs.fs.create_dir(path)
#         # Instantiate and return an FsDir object
#         return FsDir.from_path(path)
#
#     return _create_fs_dir


def test_mock_dirs():
    assert pathlib.Path("/testroot/basic/small").exists()
    assert pathlib.Path("/testroot/basic/big").exists()
    assert pathlib.Path("/testroot/random/small/prime").exists()
    assert pathlib.Path("/testroot/random/big/prime").exists()
    assert pathlib.Path("/testroot/special/utf8/name").exists()
    assert pathlib.Path("/testroot/special/utf8/content").exists()


def test_mock_files():
    # Test existence
    assert pathlib.Path("/testroot/basic/small/empty.txt").exists()
    assert pathlib.Path("/testroot/basic/small/hello.txt").exists()
    # Test content
    assert pathlib.Path("/testroot/basic/small/empty.txt").read_text() == ""
    assert (
        pathlib.Path("/testroot/basic/small/hello.txt").read_text() == "Hello, World!\n"
    )


@pytest.mark.parametrize(
    "path, dirname",
    [
        ("/testroot", "testroot"),
        ("/testroot/basic", "basic"),
        ("/testroot/basic/small", "small"),
        ("/testroot/random", "random"),
        ("/testroot/special", "special"),
        ("/testroot/special/utf8", "utf8"),
    ],
)
def test_from_path(path, dirname):
    d = FsDir.from_path(path)
    assert d.name == dirname
    assert d.path == path
