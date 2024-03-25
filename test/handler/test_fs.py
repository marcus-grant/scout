import pathlib
from pyfakefs.fake_filesystem_unittest import Patcher
import pytest

from scoutlib.model.dir import Dir
from scoutlib.handler import fs


@pytest.fixture()
def setup_fakefs():
    with Patcher() as patcher:
        # Create the fake FS directory paths
        dir_paths = [
            "/test/basic/small",  # Basic <1MiB test files
            "/test/basic/big",  # Basic >= 1MiB test files
            # "/test/random",  # Random generated files with known seed
            # "/test/random/small/prime",  # <1MiB prime byte-count
            # "/test/random/big/prime",  # >=1MiB prime byte-count
            "/test/special",  # (encoding, hidden, unsafe chars, etc)
            # "/test/special/utf8/name",  # UTF-8 high-codepoint file names
            # "/test/special/utf8/content",  # UTF-8 high-codepoint contents
            "/test/special/nest/real/deep",
        ]
        file_paths = [
            "/test/basic/small/empty.txt",
            "/test/basic/small/hello.txt",
            "/test/special/nest/real/deep/deepfile",
        ]
        file_contents = [
            "",
            "Hello, World!\n",
            "/test/special/nest/real/deep/deepfile",
        ]

        # Automatically create a common directory structure or files needed by all tests
        for path in dir_paths:
            patcher.fs.create_dir(path)  # type: ignore pyright cant find create_dir method
        for path, contents in zip(file_paths, file_contents):
            patcher.fs.create_file(path, contents=contents)  # type: ignore
        yield patcher  # Yielding patcher for tests that might want to use it directly


def test_mock_dirs(setup_fakefs):
    assert pathlib.Path("/test/basic/small").exists()
    assert pathlib.Path("/test/basic/big").exists()
    # assert pathlib.Path("/test/random/small/prime").exists()
    # assert pathlib.Path("/test/random/big/prime").exists()
    # assert pathlib.Path("/test/special/utf8/name").exists()


def test_mock_files(setup_fakefs):
    # Check file exists
    assert pathlib.Path("/test/basic/small/empty.txt").exists()
    assert pathlib.Path("/test/basic/small/hello.txt").exists()
    # Check contains expected content
    assert pathlib.Path("/test/basic/small/empty.txt").read_text() == ""
    assert pathlib.Path("/test/basic/small/hello.txt").read_text() == "Hello, World!\n"


# @pytest.mark.parametrize(
#     "search_path, expected_path, expected_name",
#     [
#         ("/test", "/test", "test"),
#         ("/test", "/test/basic/small", "small"),
#         ("/test", "/test/basic/big", "big"),
#         ("/test", "/test/special", "special"),
#     ],
# )
# def test_find_all_dirs(setup_fakefs, search_path, expected_path, expected_name):
#     dirs = fs.find_all_dirs(search_path)
#     actual_dir = next(
#         (d for d in dirs if d.path == expected_path and d.name == expected_name), None
#     )
#
#     assert (
#         actual_dir is not None
#     ), f"Directory with path {expected_path} and name {expected_name} not found"


def test_find_all_dirs_not_exist(setup_fakefs):
    dirs = fs.find_all_dirs("/test")
    actual_paths = [str(d.path) for d in dirs]
    actual_names = [str(d.name) for d in dirs]
    assert "/test/basic/small" in actual_paths  # control
    assert "/not/actually/there" not in actual_paths
    assert "not-there" not in actual_names


def test_find_all_dirs_path_not_exist(setup_fakefs):
    empty = fs.find_all_dirs("/not/actually/there/should/be/empty")
    assert len(empty) == 0


# def test_find_common_root(setup_fakefs):  #
#     # First assert empty list returns None
#     assert fs.find_common_root([]) is None
#
#     # Ensure a two flat sibling dirs return None
#     dir_a = Dir.from_path("/a")
#     dir_b = Dir.from_path("/b")
#     assert fs.find_common_root([dir_a, dir_b]) is None
#
#     # Check the basic case of /test being the shared deepest root
#     dirs = fs.find_all_dirs("/test")
#     root = fs.find_common_root(dirs)
#     assert root is not None
#     assert root.path == "/test"
#
#     # Check the expected /extra-root/test dir when a /extra-root/test exists
#     # As opposed to the less deep common root of /extra-root
#     for d in dirs:
#         d.path = "/extra-root" / d.path  # Mock new parent to /test, /extra-root
#     root = fs.find_common_root(dirs)
#     assert root is not None
#     assert root.path == "/extra-root/test"


def test_sort_dirs_dfs():
    expected = ["/a", "/a/b", "/a/b/c", "/a/b/c/d", "/a/b/c/e", "/b", "/b/c"]
    expected = [Dir.from_path(p) for p in expected]
    dirs = expected[::-1]
    assert fs.dirs_sorted_dfs(dirs) == expected
