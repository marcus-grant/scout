import unittest.mock as mock
import os
import pytest
from pathlib import PurePath as PP

from lib.model.file import File


class TestInitAttrs:
    """
    Tests proving that the returned File object has the expected attributes.
    """

    def testPathIsPath(self):
        """Test that File.__init__ creates PurePath for path attribute."""
        # Relative path as string
        assert File("a/b/c").path == PP("a/b/c")
        # Relative path as PurePath
        assert File(PP("a/b/c")).path == PP("a/b/c")
        # Absolute path as string
        assert File("/a/b/c").path == PP("/a/b/c")
        # Absolute path as PurePath
        assert File(PP("/a/b/c")).path == PP("/a/b/c")

    def test_name_split_from_path(self):
        """Test that File.__init__ creates name from path correctly as str."""
        expect = "c"
        # Relative path as string
        assert File(path="a/b/c").path.name == expect
        # Relative path as PurePath
        assert File(path=PP("a/b/c")).path.name == expect
        # Absolute path as string
        assert File(path="/a/b/c").path.name == expect
        # Absolute path as PurePath
        assert File(path=PP("/a/b/c")).path.name == expect

    def test_parent_split_from_path(self):
        """Test that File.__init__ creates parent from path correctly as PP."""
        expect_rel = PP("a/b")
        expect_abs = PP("/a/b")
        # Relative path as string
        assert File(path="a/b/c").path.parent == expect_rel
        # Relative path as PurePath
        assert File(path=PP("a/b/c")).path.parent == expect_rel
        # Absolute path as string
        assert File(path="/a/b/c").path.parent == expect_abs
        # Absolute path as PurePath
        assert File(path=PP("/a/b/c")).path.parent == expect_abs

    def test_basic_attrs(self):
        """Test that File.__init__ assigns all other attributes correctly."""
        id = 42
        dir_id = 24
        size = 1024
        md5 = mock.MagicMock()
        mtime = mock.MagicMock()
        updated = mock.MagicMock()
        file = File(
            path="a/b/c",
            id=id,
            dir_id=dir_id,
            size=size,
            md5=md5,
            mtime=mtime,
            updated=updated,
        )
        assert file.id == id
        assert file.dir_id == dir_id
        assert file.size == size
        assert file.md5 == md5
        assert file.mtime == mtime
        assert file.updated == updated


@pytest.mark.parametrize(
    "path_arg, path_expect",
    [
        ("a/b/c", PP("a/b/c")),  # Relative path str
        ("/a/b/c", PP("/a/b/c")),  # Absolute path str
        ("/a", PP("/a")),  # Root file
    ],
    ids=["Absolute", "Relative", "Root"],
)
class TestPathProperty:
    """
    Tests for the File.path property which join parent and name.
    It is known from previous tests that File(path=) correctly assigns to
    File.dir_path and File.name.
    This test checks that File.path returns so no need to try different arg types.
    """

    def test_is_path_type(self, path_arg, path_expect):  # noqa
        """Test that File.path is a PurePath."""
        msg = "File.path is not a PurePath type."
        assert isinstance(File(path=path_arg).path, PP), msg

    def test_path_join(self, path_arg, path_expect):
        """Test that File.path joins parent and name correctly."""
        assert File(path=path_arg).path == path_expect
