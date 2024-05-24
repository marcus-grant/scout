import unittest.mock as mock
import os
import pytest
from pathlib import PurePath as PP

from scoutlib.model.file import File


@pytest.mark.parametrize(
    "path, dir_path, name, expect",
    [
        (None, None, None, False),  # Raises
        (None, None, "c", False),  # Raises
        (None, PP("a/b"), None, False),  # Raises
        (None, "a/b", "c", True),  # Passes
        (PP("a/b/c"), None, None, True),  # Passes
        ("a/b/c", None, "c", True),  # Passes
        (PP("a/b/c"), PP("a/b"), None, True),  # Passes
        ("a/b/c", "a/b", "c", True),  # Passes
    ],
    ids=["#0", "#1", "#2", "#3", "#4", "#5", "#6", "#7"],
)
class TestInitValidation:
    """
    Test class that groups tests for File.__init__ & its helper .validate_init_args.
    There's three cases for the args path, dir_path, name will raise a TypeError.
        - path is None, dir_path is None, name is None
        - path is None, dir_path is None, but name is given.
        - path is None, name is None, but dir_path is given.
    That is because there's not enough info to determine the path of a file.
    Parametrized above are the 8 permutations of these three arguments.
    They're split into the 3 cases that raise and the 5 that pass.
    """

    def test_helper(self, path, dir_path, name, expect):
        """File.validate_init_args returns the expected result in 'pass'."""
        fn = File._validate_init_args
        assert fn(path=path, dir_path=dir_path, name=name) == expect

    def test_raises(self, path, dir_path, name, expect):
        """File.__init__ raises TypeError if the test case is expected to raise."""
        if not expect:
            with pytest.raises(TypeError):
                File(path=path, dir_path=dir_path, name=name)
        else:
            try:
                File(path=path, dir_path=dir_path, name=name)
            except TypeError:
                pytest.fail("File.__init__ raised TypeError unexpectedly.")


class TestInitAttrs:
    """
    Tests proving that the returned File object has the expected attributes.
    """

    def test_dir_path_is_path(self):
        """Test that File.__init__ creates paths for parent attribute."""
        # Relative path as string
        assert File(path="a/b/c").dir_path == PP("a/b")
        # Relative path as PurePath
        assert File(path=PP("a/b/c")).dir_path == PP("a/b")
        # Absolute path as string
        assert File(path="/a/b/c").dir_path == PP("/a/b")
        # Absolute path as PurePath
        assert File(path=PP("/a/b/c")).dir_path == PP("/a/b")

    def test_name_split_from_path(self):
        """Test that File.__init__ creates name from path correctly as str."""
        expect = "c"
        # Relative path as string
        assert File(path="a/b/c").name == expect
        # Relative path as PurePath
        assert File(path=PP("a/b/c")).name == expect
        # Absolute path as string
        assert File(path="/a/b/c").name == expect
        # Absolute path as PurePath
        assert File(path=PP("/a/b/c")).name == expect

    def test_parent_split_from_path(self):
        """Test that File.__init__ creates parent from path correctly as PP."""
        expect_rel = PP("a/b")
        expect_abs = PP("/a/b")
        # Relative path as string
        assert File(path="a/b/c").dir_path == expect_rel
        # Relative path as PurePath
        assert File(path=PP("a/b/c")).dir_path == expect_rel
        # Absolute path as string
        assert File(path="/a/b/c").dir_path == expect_abs
        # Absolute path as PurePath
        assert File(path=PP("/a/b/c")).dir_path == expect_abs

    @pytest.mark.parametrize(
        "path, dir_path, name",
        [
            (None, "a/b", "c"),  # No path - join parent & name
            (PP("a/b/c"), "a", "b"),  # PP type path, use path
            ("a/b/c", "a", "b"),  # Str type path, use path
            ("a/b/c", "a", None),  # Path & name, use path
            ("a/b/c", None, "b"),  # Path & parent, use path
        ],
        ids=["#0", "#1", "#2", "#3", "#4"],
    )
    def test_path_priority(self, path, dir_path, name):
        """File.__init__ prioritizes path over
        parent and name when assigning parent and name.
        Set expected parent to PurePath("a/b") and name to "c" and
        vary the path, parent, and name arguments to test the priority.
        """
        file = File(path=path, dir_path=dir_path, name=name)
        assert file.dir_path == PP("a/b")
        assert file.name == "c"

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
