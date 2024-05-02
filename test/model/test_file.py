import unittest.mock as mock
import os
import pytest
from pathlib import PurePath as PP

from scoutlib.model.file import File


@pytest.mark.parametrize(
    "path, parent, name, expect",
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
    There's three cases for the args path, parent, name will raise a TypeError.
        - path is None, parent is None, name is None
        - path is None, parent is None, but name is given.
        - path is None, name is None, but parent is given.
    That is because there's not enough info to determine the path of a file.
    Parametrized above are the 8 permutations of these three arguments.
    They're split into the 3 cases that raise and the 5 that pass.
    """

    def test_helper(self, path, parent, name, expect):
        """File.validate_init_args returns the expected result in 'pass'."""
        fn = File._validate_init_args
        assert fn(path=path, parent=parent, name=name) == expect

    def test_raises(self, path, parent, name, expect):
        """File.__init__ raises TypeError if the test case is expected to raise."""
        if not expect:
            with pytest.raises(TypeError):
                File(path=path, parent=parent, name=name)
        else:
            try:
                File(path=path, parent=parent, name=name)
            except TypeError:
                pytest.fail("File.__init__ raised TypeError unexpectedly.")


class TestInitAttrs:
    """
    Tests proving that the returned File object has the expected attributes.
    """

    def test_parent_is_path(self):
        """Test that File.__init__ creates paths for parent attribute."""
        # Relative path as string
        assert File(path="a/b/c").parent == PP("a/b")
        # Relative path as PurePath
        assert File(path=PP("a/b/c")).parent == PP("a/b")
        # Absolute path as string
        assert File(path="/a/b/c").parent == PP("/a/b")
        # Absolute path as PurePath
        assert File(path=PP("/a/b/c")).parent == PP("/a/b")

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
        assert File(path="a/b/c").parent == expect_rel
        # Relative path as PurePath
        assert File(path=PP("a/b/c")).parent == expect_rel
        # Absolute path as string
        assert File(path="/a/b/c").parent == expect_abs
        # Absolute path as PurePath
        assert File(path=PP("/a/b/c")).parent == expect_abs

    @pytest.mark.parametrize(
        "path, parent, name",
        [
            (None, "a/b", "c"),  # No path - join parent & name
            (PP("a/b/c"), "a", "b"),  # PP type path, use path
            ("a/b/c", "a", "b"),  # Str type path, use path
            ("a/b/c", "a", None),  # Path & name, use path
            ("a/b/c", None, "b"),  # Path & parent, use path
        ],
        ids=["#0", "#1", "#2", "#3", "#4"],
    )
    def test_path_priority(self, path, parent, name):
        """File.__init__ prioritizes path over
        parent and name when assigning parent and name.
        Set expected parent to PurePath("a/b") and name to "c" and
        vary the path, parent, and name arguments to test the priority.
        """
        file = File(path=path, parent=parent, name=name)
        assert file.parent == PP("a/b")
        assert file.name == "c"

    def test_basic_attrs(self):
        """Test that File.__init__ assigns all other attributes correctly."""
        id = 42
        md5 = mock.MagicMock()
        mtime = mock.MagicMock()
        update = mock.MagicMock()
        file = File(
            path="a/b/c",
            id=id,
            md5=md5,
            mtime=mtime,
            update=update,
        )
        assert file.id == id
        assert file.md5 == md5
        assert file.mtime == mtime
        assert file.update == update
