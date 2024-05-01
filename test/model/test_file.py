import unittest.mock as mock
import os
import pytest
from pathlib import PurePath as PP

from scoutlib.model.file import File

# These argument permutations are all combos of None or a path type.
# Three arguments determine if the init should raise a TypeError.
# That means there's 8 permutations to test.
INIT_ARGS_THAT_PASS = [
    (None, "a/b", "c"),
    (PP("a/b/c"), None, None),
    ("a/b/c", None, "c"),
    (PP("a/b/c"), PP("a/b"), None),
    ("a/b/c", "a/b", "c"),
]
INIT_ARGS_THAT_RAISE = [
    (None, None, None),
    (None, None, "c"),
    (None, PP("a/b"), None),
]


@pytest.mark.parametrize("path, parent, name", INIT_ARGS_THAT_PASS)
def test_validate_init_args_true(path, parent, name):
    """
    File.validate_init_args should return True if given...
        - path, parent, name
        - path, parent
        - parent, name
    These examples are given in INIT_ARGS_THAT_PASS.
    """
    assert File.validate_init_args(path=path, parent=parent, name=name)


@pytest.mark.parametrize("path, parent, name", INIT_ARGS_THAT_RAISE)
def test_validate_init_args_false(path, parent, name):
    """
    File.validate_init_args should return False if inverse of previous test.
    These examples are given in INIT_ARGS_THAT_RAISE.
    """
    assert not File.validate_init_args(path=path, parent=parent, name=name)


@pytest.mark.parametrize("path, parent, name", INIT_ARGS_THAT_PASS)
def test_file_init_passes(path, parent, name):
    """
    File.__init__ should pass if given arg case of test_validate_init_args_true.
    These examples are given in INIT_ARGS_THAT_PASS.
    """
    try:
        File(path=path, parent=parent, name=name)
    except TypeError:
        pytest.fail("File.__init__ raised TypeError unexpectedly.")


@pytest.mark.parametrize("path, parent, name", INIT_ARGS_THAT_RAISE)
def test_file_init_raises(path, parent, name):
    """
    File.__init__ should raise TypeError if given arg case of
    test_validate_init_args_false.
    These examples are given in INIT_ARGS_THAT_RAISE.
    """
    with pytest.raises(TypeError):
        File(path=path, parent=parent, name=name)
