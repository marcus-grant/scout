import os
from pathlib import PurePath as PP
import pytest
import sqlite3
import tempfile
from typing import Optional, Union, Tuple

from scoutlib.handler.db_manager import DBManager


def mktemp_safe() -> PP:
    fd, path = tempfile.mkstemp()  # Setup a temp file
    os.close(fd)  # Close the file descriptor (avoids leaks)
    return PP(path)


def clean_tempfile(path: Union[PP, str]):
    os.unlink(path)  # Cleanup


class TestInit:
    """Test cases for init and associated methods of DBManager."""

    # TODO: Needs some thought
    #       must:
    #           - Use a real absolute path based on tempfile path
    #           - Should all result in same expected path to db and fs.
    #           - Should test all None, str and PurePath permutations.
    #           - Should test default case where path_db = path_fs / .scout.db
    @pytest.mark.parametrize(
        "args, expect",
        [
            ((PP("/a/b/c"), None), (PP("/a/b/c"), PP("/a/b"))),  # Default root
            (("a/b/c", None), (PP("a/b/c"), PP("a/b"))),  # Same but str db_path
            ((PP("/a/b/c"), PP("/f/g")), (PP("/a/b/c"), PP("/f/g"))),  # Outside root
            (("a/b/c", "f/g"), (PP("a/b/c"), PP("f/g"))),  # Same but str root
        ],
        # ids=["#0", "#1", "#2", "#3"],
    )
    def test_members_ok(self, args, expect):
        """Test that the paths are converted to PurePaths."""
        db = DBManager(*args)
        assert db.path == expect[0]
        assert db.root == expect[1]

    # TODO: Test that db already exists and checks mock call for _init_db called
    # TODO: Test for init raises for wrong args
    # TODO: Check that _init_db creates config table


@pytest.fixture
def base_repo():
    """
    Fixture for basic initialized database.
    Handled in-memory-based sqlite temp file using tempfile.
    """
    fd, path = tempfile.mkstemp()  # Setup a temp file
    os.close(fd)  # Close the file descriptor (avoids leaks)
    yield DBManager(path)  # Give up context to the init'd DBManager
    clean_tempfile(path)  # Cleanup
