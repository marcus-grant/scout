import os
from pathlib import PurePath as PP
import pytest
import sqlite3
import tempfile

from scoutlib.handler.db_manager import DBManager


class TestInit:
    """Test cases for init and associated methods of DBManager."""

    # TODO: Needs some thought
    #       must:
    #           - Use a real absolute path based on tempfile path
    #           - Should all result in same expected path to db and fs.
    #           - Should test all None, str and PurePath permutations.
    #           - Should test default case where path_db = path_fs / .scout.db
    @pytest.mark.parametrize(
        "path_db, path_fs",
        [
            (None, None),  # Raises
            (None, None),  # Passes
            (PP("a/b/c"), None),  # Passes
            ("a/b/c", "a/b"),  # Passes
        ],
        # ids=["#0", "#1", "#2", "#3"],
    )
    def test_members_correct(self):
        """Test that the paths are converted to PurePaths."""
        db = DBManager("a/b/c")
        assert db.path_fs == PP("/a/b/scout")
        assert db.path_db == PP("a/b/c")


@pytest.fixture
def base_repo():
    """
    Fixture for basic initialized database.
    Handled in-memory-based sqlite temp file using tempfile.
    """
    fd, path = tempfile.mkstemp()  # Setup a temp file
    os.close(fd)  # Close the file descriptor (avoids leaks)
    repo = DBManager(path)  # Init DBManager
    yield repo  # Give up context to the base repo fixture
    os.unlink(path)  # Cleanup
