from contextlib import contextmanager
from unittest.mock import patch
import os
from pathlib import PurePath as PP
import pytest
import sqlite3
import tempfile
from typing import Optional, Union, Generator

from scoutlib.handler.db_manager import DBManager


@pytest.fixture
def base_repo():
    """Fixture for DirRepo class wrapped in temporary SQLite db file"""
    # Setup a temp file for SQLite db
    fd, path = tempfile.mkstemp(suffix=".db")
    # Close tempfile descriptor to avoid resource leak
    os.close(fd)  # DirRepo will open it as needed
    # Init DirRepo with temporary db file
    repo = DBManager(path)
    yield repo  # Provide fixture return so it gets used
    # Teardown, so we don't leave temp files around
    os.unlink(path)


class TestInit:
    """Test cases for init and associated methods of DBManager."""

    @pytest.mark.parametrize(
        "path, root",
        [
            (1, None),
            (PP("/a/b/c"), 1),
            (None, None),
        ],
    )
    def test_args_raise(self, path, root):
        """Test that these conditions raise a ValueError or TypeError.
        - path_db is not a PurePath or str
        - path_fs is not a PurePath or str
        - parent of path_db is not a dir
        - path_fs is not a dir
        """
        with pytest.raises((ValueError, TypeError)):
            DBManager(path, root)

    def test_default_root(self):
        """When no root given, assumes parent dir of db file is root"""
        # Create fake file
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        # Act
        db = DBManager(path)
        # Assert
        assert db.root == PP(os.path.dirname(path))
        # Cleanup
        os.unlink(path)

    def test_call_init_db_if_not_there(self):
        """Calls DBManager._init_db if the db does not exist."""
        # Setup a temp directory using tempfile
        with tempfile.TemporaryDirectory() as tmpdir:
            # Setup mock of _init_db
            with patch("scoutlib.handler.db_manager.DBManager._init_db") as mock_init:
                DBManager(f"{tmpdir}/scout.db")
                mock_init.assert_called_once()

    def test_dont_call_init_db_if_there(self, base_repo):
        """Test that the db exists and does not call _init_db."""
        with patch("scoutlib.handler.db_manager.DBManager._init_db") as mock_init:
            DBManager(base_repo.path)
            mock_init.assert_not_called()

    def test_fs_meta_table_exists(self, base_repo):
        """Test that the fs_meta table exists."""
        # First check that the file exists
        with sqlite3.connect(base_repo.path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            assert ("fs_meta",) in tables

    def test_fs_meta_schema(self, base_repo):
        # Expected schema is list for every column with tuple of:
        # (num: int, name: str, dtype: str, nullable: bool, prime_key: bool)
        # Bools are represented as 0|1, but python evaluates them as False|True
        expected_schema = [
            (0, "property", "TEXT", 0, None, 1),
            (1, "value", "TEXT", 0, None, 0),
        ]
        with sqlite3.connect(base_repo.path) as conn:
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(fs_meta);")
            schema = cursor.fetchall()
            assert schema == expected_schema

    # TODO: This needs to be parroted on all component repo modules
    def test_init_db_not_alter_existing_table(self):
        """Test that _init_db does not alter existing tables."""
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        with sqlite3.connect(path) as conn:
            c = conn.cursor()
            c.execute("CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);")
            c.execute(
                "INSERT INTO fs_meta (property, value) VALUES ('root', '/a/b/c');"
            )
            conn.commit()
        # Call DBManager with existing db files
        db = DBManager(path)
        with sqlite3.connect(db.path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM fs_meta;")
            assert cursor.fetchall() == [("root", "/a/b/c")]
        # Cleanup
        os.unlink(path)

    def test_member_repos(self, base_repo):
        """Test that the DirRepo and FileRepo members are created."""
        assert base_repo.dir_repo is not None
        assert base_repo.dir_repo.path_db == base_repo.path
        assert base_repo.dir_repo.root == base_repo.root
        assert base_repo.file_repo is not None

    # TODO: Test that db already exists and checks mock call for _init_db called
    # TODO: Test for init raises for wrong args
    # TODO: Check that _init_db creates config table
