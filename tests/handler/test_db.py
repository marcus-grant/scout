import os
from pathlib import Path
import pytest
import sqlite3
import tempfile

from scoutlib.handler.db import DirRepo
from db_helpers import assert_table_schema, assert_table_exists
from scoutlib.model.fs import Directory


@pytest.fixture
def dir_repo():
    """Fixture for DirRepo class wrapped in temporary SQLite db file"""
    # Setup a temp file for SQLite db
    fd, path = tempfile.mkstemp(suffix=".db")
    # Close tempfile descriptor to avoid resource leak
    os.close(fd)  # DirRepo will open it as needed
    # Init DirRepo with temporary db file
    repo = DirRepo(path)
    yield repo  # Provide fixture return so it gets used
    # Teardown, so we don't leave temp files around
    os.unlink(path)


def test_dir_repo_dir_table_exists(dir_repo):
    conn = sqlite3.connect(dir_repo.path_db)
    assert_table_exists(conn, "dir")


def test_dir_repo_dir_table_schema(dir_repo):
    """
    Test that the 'directory' table exists with the expected schema, including
    checks for primary keys, foreign keys, and nullability.
    """
    # TODO: Deprecate, favoring more declarative schema testing
    # Expected schema is list for every column with tuple of:
    # (col_name: str, dtype: str, nullable: bool, prime_key: bool)
    expected_schema = [
        ("id", "INTEGER", False, True),
        ("name", "TEXT", False, False),
        # Add more columns as needed
    ]

    with sqlite3.connect(dir_repo.path_db) as conn:
        assert_table_schema(conn, "dir", expected_schema)


def test_dir_repo_dir_ancestor_table_exists(dir_repo):
    """Test that the 'dir_ancestor' table exists."""
    conn = sqlite3.connect(dir_repo.path_db)
    assert_table_exists(conn, "dir_ancestor")


def test_dir_repo_dir_ancestor_table_schema(dir_repo):
    """
    Test that the 'dir_ancestor' table exists with the expected schema, including
    checks for primary keys, foreign keys, and nullability.
    """
    # Expected schema is list for every column with tuple of:
    # (col_name: str, dtype: str, nullable: bool, prime_key: bool)
    expected_schema = [
        ("dir_id", "INTEGER", False, True),
        ("ancestor_id", "INTEGER", False, True),
        ("depth", "INTEGER", False, False),
        # Add more columns as needed
    ]
    with sqlite3.connect(dir_repo.path_db) as conn:
        assert_table_schema(conn, "dir_ancestor", expected_schema)

        schema_query = "PRAGMA table_info(dir_ancestor)"
        real_schema = conn.execute(schema_query).fetchall()

        # Pragma schema queries come in form of:
        # list of (cid, name, type, notnull, dflt_value, key) per column
        COL1, COL2, COL3 = 0, 1, 2
        NAME, DTYPE, NOTNULL, DEFAULT, ISKEY = 1, 2, 3, 4, 5

        # Check number of columns
        assert len(real_schema) == 3  # 3 columns

        # Check dir_id column
        dir_col = real_schema[COL1]
        assert dir_col[NAME] == "dir_id"
        assert dir_col[DTYPE] == "INTEGER"
        assert dir_col[NOTNULL]
        assert dir_col[DEFAULT] is None
        assert dir_col[ISKEY]

        # Check ancestor_id column
        ancestor_col = real_schema[COL2]
        assert ancestor_col[NAME] == "ancestor_id"
        assert ancestor_col[DTYPE] == "INTEGER"
        assert ancestor_col[NOTNULL] == 1
        assert ancestor_col[DEFAULT] is None
        assert ancestor_col[ISKEY]

        # Check depth column
        depth_col = real_schema[COL3]
        assert depth_col[NAME] == "depth"
        assert depth_col[DTYPE] == "INTEGER"
        assert depth_col[NOTNULL] == 1
        assert depth_col[DEFAULT] is None
        assert not depth_col[ISKEY]


def test_dir_repo_connection_returns_context_manager(dir_repo):
    with dir_repo.connection() as conn:
        # Assert that we have a valid connection object
        assert conn is not None
        assert isinstance(conn, sqlite3.Connection)
        # Perform a simple operation to ensure the connection is open
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result is not None  # This should succeed if the connection is open
    # TODO: Add connection based tests when more thought out


def test_dir_repo_normalize_path(dir_repo):
    """
    Test that normalize_path correctly normalizes paths to
    the root of the directory repo.
    """
    # First check the ValueError cases:
    # - Path of parent to repo
    # - Path completely outside the repo
    # - Empty strings
    # - Relative paths

    # Parent
    parentpath = dir_repo.path.parent
    with pytest.raises(ValueError) as excinfo:
        dir_repo.normalize_path(parentpath)
    assert str(excinfo.value) == f"Path, {parentpath}, not within DirRepo!"

    # Outside repo path
    outpath = parentpath / "outside"
    with pytest.raises(ValueError) as excinfo:
        dir_repo.normalize_path(outpath)
    assert str(excinfo.value) == f"Path, {outpath}, not within DirRepo!"

    with pytest.raises(ValueError) as excinfo:
        dir_repo.normalize_path("")
    assert str(excinfo.value) == f"Path, {'.'}, not within DirRepo!"

    with pytest.raises(ValueError) as excinfo:
        dir_repo.normalize_path("foo/bar")
    assert str(excinfo.value) == f"Path, {'foo/bar'}, not within DirRepo!"

    # Now check the valid cases
    # - Root path
    # - Path within repo (str)
    # - Path within repo (path)
    # - Path with multiple components

    assert dir_repo.normalize_path(dir_repo.path) == Path(
        "."
    ), "Root path, should return '.'"
    assert dir_repo.normalize_path(dir_repo.path / "foo") == Path(
        "foo"
    ), "Not returning path relative to repo root"
    assert dir_repo.normalize_path(f"{dir_repo.path}/foo") == Path(
        "foo"
    ), "Not returning path relative to repo root"
    assert dir_repo.normalize_path(dir_repo.path / "foo" / "bar") == Path(
        "foo/bar"
    ), "Not returning path relative to repo root"
    assert dir_repo.normalize_path(f"{dir_repo.path}/foo/bar") == Path(
        "foo/bar"
    ), "Not returning path relative to repo root"


def test_dir_repo_get_by_id(dir_repo):
    """
    Test that get() correctly retrieves a directory by id.
    """
    assert dir_repo.get(id=1) == Directory.from_path(
        dir_repo.path, id=1
    ), "Should return None if no directory exists"
