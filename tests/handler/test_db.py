# TODO: Test constraints & indices on tables
import os
from pathlib import Path, PurePath
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


def test_dir_repo_dir_table(dir_repo):
    """
    Test that the 'directory' table exists with the expected schema,
    including checks for primary keys, foreign keys, and nullability.
    """
    with sqlite3.connect(dir_repo.path_db) as conn:
        # First ensure the table exists
        table_query = """SELECT name FROM sqlite_master
                    WHERE type='table' AND name='dir'"""
        assert (
            "dir" in conn.execute(table_query).fetchone()
        ), "Table 'dir' does not exist."

        # Now query the schema and assert it's valid
        # Pragma schema queries come in form of:
        # list of (cid, name, type, notnull, dflt_value, key) per column
        schema_query = "PRAGMA table_info(dir)"
        real_schema = conn.execute(schema_query).fetchall()

        # Expected schema is list for every column with tuple of:
        # (num: int, name: str, dtype: str, nullable: bool, prime_key: bool)
        # Bools are represented as 0|1, but python evaluates them as False|True
        expected_schema = [
            (0, "id", "INTEGER", True, None, True),
            (1, "path", "TEXT", True, None, False),
        ]

        # Check column count
        assert len(real_schema) == len(
            expected_schema
        ), f"Expected {len(expected_schema)} columns, got {len(real_schema)}"

        # Check id column
        assert (
            real_schema[0] == expected_schema[0]
        ), "Bad dir table shcema in id column."

        # Check name column
        assert (
            real_schema[1] == expected_schema[1]
        ), "Bad dir table schema in name column."


def test_dir_repo_dir_ancestor_table(dir_repo):
    """
    Test that the 'dir_ancestor' table exists with the expected schema,
    including checks for primary keys, foreign keys, and nullability.
    """
    with sqlite3.connect(dir_repo.path_db) as conn:
        # First ensure the table exists
        table_query = """SELECT name FROM sqlite_master
                    WHERE type='table' AND name='dir_ancestor'"""
        assert (
            "dir_ancestor" in conn.execute(table_query).fetchone()
        ), "Table 'dir_ancestor' does not exist."

        # Now query the schema and assert it's valid
        schema_query = "PRAGMA table_info(dir_ancestor)"
        real_schema = conn.execute(schema_query).fetchall()

        # Pragma schema queries come in form of:
        # list of (cid, name, type, notnull, dflt_value, key) per column
        expected_schema = [
            (0, "dir_id", "INTEGER", True, None, 1),
            (1, "ancestor_id", "INTEGER", 1, None, 2),
            (2, "depth", "INTEGER", True, None, False),
        ]

        # Check number of columns
        assert len(real_schema) == len(expected_schema), f"""
            Expected {len(expected_schema)} columns in 'dir_ancestor',
            got {len(real_schema)}"""

        # Check dir_id column
        assert (
            real_schema[0] == expected_schema[0]
        ), "Bad dir_ancestor schema in dir_id column."

        # Check ancestor_id column
        assert (
            real_schema[1] == expected_schema[1]
        ), "Bad dir_ancestor schema in ancestor_id column."

        # Check depth column
        assert (
            real_schema[2] == expected_schema[2]
        ), "Bad dir_ancestor schema in depth column."


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


# Dir Tree: (id)
# a(1)/ ─┬─ b(2)/─── c(3)/
#        ├─ d(4)/
#        ├─ e(5)/
# f(6)/ ─┬─ g(7)/
#        └─ h(8)/


def test_dir_repo_dir_get_path(dir_repo):
    with dir_repo.connection() as conn:
        # Insert some test data
        conn.execute("INSERT INTO dir (path) VALUES ('a')")
        conn.execute("INSERT INTO dir (path) VALUES ('a/b')")
        conn.execute("INSERT INTO dir (path) VALUES ('a/b/c')")
        conn.execute("INSERT INTO dir (path) VALUES ('a/d')")
        conn.execute("INSERT INTO dir (path) VALUES ('a/e')")
        conn.execute("INSERT INTO dir (path) VALUES ('f')")
        conn.execute("INSERT INTO dir (path) VALUES ('f/g')")
        conn.execute("INSERT INTO dir (path) VALUES ('f/h')")
        conn.commit()

    # with dir_repo.connection() as conn:
    #     # Query the data
    #     dir_rows = conn.execute("SELECT * FROM dir").fetchall()
    #     raise LookupError(f"dir_rows = {dir_rows}")
    #
    base = dir_repo.path
    assert dir_repo.get_path(base / "a") == Directory(id=1, path=(base / PurePath("a")))
    assert dir_repo.get_path(base / "a/b") == Directory(
        id=2, path=(base / PurePath("a/b"))
    )
    assert dir_repo.get_path(base / "a/b/c") == Directory(
        id=3, path=(base / PurePath("a/b/c"))
    )
    assert dir_repo.get_path(base / "a/d") == Directory(
        id=4, path=(base / PurePath("a/d"))
    )
