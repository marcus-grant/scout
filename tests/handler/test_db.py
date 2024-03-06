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
            (1, "name", "TEXT", True, None, False),
            (2, "path", "TEXT", True, None, False),
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


# TODO: Rethink need for ValueError, relatives could be relative to reporoot
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

    # If relatives are passed, now treated as relative to repo root
    # with pytest.raises(ValueError) as excinfo:
    #     dir_repo.normalize_path("")
    # assert str(excinfo.value) == f"Path, {'.'}, not within DirRepo!"
    #
    # with pytest.raises(ValueError) as excinfo:
    #     dir_repo.normalize_path("foo/bar")
    # assert str(excinfo.value) == f"Path, {'foo/bar'}, not within DirRepo!"

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


def test_directory_path_ancestors(dir_repo):
    real_ancestors = dir_repo.ancestor_paths(PurePath("a/b/c/d"))
    assert real_ancestors == [
        PurePath("a"),
        PurePath("a/b"),
        PurePath("a/b/c"),
        PurePath("a/b/c/d"),
    ], f"Expected ['/a', '/a/b', '/a/b/c', '/a/b/c/d'], got {real_ancestors}"
    # Now try with string off root of dir_repo
    real_ancestors = dir_repo.ancestor_paths(f"{dir_repo.path}/a")
    assert real_ancestors == [
        PurePath("a"),
    ], f"Expected ['/a'], got {real_ancestors}"
    # Finally, an empty path
    real_ancestors = dir_repo.ancestor_paths("")
    assert real_ancestors == [], f"Expected [], got {real_ancestors}"


# test_repo:
# Dir Tree: (id)
# a(1)/ ─┬─ b(2)/─── c(3)/
#        ├─ d(4)/
#        └─ e(5)/
# f(6)/ ─┬─ g(7)/
#        └─ h(8)/
@pytest.fixture
def test_repo(dir_repo):
    # Uses dir_repo initialized DirRepo and adds test data to it
    # Then yields it to test functions asking for it.
    # Finally tears it all down

    with dir_repo.connection() as conn:
        # Insert some test data
        query = "INSERT INTO dir (name, path) VALUES (?, ?)"
        conn.execute(query, ("a", "a"))
        conn.execute(query, ("b", "a/b"))
        conn.execute(query, ("c", "a/b/c"))
        conn.execute(query, ("d", "a/d"))
        conn.execute(query, ("e", "a/e"))
        conn.execute(query, ("f", "f"))
        conn.execute(query, ("g", "f/g"))
        conn.execute(query, ("h", "f/h"))
        query = "INSERT INTO dir_ancestor VALUES (?, ?, ?)"
        conn.execute(query, (1, 1, 0))
        conn.execute(query, (2, 2, 0))
        conn.execute(query, (2, 1, 1))
        conn.execute(query, (3, 3, 0))
        conn.execute(query, (3, 2, 1))
        conn.execute(query, (3, 1, 2))
        conn.execute(query, (4, 4, 0))
        conn.execute(query, (4, 1, 1))
        conn.execute(query, (5, 5, 0))
        conn.execute(query, (5, 1, 1))
        conn.execute(query, (6, 6, 0))
        conn.execute(query, (7, 7, 0))
        conn.execute(query, (7, 6, 1))
        conn.execute(query, (8, 8, 0))
        conn.execute(query, (8, 6, 1))
        conn.commit()
    yield dir_repo
    # NOTE: Might not be necessary
    # Teardown, so we don't leave temp files around
    # os.unlink(dir_repo.path_db)


def test_dir_repo_select_dir_where_path(test_repo):
    row = test_repo.query_path("a")
    assert row[0] == 1, f"Expected dir.id = 1, got {row[0]}"
    row = test_repo.query_path("a")
    assert row[0] == 1, f"Expected dir.id = 1, got {row[0]}"
    row = test_repo.query_path("a/b")
    assert row[0] == 2, f"Expected dir.id = 2, got {row[0]}"
    row = test_repo.query_path("a/b/c")
    assert row[0] == 3, f"Expected dir.id = 3, got {row[0]}"
    row = test_repo.query_path("a/d")
    assert row[0] == 4, f"Expected dir.id = 4, got {row[0]}"
    row = test_repo.query_path("a/e")
    assert row[0] == 5, f"Expected dir.id = 5, got {row[0]}"
    row = test_repo.query_path("f")
    assert row[0] == 6, f"Expected dir.id = 6, got {row[0]}"
    row = test_repo.query_path("f/g")
    assert row[0] == 7, f"Expected dir.id = 7, got {row[0]}"
    row = test_repo.query_path("f/h")
    assert row[0] == 8, f"Expected dir.id = 8, got {row[0]}"


def test_dir_repo_select_joined_path_ancestors(test_repo):
    # Change dir entry of id 8 to have name 'd' to
    # test duplicates with id 4.
    # Dirnames are not unique, paths are.
    # So this needs to be able to discriminate between them.
    with test_repo.connection() as conn:
        conn.execute("UPDATE dir SET name = 'd' WHERE id = 8")
        conn.execute("UPDATE dir SET path = 'f/d' WHERE id = 8")
        conn.commit()
    row = test_repo.select_joined_path_ancestors("f/d")
    assert row[0] == 8, f"Expected dir.id = 8, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("a")
    # assert row[0] == 1, f"Expected dir.id = 1, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("a/b")
    # assert row[0] == 2, f"Expected dir.id = 2, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("a/b/c")
    # assert row[0] == 3, f"Expected dir.id = 3, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("a/d")
    # assert row[0] == 4, f"Expected dir.id = 4, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("a/e")
    # assert row[0] == 5, f"Expected dir.id = 5, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("f")
    # assert row[0] == 6, f"Expected dir.id = 6, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("f/g")
    # assert row[0] == 7, f"Expected dir.id = 7, got {row[0]}"
    # row = test_repo.select_joined_path_ancestors("f/h")
    # assert row[0] == 8, f"Expected dir.id = 8, got {row[0]}"


# def test_dir_repo_add(dir_repo):
#     # Dir Tree: (id)
#     # a(1)/ ─┬─ b(2)/─── c(3)/
#     #        ├─ d(4)/
#     #        ├─ e(5)/
#     # f(6)/ ─┬─ g(7)/
#     #        └─ h(8)/
#     # Arrange
#     expected_dirs: list[tuple[int, str]] = []
#     expected_dirs.extend([(1, "a"), (2, "a/b"), (3, "a/b/c")])
#     expected_dirs.extend([(4, "a/d"), (5, "a/e")])
#     expected_dirs.extend([(6, "f"), (7, "f/g"), (8, "f/h")])
#     # expected_ancestors: list[tuple[int, int, int]] = [
#     #     (1, 1, 0),
#     #     (2, 2, 0),
#     #     (3, 3, 0),
#     #     (4, 4, 0),
#     #     (5, 5, 0),
#     #     (6, 6, 0),
#     #     (7, 7, 0),
#     #     (8, 8, 0),
#     #     (2, 1, 1),
#     #     (3, 2, 1),
#     #     (4, 1, 1),
#     #     (5, 1, 1),
#     #     (7, 6, 1),
#     #     (8, 6, 1),
#     # ]
#     # Act
#     dir_repo.add("a")
#     dir_repo.add("a/b")
#     dir_repo.add("a/b/c")
#     dir_repo.add("a/d")
#     dir_repo.add("a/e")
#     dir_repo.add("f")
#     dir_repo.add("f/g")
#     dir_repo.add(PurePath(dir_repo.path) / "f/h")
#     # Assert
#     with dir_repo.connection() as conn:
#         real_dirs = conn.execute("SELECT * FROM dir").fetchall()
#         assert real_dirs == expected_dirs
#         real_ancestors = conn.execute("SELECT * FROM dir_ancestor").fetchall()
#         raise LookupError(f"real_ancestors = {real_ancestors}")
