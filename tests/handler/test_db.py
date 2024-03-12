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


@pytest.mark.parametrize(
    "input_path,expected_ancestors",
    [
        # Test case 1: deep path
        (PurePath("a/b/c"), [PurePath("a"), PurePath("a/b"), PurePath("a/b/c")]),
        ("a", [PurePath("a")]),  # Test case 2: shallow path
        ("", []),  # Test case 3: empty path
    ],
)
def test_directory_path_ancestors(dir_repo, input_path, expected_ancestors):
    if isinstance(input_path, str) and input_path:
        input_path = f"{dir_repo.path}/{input_path}"
    real_ancestors = dir_repo.ancestor_paths(input_path)
    assert (
        real_ancestors == expected_ancestors
    ), f"Expected {expected_ancestors}, got {real_ancestors}"


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


@pytest.mark.parametrize(
    "path,id",
    [
        ("not/actually/there", None),
        ("a", 1),
        ("a/b", 2),
        ("a/b/c", 3),
        ("a/d", 4),
        ("a/e", 5),
        ("f", 6),
        ("f/g", 7),
        ("f/h", 8),
    ],
)
def test_dir_repo_select_dir_where_path(test_repo, path, id):
    if not id:
        assert test_repo.select_dir_where_path(path) is None
        return
    real_id = test_repo.select_dir_where_path(path)[0]
    assert real_id == id, f"Expected dir.id = {id}, got {real_id}"


@pytest.mark.parametrize(
    "name, path, id",
    [("a", "a", 1), ("b", "a/b", 1), ("c", "a/b/c", 1)],
)
def test_insert_into_dir_valid(dir_repo, name, path, id):
    real_id = dir_repo.insert_into_dir(name, path)
    assert real_id == id, f"Expected id = {id}, got {real_id}"
    with dir_repo.connection() as conn:
        row = conn.execute("SELECT * FROM dir WHERE path = ?", (path,)).fetchone()
        assert row[0] == id, f"Expected id = {id}, got {row[0]}"
        assert row[1] == name, f"Expected name = {name}, got {row[1]}"
        assert row[2] == path, f"Expected path = {path}, got {row[2]}"


def test_insert_into_dir_duplicate(dir_repo):
    # Insert a record
    dir_repo.insert_into_dir("a", "a")
    # Try to insert a duplicate record
    real_id = dir_repo.insert_into_dir("a", "a")
    assert (
        real_id == 1
    ), f"Expected returned ID of 1 of the duplicate row, got {real_id}"
    with dir_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir WHERE path = 'a'").fetchall()
        assert len(real_rows) == 1, f"Expected 1 row, got {len(real_rows)}"


def test_insert_into_dir_raise(dir_repo):
    with pytest.raises(ValueError) as excinfo:
        dir_repo.insert_into_dir("a", dir_repo.path.parent)
    assert str(excinfo.value) == f"Path, {dir_repo.path.parent}, not within DirRepo!"
    with pytest.raises(TypeError) as excinfo:
        dir_repo.insert_into_dir("a")


def test_insert_into_dir_ancestor_success(dir_repo):
    rows = [(1, 0, 1), (2, 1, 2), (3, 0, 1)]
    dir_repo.insert_into_dir_ancestor(rows)
    with dir_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
        assert real_rows == rows, f"Expected rows: {rows}, got {real_rows}"
    # assert result == rows[0][1], f"Expected top-level ancestor_id: {rows[0][1]}, got {result}"


def test_insert_into_dir_ancestor_duplicate(dir_repo):
    """Test that duplicate ancestor_dir rows are handled gracefully.
    Note that 2 rows are expected, not 3 or 1.
    This is becaues we should gracefully handle the duplicate row,
    but still add all the unique rows in the input.
    """
    dir_repo.insert_into_dir_ancestor([(1, 0, 1), (1, 0, 1), (2, 0, 1)])
    with dir_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
    assert len(real_rows) == 2, "Expected no duplicate ancestor_dir rows"


def test_add_without_ancestors(dir_repo):
    base = dir_repo.path
    dir = Directory(path=base / "a")
    dir_repo.add(dir)
    assert dir.id == 1, f"Expected id = 1, got {dir.id}"
    assert str(dir.path) == f"{base}/a", f"Expected path = {base}/a, got {dir.path}"


def test_add_with_ancestors(dir_repo):
    # First arrange expected returned Directory list
    dira = Directory(path=dir_repo.path / "a", id=1)
    dirb = Directory(path=dir_repo.path / "a/b", id=2)
    dirc = Directory(path=dir_repo.path / "a/b/c", id=3)
    dird = Directory(path=dir_repo.path / "a/b/c/d", id=4)
    dirs = [dira, dirb, dirc, dird]

    # Act on the repo with add
    base = dir_repo.path
    real_dirs = dir_repo.add(Directory(path=(base / "a/b/c/d")))

    # Assert that the returned list is as expected
    assert real_dirs == dirs, f"Expected Directory list: {dirs}, got {real_dirs}"
    # Assert the dir & dir_ancestor tables are as expected
    d_rows = [(1, "a", "a"), (2, "b", "a/b"), (3, "c", "a/b/c"), (4, "d", "a/b/c/d")]
    da_rows = [(1, 1, 0)]
    da_rows += [(2, 2, 0), (2, 1, 1)]
    da_rows += [(3, 3, 0), (3, 2, 1), (3, 1, 2)]
    da_rows += [(4, 4, 0), (4, 3, 1), (4, 2, 2), (4, 1, 3)]
    with dir_repo.connection() as conn:
        real_drows = conn.execute("SELECT * FROM dir").fetchall()
        real_da_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
    assert real_drows == d_rows, f"Expected rows: {d_rows}, got {real_drows}"
    assert real_da_rows == da_rows, f"Expected rows: {da_rows}, got {real_da_rows}"
