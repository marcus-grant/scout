# TODO: Test constraints & indices on tables
# TODO: Normalize: Rethink need for ValueError, relatives could be relative to reporoot
# TODO: Test denormalize_path
import os
from pathlib import PurePath
import pytest
import sqlite3
import tempfile

from scoutlib.handler.dir_repo import DirRepo
from scoutlib.model.fs import Directory

PP = PurePath


@pytest.fixture
def base_repo():
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


def test_table_dir(base_repo):
    """
    Test that the 'directory' table exists with the expected schema,
    including checks for primary keys, foreign keys, and nullability.
    """
    with sqlite3.connect(base_repo.path_db) as conn:
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


def test_table_dir_ancestor(base_repo):
    """
    Test that the 'dir_ancestor' table exists with the expected schema,
    including checks for primary keys, foreign keys, and nullability.
    """
    with sqlite3.connect(base_repo.path_db) as conn:
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
        msg = "Expected 3 columns in 'dir_ancestor', got {len(real_schema)}"
        assert len(real_schema) == len(expected_schema), msg

        # Check dir_id column
        msg = "Bad dir_ancestor schema in dir_id column."
        assert real_schema[0] == expected_schema[0], msg

        # Check ancestor_id column
        msg = "Bad dir_ancestor schema in ancestor_id column."
        assert real_schema[1] == expected_schema[1], msg

        # Check depth column
        msg = "Bad dir_ancestor schema in depth column."
        assert real_schema[2] == expected_schema[2], msg


def test_connect(base_repo):
    """DirRepo.connection() returns a valid sqlite3.Connection object."""
    with base_repo.connection() as conn:
        # Assert that we have a valid connection object
        assert conn is not None
        assert isinstance(conn, sqlite3.Connection)
        # Perform a simple operation to ensure the connection is open
        cursor = conn.cursor().execute("SELECT 1")
        result = cursor.fetchone()
        assert result is not None  # This should succeed if the connection is open


def test_normalize_path(base_repo):
    """
    Test that normalize_path correctly normalizes paths to
    the root of the directory repo.
    Checking for inputs of
    parent to repo, outside repo, empty strings, relative paths and
    finally absolute paths within repo returned as relative to its root.
    """
    # Parent, should raise error as it's not within repo
    parentpath = base_repo.path.parent
    with pytest.raises(ValueError) as excinfo:
        base_repo.normalize_path(parentpath)
    assert str(excinfo.value) == f"Path, {parentpath}, not within DirRepo!"

    # Outside repo path, should raise error
    outpath = parentpath / "outside"
    with pytest.raises(ValueError) as excinfo:
        base_repo.normalize_path(outpath)
    assert str(excinfo.value) == f"Path, {outpath}, not within DirRepo!"

    # Empty string, should be root of repo
    msg = "Root path, should return '.', aka root of repo."
    assert base_repo.normalize_path("") == PP("."), msg

    # Relative path, should be relative to repo root
    relpath = "foo/bar"
    msg = "Expected relative path to repo; "
    msg += f"{relpath}, got {base_repo.normalize_path(relpath)}"
    assert str(base_repo.normalize_path(relpath)) == relpath, msg

    # Absolute path within repo, should be relative to repo root
    abspath = base_repo.path / "foo" / "bar"
    msg = "Expected relative path to repo; "
    msg += f"{abspath}, got {base_repo.normalize_path(abspath)}"
    assert str(base_repo.normalize_path(abspath)) == "foo/bar", msg


@pytest.mark.parametrize(
    "path,ancestors",  # Input path & expected ancestors
    [  # Test case 1: deep path
        (PP("a/b/c"), [PP("a"), PP("a/b"), PP("a/b/c")]),
        ("a", [PP("a")]),  # Test case 2: shallow path
        ("", []),  # Test case 3: empty path
    ],
)
def test_ancestor_paths(base_repo, path, ancestors):
    """
    DirRepo.ancestor_paths() returns correct ancestors of a path.
    In this test, the ancestors of 'a/b/c' are 'a', 'a/b', and 'a/b/c'.
    """
    if isinstance(path, str) and path:
        path = f"{base_repo.path}/{path}"
    real_ancestors = base_repo.ancestor_paths(path)
    assert real_ancestors == ancestors, f"Expected {ancestors}, got {real_ancestors}"


@pytest.mark.parametrize(
    "name, path, id",
    [("a", "a", 1), ("b", "a/b", 1), ("c", "a/b/c", 1)],
)
def test_insert_into_dir(base_repo, name, path, id):
    """DirRepo.insert_into_dir() inserts correct records & returns correct id."""
    real_id = base_repo.insert_into_dir(name, path)
    assert real_id == id, f"Expected id = {id}, got {real_id}"
    with base_repo.connection() as conn:
        row = conn.execute("SELECT * FROM dir WHERE path = ?", (path,)).fetchone()
        assert row[0] == id, f"Expected id = {id}, got {row[0]}"
        assert row[1] == name, f"Expected name = {name}, got {row[1]}"
        assert row[2] == path, f"Expected path = {path}, got {row[2]}"


def test_insert_into_dir_duplicate(base_repo):
    """DirRepo.insert_into_dir() handles duplicate records gracefully."""
    # Insert a record
    base_repo.insert_into_dir("a", "a")
    # Try to insert a duplicate record
    real_id = base_repo.insert_into_dir("a", "a")
    msg = f"Expected ID of 1 of duplicate dir row, got {real_id}"
    assert real_id == 1, msg
    with base_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir WHERE path = 'a'").fetchall()
        assert len(real_rows) == 1, f"Expected 1 row, got {len(real_rows)}"


def test_insert_into_dir_raise(base_repo):
    """DirRepo.insert_into_dir() raises ValueError for invalid paths."""
    with pytest.raises(ValueError) as excinfo:
        base_repo.insert_into_dir("a", base_repo.path.parent)
    assert str(excinfo.value) == f"Path, {base_repo.path.parent}, not within DirRepo!"
    with pytest.raises(TypeError) as excinfo:
        base_repo.insert_into_dir("a")


def test_insert_into_dir_ancestor(base_repo):
    """DirRepo.insert_into_dir_ancestor() inserts correct records."""
    rows = [(1, 0, 1), (2, 1, 2), (3, 0, 1)]
    base_repo.insert_into_dir_ancestor(rows)
    with base_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
    assert real_rows == rows, f"Expected rows: {rows}, got {real_rows}"


def test_insert_into_dir_ancestor_duplicate(base_repo):
    """
    DirRepo.insert_into_dir_ancestor() handles duplicate records gracefully.
    Note that 2 rows are expected, not 3 or 1.
    This is becaues we should gracefully handle the duplicate row,
    but still add all the unique rows in the input.
    """
    base_repo.insert_into_dir_ancestor([(1, 0, 1), (1, 0, 1), (2, 0, 1)])
    with base_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
    assert len(real_rows) == 2, "Expected no duplicate ancestor_dir rows"


def test_add_without_ancestors(base_repo):
    """DirRepo.add() adds a directory without ancestors correctly."""
    base = base_repo.path
    dir = Directory(path=base / "a")
    base_repo.add(dir)
    assert dir.id == 1, f"Expected id = 1, got {dir.id}"
    assert str(dir.path) == f"{base}/a", f"Expected path = {base}/a, got {dir.path}"
    dir_b = Directory(path=base / "b")
    base_repo.add(dir_b)
    assert dir_b.id == 2, f"Expected id = 2, got {dir_b.id}"
    assert str(dir_b.path) == f"{base}/b", f"Expected path = {base}/b, got {dir_b.path}"


def test_add_with_ancestors(base_repo):
    """
    DirRepo.add() adds a directory with ancestors correctly.
        - dir table has a, a/b, a/b/c, a/b/c/d as individual records
        - they all have 1...4 as their id respectively
        - all other dependant columns are as expected
        - dir_ancestor table has all the expected foreign keys
        - dir_ancestor records are in right order
    """
    # First arrange expected returned Directory list
    dira = Directory(path=base_repo.path / "a", id=1)
    dirb = Directory(path=base_repo.path / "a/b", id=2)
    dirc = Directory(path=base_repo.path / "a/b/c", id=3)
    dird = Directory(path=base_repo.path / "a/b/c/d", id=4)
    dirs = [dira, dirb, dirc, dird]

    # Act on the repo with add
    base = base_repo.path
    real_dirs = base_repo.add(Directory(path=(base / "a/b/c/d")))

    # Assert that the returned list is as expected
    assert real_dirs == dirs, f"Expected Directory list: {dirs}, got {real_dirs}"
    # Assert the dir & dir_ancestor tables are as expected
    d_rows = [(1, "a", "a"), (2, "b", "a/b"), (3, "c", "a/b/c"), (4, "d", "a/b/c/d")]
    da_rows = [(1, 1, 0)]
    da_rows += [(2, 2, 0), (2, 1, 1)]
    da_rows += [(3, 3, 0), (3, 2, 1), (3, 1, 2)]
    da_rows += [(4, 4, 0), (4, 3, 1), (4, 2, 2), (4, 1, 3)]
    with base_repo.connection() as conn:
        real_drows = conn.execute("SELECT * FROM dir").fetchall()
        real_da_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
    assert real_drows == d_rows, f"Expected rows: {d_rows}, got {real_drows}"
    assert real_da_rows == da_rows, f"Expected rows: {da_rows}, got {real_da_rows}"


# TODO: Should teardown be added?
@pytest.fixture
def test_repo(base_repo):
    """
    Create a DirRepo with a preset directory tree for testing like so:
    Dir Tree: (id)
    a(1)/ ─┬─ b(2)/─── c(3)/
           ├─ d(4)/
           └─ e(5)/
    f(6)/ ─┬─ g(7)/
           └─ h(8)/
    """

    # Use tested add method to create dirtree in dir & dir_ancestor tables
    base_repo.add(Directory(path=base_repo.path / "a/b/c"))
    base_repo.add(Directory(path=base_repo.path / "a/d"))
    base_repo.add(Directory(path=base_repo.path / "a/e"))
    base_repo.add(Directory(path=base_repo.path / "f/g"))
    base_repo.add(Directory(path=base_repo.path / "f/h"))

    yield base_repo


@pytest.mark.parametrize(
    "path,id",
    [
        ("no/exist", None),
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
def test_select_dir_where_path(test_repo, path, id):
    """Test that select_dir_where_path returns expected id for given path."""
    if not id:
        assert test_repo.select_dir_where_path(path) is None
        return
    real_id = test_repo.select_dir_where_path(path)[0]
    assert real_id == id, f"Expected dir.id = {id}, got {real_id}"