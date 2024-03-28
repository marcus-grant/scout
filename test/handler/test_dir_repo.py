# TODO: Test constraints & indices on tables
# TODO: Normalize: Rethink need for ValueError, relatives could be relative to reporoot
# TODO: Test denormalize_path
import os
from pathlib import PurePath
import pytest
from unittest.mock import patch
import sqlite3
import tempfile

from scoutlib.handler.dir_repo import DirRepo
from scoutlib.model.dir import Dir

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

    # Redo foobar absolute path with Dir object
    abspath = Dir(path=abspath)
    msg = "Expected relative path to repo; "
    msg += f"{abspath.path}, got {base_repo.normalize_path(abspath)}"
    assert str(base_repo.normalize_path(abspath)) == "foo/bar", msg


def test_denormalize_path(base_repo):
    """
    DirRepo.denormalize_path() takes path presumed within repo...
    - Relative path returned as absolute path appended to repo root
    - Absolute path returned as itself IFF within the repo otherwise ValueError
    - Empty path returned as repo root's path
    """
    base = base_repo.path
    path = PP("a/b/c")  # Test case 1: relative path
    real_path = base_repo.denormalize_path(path)
    assert real_path == base / PP("a/b/c"), f"Expected '{base}/a/b/c', got {real_path}"

    path = "a"  # Top level relpath ensuring lack of sep and str input OK
    real_path = base_repo.denormalize_path(path)
    assert real_path == base / PP("a"), f"Expected '{base}/a', got {real_path}"

    path = ""  # Empty strings should be OK & represent root of repo
    real_path = base_repo.denormalize_path(path)
    assert real_path == base, f"Expected repo root {base}, got {real_path}"

    path = base / PP("a")  # Absolute path within repo
    real_path = base_repo.denormalize_path(path)
    assert real_path == path, f"Expected '{base}/a', got {real_path}"

    path = base.parent / "outside"  # Absolute path outside repo is error
    # Assert ValueError gets raised
    with pytest.raises(ValueError) as excinfo:
        base_repo.denormalize_path(path)
    assert str(excinfo.value) == f"Path, {path}, not within DirRepo {base}!"


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
    dir = Dir(path=base / "a")
    base_repo.add(dir)
    assert dir.id == 1, f"Expected id = 1, got {dir.id}"
    assert str(dir.path) == f"{base}/a", f"Expected path = {base}/a, got {dir.path}"
    dir_b = Dir(path=base / "b")
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
    # First arrange expected returned Dir list
    dira = Dir(path=base_repo.path / "a", id=1)
    dirb = Dir(path=base_repo.path / "a/b", id=2)
    dirc = Dir(path=base_repo.path / "a/b/c", id=3)
    dird = Dir(path=base_repo.path / "a/b/c/d", id=4)
    dirs = [dira, dirb, dirc, dird]

    # Act on the repo with add
    base = base_repo.path
    real_dirs = base_repo.add(Dir(path=(base / "a/b/c/d")))

    # Assert that the returned list is as expected
    assert real_dirs == dirs, f"Expected Dir list: {dirs}, got {real_dirs}"
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
    base_repo.add(Dir(path=base_repo.path / "a/b/c"))
    base_repo.add(Dir(path=base_repo.path / "a/d"))
    base_repo.add(Dir(path=base_repo.path / "a/e"))
    base_repo.add(Dir(path=base_repo.path / "f/g"))
    base_repo.add(Dir(path=base_repo.path / "f/h"))

    yield base_repo


def test_test_repo_tables(test_repo):
    rows = [(1, "a", "a"), (2, "b", "a/b"), (3, "c", "a/b/c")]
    rows += [(4, "d", "a/d"), (5, "e", "a/e")]
    rows += [(6, "f", "f"), (7, "g", "f/g"), (8, "h", "f/h")]
    real_rows = []
    with test_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir").fetchall()
    assert real_rows == rows, f"Expected rows: {rows}, got {real_rows}"
    rows = [(1, 1, 0)]
    rows += [(2, 2, 0), (2, 1, 1)]
    rows += [(3, 3, 0), (3, 2, 1), (3, 1, 2)]
    rows += [(4, 4, 0), (4, 1, 1)]
    rows += [(5, 5, 0), (5, 1, 1)]
    rows += [(6, 6, 0)]
    rows += [(7, 7, 0), (7, 6, 1)]
    rows += [(8, 8, 0), (8, 6, 1)]
    with test_repo.connection() as conn:
        real_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
    assert real_rows == rows, f"Expected rows: {rows}, got {real_rows}"


@pytest.mark.parametrize(
    "path,id",
    [("no/exist", None), ("a", 1), ("a/b/c", 3), ("a/d", 4), ("f/g", 7)],
)
def test_select_dir_where_path(test_repo, path, id):
    """Test that select_dir_where_path returns expected id for given path."""
    if not id:
        assert test_repo.select_dir_where_path(path) is None
        return
    real_id = test_repo.select_dir_where_path(path)[0]
    assert real_id == id, f"Expected dir.id = {id}, got {real_id}"


@pytest.mark.parametrize(
    "id,path",
    [(None, "no/exist"), (3, "a/b/c"), (5, "a/e"), (6, "f"), (8, "f/h")],
)
def test_select_dir_where_id(test_repo, id, path):
    """DirRepo.select_dir_where_id() returns correct Dir object."""
    row = test_repo.select_dir_where_id(id)
    if not id:  # If id is None, we should get None
        assert row is None, "Dir with path 'no/exist' should return an table row"
        return
    msg = f"Expected Dir.id = {id} when select by id, got {row[0]}"
    assert row[0] == id, msg
    msg = f"Expected Dir.path = {path} when select id={id}, got {row[1]}"
    assert str(row[2]) == path, msg


def same_row(real: list[tuple], expected: list[tuple], pk_index: int = 0):
    """
    Assert that two lists of tuples are the same record by primary key.
    Lists of tuples get returned by sqlite fetches, except fetchone.
    Simply provide two lists of tuples and
    (optionally) the index of the primary key in the tuple, default is 0.
    """
    if len(real) != len(expected):
        return False
    for r, e in zip(real, expected):
        if r[pk_index] != e[pk_index]:
            return False
    return True


def test_same_row():
    """
    Test helper function same_row:
    - True for rows with matching primary keys but diff num fields
    - True for rows with matching keys but different key index
    - False for rows with different primary keys
    - Same but nonzero pk_index
    - False on row count mismatch
    """
    real = [(1, "a", 10), (2, "b", 20), (3, "c", 30)]
    expected = [(1, "a"), (2, "b"), (3, "x")]
    assert same_row(real, expected)

    real = [(False, 1), (True, 2), (False, 3)]
    expected = [("a", 1, False), ("b", 2, True), ("c", 3, False)]
    assert same_row(real, expected, pk_index=1)

    real = [(4,), (9,)]
    expected = [(1,), (2,)]
    assert not same_row(real, expected)

    real = [(1, "a"), (2, "b"), (3, "c")]
    expected = [("a", 1), ("b", 2), ("c", 3)]
    assert not same_row(real, expected, pk_index=1)

    real = [(1, "a"), (2, "b"), (3, "c")]
    expected = [(1, "a"), (2, "b")]
    assert not same_row(real, expected)


def test_ancestor_dirs_where_path(test_repo):
    """DirRepo.select_dirs_where_ancestor() returns:
    - Ancestor rows of ids [2, 1] and no depth limit for (a/b/c)
    - Same result when depth is a high number
    - Same but depth=1, limits it to row of id 2
    - Ancestor row of id 6 for (f/g) with None in depth
    - Empty list for repo top level directory (f)
    - Absolute & relative repo paths make no difference"""
    fn = test_repo.ancestor_dirs_where_path

    assert same_row(fn("a/b/c"), fn(test_repo.path / "a/b/c"))
    assert same_row(fn("a/b/c"), [(2,), (1,)])
    assert same_row(fn("a/b/c", depth=99), [(2,), (1,)])
    assert same_row(fn("a/b/c", depth=1), [(2,)])
    assert same_row(fn("f/g"), [(6,)])
    assert same_row(fn("f"), [])


def test_ancestor_dirs_where_id(test_repo):
    """DirRepo.select_dirs_where_ancestor() has same spec as
    test_ancestor_dirs_where_path but with id instead of path as arg"""
    assert same_row(
        test_repo.ancestor_dirs_where_id(3),
        test_repo.ancestor_dirs_where_id(3),
    )
    assert same_row(test_repo.ancestor_dirs_where_id(3), [(2,), (1,)])
    assert same_row(test_repo.ancestor_dirs_where_id(3, depth=99), [(2,), (1,)])
    assert same_row(test_repo.ancestor_dirs_where_id(3, depth=1), [(2,)])
    assert same_row(test_repo.ancestor_dirs_where_id(7), [(6,)])
    assert same_row(test_repo.ancestor_dirs_where_id(6), [])


def test_descendant_dirs_where_path(test_repo):
    """DirRepo.select_dirs_where_descendant() returns:
    - Descendant row ids of just 3 for path (a/b)
    - Descendant rows of ids [2, 4, 5, 3] and no depth limit for (a)
    - Same result when depth is a high number
    - Same but depth=1, limits it to rows of id [2, 4, 5]
    - Descendant rows of id [7, 8] for (f)
    - Empty list for leaf directory (a/b/c)
    - Emptly list for other leaf (a/d)"""
    fn = test_repo.descendant_dirs_where_path
    assert same_row(fn("a"), fn(test_repo.path / "a"))
    assert same_row(fn("a/b"), [(3,)])
    assert same_row(fn("a"), [(2,), (4,), (5,), (3,)])
    assert same_row(fn("a", depth=99), [(2,), (4,), (5,), (3,)])
    assert same_row(fn("a", depth=1), [(2,), (4,), (5,)])
    assert same_row(fn("f"), [(7,), (8,)])
    assert same_row(fn("a/d"), [])


def test_descendant_dirs_where_id(test_repo):
    """DirRepo.select_dirs_where_descendant() has same spec as
    test_descendant_dirs_where_path but with id instead of path as arg"""
    fn = test_repo.descendant_dirs_where_id

    assert same_row(fn(1), fn(1))
    assert same_row(fn(2), [(3,)])
    assert same_row(fn(1), [(2,), (4,), (5,), (3,)])
    assert same_row(fn(1, depth=99), [(2,), (4,), (5,), (3,)])
    assert same_row(fn(1, depth=1), [(2,), (4,), (5,)])
    assert same_row(fn(4), [])
    assert same_row(fn(3), [])


@pytest.mark.parametrize(
    "id,path,dir,exp",
    [
        (8, "f/g", Dir(id=1, path="a"), Dir(id=8, path="f/h")),
        (None, "f/g", Dir(id=1, path="a"), Dir(id=7, path="f/g")),
        (None, None, Dir(id=1, path="a"), Dir(id=1, path="a")),
        (3, PP("a/e"), Dir(id=1, path="f"), Dir(id=3, path="a/b/c")),
        (None, PP("a/e"), Dir(id=1, path="f"), Dir(id=5, path="a/e")),
        (None, None, Dir(id=6, path="f"), Dir(id=6, path="f")),
        (None, None, None, ValueError),
    ],
)
def test_getone(test_repo, id, path, dir, exp):
    """
    DirRepo.getone() returns:
    - These test the order of precedence for id, path, and dir in that order
      - Dir for id=8 (f/h), despite dir & path having different values
      - Dir for path f/g and id=None, despite dir being different
      - Dir for in dir id=1, path=a, when no other arg present
    - Other set of pats, ids, dirs to ensure correct return despite changes
      - Dir for id=3 (a/b/c), despite dir and id being different
      - Dir for path a/e and id=None, despite dir being different
      - Dir for in dir id=6, path=f, when no other arg present
    - Some None returns for non-existent dir records
      - Path
    - Raise ValueError for non-existent id, path, or dir

    """
    if exp == ValueError:
        with pytest.raises(ValueError):
            test_repo.getone(id=id, path=path, dir=dir)
        return
    # Returned directories need to be denormalized and checked as normal
    # They're parametrized as normalized to be shorter
    exp.path = test_repo.path / exp.path
    assert test_repo.getone(id=id, path=path, dir=dir) == exp


def test_getone_abspath_same(test_repo):
    """Ensure absolute paths including the repo root and
    are recorded are same as relative paths within.
    This also ensures paths get normalized before query then
    denormalized when the results are returned.
    """
    fn = test_repo.getone
    assert fn(path=test_repo.path / "a/b/c") == fn(path="a/b/c")
    assert fn(path=str(test_repo.path) + "/f") == fn(path="f")
    assert fn(path=test_repo.path / "f/g") == fn(path="f/g")


def test_getone_not_exist(test_repo):
    """Ensure that getone returns None for non-existent paths within repo"""
    assert test_repo.getone(path="no/exist") is None
    assert test_repo.getone(path="a/b/noexist") is None


def test_getone_raise_outside(test_repo):
    """Ensure that getone raises ValueError for paths outside repo"""
    with pytest.raises(ValueError):
        test_repo.getone(path=test_repo.path.parent / "noexist")


# TODO: Errors when dirs get used, add testcases to normalize_path and fix
@pytest.mark.parametrize(
    "id,path,dir,method",
    [
        (8, "f/g", Dir(id=1, path="a"), "id,8"),
        (None, "f/g", Dir(id=1, path="a"), "path,f/g"),
        (None, None, Dir(id=1, path="a"), "id,1"),
        (None, None, Dir(path="a"), "path,a"),
    ],
)
def test_get_ancestors_arg_priority(test_repo, id, path, dir, method):
    """Uses patching to test correct query method gets called based on arguments"""
    with patch.object(test_repo, "ancestor_dirs_where_path") as mock_path, patch.object(
        test_repo, "ancestor_dirs_where_id"
    ) as mock_id:
        test_repo.get_ancestors(id=id, path=path, dir=dir)
        # depth = 2**31 - 1  # Max depth
        if "id" in method:
            mock_id.assert_called_once_with(id)
            mock_path.assert_not_called()
        elif "path" in method:
            mock_path.assert_called_once_with(test_repo.normalize_path(path))
            mock_id.assert_not_called()
