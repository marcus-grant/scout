# TODO: Test constraints & indices on tables
from contextlib import contextmanager
import os
from pathlib import PurePath
import pytest
from unittest.mock import patch
import tempfile
from typing import List, Tuple

from lib.handler.dir_repo import DirRepo
from lib.model.dir import Dir
from lib.handler.db_connector import DBConnector
from lib.handler.db_connector import DBPathOutsideTargetError

PP = PurePath

MOD_REPO = "lib.handler.dir_repo.DirRepo"
MOD_DBC = "lib.handler.db_connector.DBConnector"


### Module fixtures
@contextmanager
def temp_dir_context():
    with tempfile.TemporaryDirectory() as tempdir:
        yield PP(tempdir)


@pytest.fixture
@contextmanager
def base_dbconn():
    with temp_dir_context() as tempdir:
        db = DBConnector(tempdir / ".scout.db")
        yield db


@pytest.fixture
@contextmanager
def base_repo(base_dbconn):
    with base_dbconn as db:
        repo = DirRepo(db)
        yield repo


def same_rows(real: List[Tuple], expected: List[Tuple], pk_index: int = 0) -> bool:
    """
    Determine if two lists of tuples represent the same records by comparing the primary key.

    This function compares two lists of tuples typically returned by SQLite fetch operations.
    It checks if the primary keys (as specified by pk_index) of the tuples in both lists are the same.

    Args:
        real (List[Tuple]): The first list of tuples to compare.
        expected (List[Tuple]): The second list of tuples to compare.
        pk_index (int, optional): The index of the primary key in the tuple. Default is 0.

    Returns:
        bool: True if both lists have the same primary keys in the same order, False otherwise.
    """
    if len(real) != len(expected):
        return False

    real_keys = {row[pk_index] for row in real}
    expected_keys = {row[pk_index] for row in expected}

    return real_keys == expected_keys


# TODO: Should teardown be added?
@pytest.fixture
@contextmanager
def test_repo(base_repo):
    """
    Create a DirRepo with a preset directory tree for testing like so:
    Dir Tree: (id)
    a(1)/ ─┬─ b(2)/─── c(3)/
           ├─ d(4)/
           └─ e(5)/
    f(6)/ ─┬─ g(7)/
           └─ h(8)/
    NOTE: Uses DirRepo.add() to create the tree in the database.
            Should be used AFTER asserting DirRepo.add() works.
    """
    with base_repo as repo:
        repo.add(Dir(path=repo.db.root / "a/b/c"))
        repo.add(Dir(path=repo.db.root / "a/d"))
        repo.add(Dir(path=repo.db.root / "a/e"))
        repo.add(Dir(path=repo.db.root / "f/g"))
        repo.add(Dir(path=repo.db.root / "f/h"))
        yield repo


class Dirs:
    def __init__(self, root) -> None:
        root = PP(root) if root is not None else PP()
        self.a = Dir(id=1, path=root / "a")
        self.b = Dir(id=2, path=root / "a/b")
        self.c = Dir(id=3, path=root / "a/b/c")
        self.d = Dir(id=4, path=root / "a/d")
        self.e = Dir(id=5, path=root / "a/e")
        self.f = Dir(id=6, path=root / "f")
        self.g = Dir(id=7, path=root / "f/g")
        self.h = Dir(id=8, path=root / "f/h")


class TestFixtures:
    """Test fixtures for use in later tests"""

    def testTempDirContext(self):
        with temp_dir_context() as tempdir:
            assert os.path.isdir(tempdir)
            with open(tempdir / "foobar.txt", "w") as f:
                f.write("foobar")
            with open(tempdir / "foobar.txt", "r") as f:
                assert f.read() == "foobar"

    def testTempDirContextCleanup(self):
        """Ensure cleanup of temporary directory after context"""
        with temp_dir_context() as tempdir:
            with pytest.raises(FileNotFoundError):
                with open(tempdir / "foobar.txt", "r") as f:
                    f.read()
        assert not os.path.isdir(tempdir)

    def testBaseDBConn(self, base_dbconn):
        with base_dbconn as db:
            assert db.root == db.path.parent
            assert os.path.isfile(db.path)
            assert os.path.isdir(db.root)
            assert DBConnector.read_root(db.path) == db.root
            assert DBConnector.is_scout_db_file(db.path)

    def testBaseRepo(self, base_repo):
        with base_repo as repo:
            assert repo.db.root == repo.db.path.parent
            assert repo.db.path == repo.db.root / ".scout.db"
            assert os.path.isfile(repo.db.path)
            assert os.path.isdir(repo.db.root)
            with repo.db.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                assert ("dir",) in tables
                assert ("dir_ancestor",) in tables

    def testTestRepo(self, test_repo):
        d_rows = []
        da_rows = []
        with test_repo as repo:
            with repo.db.connect() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM dir")
                d_rows = cursor.fetchall()
                cursor.execute("SELECT * FROM dir_ancestor")
                da_rows = cursor.fetchall()
        d_expect = [(1, "a"), (2, "a/b"), (3, "a/b/c")]
        d_expect += [(4, "a/d"), (5, "a/e"), (6, "f"), (7, "f/g"), (8, "f/h")]
        assert d_rows == d_expect
        da_expect = [(1, 1, 0)]
        da_expect += [(2, 2, 0), (2, 1, 1)]
        da_expect += [(3, 3, 0), (3, 2, 1), (3, 1, 2)]
        da_expect += [(4, 4, 0), (4, 1, 1)]
        da_expect += [(5, 5, 0), (5, 1, 1)]
        da_expect += [(6, 6, 0)]
        da_expect += [(7, 7, 0), (7, 6, 1)]
        da_expect += [(8, 8, 0), (8, 6, 1)]
        assert da_rows == da_expect

    def testSameRowEqual(self):
        real = [(1, "a", b"dead"), (2, "b", b"beef"), (3, "c", b"cafe")]
        expect = real
        assert same_rows(real, expect)

    def testSameRowDiffLen(self):
        rows = [(1,), (2,)]
        assert not same_rows(rows, rows + [(3,)])

    def testSameRowDiffPK(self):
        assert not same_rows(
            [(1, "a"), (2, "b"), (3, "c")], [(1, "a"), (2, "b"), (4, "c")]
        )

    def testSameRowNonZeroPKIdx(self):
        assert same_rows(
            [(1, "a"), (2, "b"), (3, "c")], [(1, "a"), (2, "b"), (3, "c")], pk_index=1
        )

    def testSameRowEmpty(self):
        assert same_rows([], [])
        assert not same_rows([(1,)], [])


class TestInitHelpers:
    """Tests helper methods __init__ uses, NOT __init__ itself."""

    def testCreateDirTable(self, base_dbconn):
        # Arrange
        table_query = "SELECT name FROM sqlite_master WHERE type='table';"
        # Pragma schema queries come in form of:
        # list of (cid, name, type, notnull, dflt_value, key) per column
        schema_query = "PRAGMA table_info(dir)"
        expect = [
            # Expected schema is list for every column with tuple of:
            # (num: int, name: str, dtype: str, nullable: bool, prime_key: bool)
            # Bools are represented as 0|1, but python evaluates them as False|True
            (0, "id", "INTEGER", 1, None, 1),
            (1, "path", "TEXT", 1, None, 0),
        ]
        # Act
        with base_dbconn as db:
            DirRepo.create_dir_table(db)
            with db.connect() as conn:
                cursor = conn.cursor()

                # Assert
                cursor.execute(table_query)
                tables = cursor.fetchall()
                cursor.execute(schema_query)
                schema = cursor.fetchall()

                # Check for table name
                assert ("dir",) in tables

                # Check column count
                assert len(schema) == len(expect)

                # Check id column
                assert schema[0] == expect[0]

                # Check path column
                assert schema[1] == expect[1]

    def testCreateDirAncTable(self, base_dbconn):
        # Arrange
        table_query = "SELECT name FROM sqlite_master WHERE type='table';"
        # Pragma schema queries come in form of:
        # list of (cid, name, type, notnull, dflt_value, key) per column
        schema_query = "PRAGMA table_info(dir_ancestor)"
        expect = [
            # Expected schema is list for every column with tuple of:
            # (num: int, name: str, dtype: str, nullable: bool, prime_key: bool)
            # Bools are represented as 0|1, but python evaluates them as False|True
            (0, "dir_id", "INTEGER", True, None, 1),
            (1, "ancestor_id", "INTEGER", 1, None, 2),
            (2, "depth", "INTEGER", True, None, False),
        ]
        # Act
        with base_dbconn as db:
            DirRepo.create_dir_ancestor_table(db)
            with db.connect() as conn:
                cursor = conn.cursor()

                # Assert
                cursor.execute(table_query)
                tables = cursor.fetchall()
                cursor.execute(schema_query)
                schema = cursor.fetchall()

                # Check for table name
                assert ("dir_ancestor",) in tables

                # Check column count
                assert len(schema) == len(expect)

                # Check id column
                assert schema[0] == expect[0]

                # Check path column
                assert schema[1] == expect[1]


class TestInit:
    """Test cases for DirRepo.__init__"""

    def testSetsMembers(self, base_dbconn):
        """__init__ sets member variables correctly"""
        with base_dbconn as db:
            repo = DirRepo(db)
            assert repo.db.path == db.path
            assert repo.db.root == db.root

    def testCallsTableExists(self, base_dbconn):
        """__init__ calls DBConnector.table_exists for dir table"""
        # Arrange
        with base_dbconn as db:
            fn_str = "lib.handler.db_connector.DBConnector.table_exists"
            with patch(fn_str) as mock_fn:
                # Act
                DirRepo(db)
                # Assert
                mock_fn.assert_any_call("dir")
                mock_fn.assert_any_call("dir_ancestor")

    @pytest.mark.parametrize(
        "dir,anc",
        # dir: bool whether dir table exists, # anc: bool whether anc table exists
        # calls: a tuple of whether dir and anc table creation functions are expected called
        [
            (False, False),
            (False, True),
            (True, False),
            (True, True),
        ],
    )
    def testCallsRightCreates(
        self,
        base_dbconn,
        dir,
        anc,
    ):
        """__init__ calls the correct table creation method based on
        whether the table exists.
        The parametrization simply passes all possible combinations of
        dir (dir) and anc (ancestor) table existence.
        Then the expected calls are the inverse of whether the table exists.
        """
        with base_dbconn as db:
            with db.connect() as conn:
                if dir:
                    conn.execute("CREATE TABLE dir (id INTEGER PRIMARY KEY, path TEXT)")
                    conn.execute("INSERT INTO dir (path) VALUES ('dir')")
                if anc:
                    q = "CREATE TABLE dir_ancestor (id INTEGER PRIMARY KEY, path TEXT)"
                    conn.execute(q)
                    conn.execute("INSERT INTO dir_ancestor (path) VALUES ('anc')")
                conn.commit()
            with (
                patch(f"{MOD_REPO}.create_dir_table") as mock_dir,
                patch(f"{MOD_REPO}.create_dir_ancestor_table") as mock_anc,
            ):
                DirRepo(db)
                assert mock_dir.called == (not dir)
                assert mock_anc.called == (not anc)


class TestInsertUtils:
    def testInsertDir(self, base_repo):
        """DirRepo.insert_into_dir() inserts correct records & returns correct id.
        Includes absolute and relative to root paths to check they get normalized.
        Also includes duplicate paths to ensure they don't get added twice."""
        ids = []
        with base_repo as repo:
            root = repo.db.root
            ids.append(repo.insert_dir(f"{root}/a"))
            ids.append(repo.insert_dir("a/b"))
            ids.append(repo.insert_dir("a/b"))
            ids.append(repo.insert_dir("a/b/c"))
            ids.append(repo.insert_dir("f/g"))
            ids.append(repo.insert_dir(f"{root}/f"))
            ids.append(repo.insert_dir(f"{root}/f"))
            with repo.db.connect() as conn:
                rows = conn.execute("SELECT * FROM dir").fetchall()
        assert len(rows) == 5
        assert rows[0] == (1, "a")
        assert rows[1] == (2, "a/b")
        assert rows[2] == (3, "a/b/c")
        assert rows[3] == (4, "f/g")
        assert rows[4] == (5, "f")
        assert ids == [1, 2, 2, 3, 4, 5, 5]

    # TODO: Improve test coverage
    def testInsertDirRaise(self, base_repo):
        """DirRepo.insert_into_dir() raises ValueError for invalid paths."""
        with base_repo as repo:
            with pytest.raises(DBPathOutsideTargetError):
                repo.insert_dir(repo.db.root.parent)

    def testInsertDirAncestorValues(self, base_repo):
        """DirRepo.insert_dir_ancestor() inserts correct records."""
        with base_repo as repo:
            expect = [(1, 0, 1), (2, 1, 2), (3, 0, 1)]
            repo.insert_dir_ancestor(expect)
            with repo.db.connect() as conn:
                rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
            assert rows == expect

    def testInsertDirAncestorDupes(self, base_repo):
        """DirRepo.insert_dir_ancestor() doesn't add duplicate rows to dir_ancestor"""
        dupe_row = (1, 1, 1)
        with base_repo as repo:
            repo.insert_dir_ancestor([dupe_row, dupe_row, dupe_row])
            with repo.db.connect() as conn:
                rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
            assert len(rows) == 1
            assert rows[0] == dupe_row


class TestAdd:
    def testNoAncestorDirs(self, base_repo):
        """DirRepo.add() adds a directory without ancestors correctly."""
        with base_repo as repo:
            # Dir a @ root level
            dir = Dir(path=repo.db.root / "a")
            repo.add(dir)
            assert dir.id == 1
            with repo.db.connect() as conn:
                rows = conn.execute("SELECT * FROM dir").fetchall()
                assert len(rows) == 1
                assert rows[0] == (1, "a")
                rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
                assert len(rows) == 1
                assert rows[0] == (1, 1, 0)
            # Dir b @ root level
            dir = Dir(path="b")
            repo.add(dir)
            assert dir.id == 2
            with repo.db.connect() as conn:
                rows = conn.execute("SELECT * FROM dir").fetchall()
                assert len(rows) == 2
                assert rows[1] == (2, "b")
                rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
                assert len(rows) == 2
                assert rows[1] == (2, 2, 0)

    def testDeepNesting(self, base_repo):
        """
        DirRepo.add() adds a directory with ancestors correctly.
            - dir table has a, a/b, a/b/c, a/b/c/d as individual records
            - they all have 1...4 as their id respectively
            - all other dependant columns are as expected
            - dir_ancestor table has all the expected foreign keys
            - dir_ancestor records are in right order
        """
        with base_repo as repo:
            root = repo.db.root
            # First arrange expected returned Dir list
            dira = Dir(path=root / "a", id=1)
            dirb = Dir(path=root / "a/b", id=2)
            dirc = Dir(path=root / "a/b/c", id=3)
            dird = Dir(path=root / "a/b/c/d", id=4)
            dirs = [dira, dirb, dirc, dird]

            # Act on the repo with add
            real_dirs = repo.add(Dir(path=(root / "a/b/c/d")))

            # Assert that the returned list is as expected
            assert real_dirs == dirs, f"Expected Dir list: {dirs}, got {real_dirs}"
            # Assert the dir & dir_ancestor tables are as expected
            d_rows = [(1, "a"), (2, "a/b"), (3, "a/b/c"), (4, "a/b/c/d")]
            da_rows = [(1, 1, 0)]
            da_rows += [(2, 2, 0), (2, 1, 1)]
            da_rows += [(3, 3, 0), (3, 2, 1), (3, 1, 2)]
            da_rows += [(4, 4, 0), (4, 3, 1), (4, 2, 2), (4, 1, 3)]
            with repo.db.connect() as conn:
                real_drows = conn.execute("SELECT * FROM dir").fetchall()
                real_da_rows = conn.execute("SELECT * FROM dir_ancestor").fetchall()
            assert real_drows == d_rows
            assert real_da_rows == da_rows


class TestSelectUtils:
    """Test SELECT query utility methods"""

    @pytest.mark.parametrize("path,expect", [("f", 6), ("a/b/c", 3), ("f/g", 7)])
    def testDirWherePath(self, test_repo, path, expect):
        """Test that select_dir_where_path returns expected id for given path."""
        with test_repo as repo:
            assert repo.select_dir_where_path(path)[0] == expect

    def testDirWherePathNoExist(self, base_repo):
        """Test that select_dir_where_path returns None for paths that dont exist in dir."""
        # Base repo has no dir records so anything we select shouldnt exist
        with base_repo as repo:
            assert repo.select_dir_where_path("foobar") is None
            repo.add(Dir(path="foobaz"))
            assert repo.select_dir_where_path("foobar") is None

    @pytest.mark.parametrize("id,expect", [(6, "f"), (3, "a/b/c"), (7, "f/g")])
    def testDirWhereId(self, test_repo, id, expect):
        """DirRepo.select_dir_where_id() returns correct row from dir"""
        with test_repo as repo:
            assert repo.select_dir_where_id(id)[0] == id
            assert repo.select_dir_where_id(id)[1] == expect

    def testDirWhereIdNoExist(self, base_repo):
        """DirRepo.select_dir_where_id returns None on non-existing ids in dir."""
        with base_repo as repo:
            assert repo.select_dir_where_id(42) is None
            repo.add(Dir(path="foobar/blah/foobaz"))
            assert repo.select_dir_where_id(4) is None


class TestSelectAncestor:
    """DirRepo.select_ancestors_where_{path,id} method tests"""

    def testWherePath(self, test_repo):
        """
        Test DirRepo.select_dirs_where_ancestor() with different paths and depths.

        Verifies:
        - Ancestor rows for 'a/b/c' with no depth limit.
        - Ancestor rows for 'a/b/c' with a high depth limit.
        - Ancestor rows for 'a/b/c' with depth=1.
        - Ancestor row for 'f/g'.
        - Empty result for top-level directory 'f'.
        - Consistency between absolute and relative paths.
        """
        with test_repo as repo:
            fn = repo.select_ancestors_where_path
            assert same_rows(fn("a/b/c"), [(2,), (1,)])
            assert same_rows(fn("a/b/c", depth=99), [(2,), (1,)])
            assert same_rows(fn("a/b/c", depth=1), [(2,)])
            assert same_rows(fn("f/g"), [(6,)])
            assert same_rows(fn("f"), [])

    def testWhereId(self, test_repo):
        """
        Test DirRepo.select_dirs_where_ancestor() with different IDs and depths.

        Verifies:
        - Ancestor rows for ID 3 with no depth limit.
        - Ancestor rows for ID 3 with a high depth limit.
        - Ancestor rows for ID 3 with depth=1.
        - Ancestor row for ID 7.
        - Empty result for top-level directory with ID 6.
        """
        with test_repo as repo:
            fn = repo.select_ancestors_where_id
            assert same_rows(fn(3), [(2,), (1,)])
            assert same_rows(fn(3, depth=99), [(2,), (1,)])
            assert same_rows(fn(3, depth=1), [(2,)])
            assert same_rows(fn(7), [(6,)])
            assert same_rows(fn(6), [])


class TestSelectDescendants:
    """DirRepo.select_descendants_where_{path,id} method tests"""

    def testWherePath(self, test_repo):
        """
        Test DirRepo.select_descendants_where_path() with different paths and depths.

        Verifies:
        - Descendant row IDs for path 'a/b'.
        - Descendant rows for 'a' with no depth limit.
        - Same result for 'a' with a high depth limit.
        - Descendant rows for 'a' with depth=1.
        - Descendant rows for 'f'.
        - Empty result for leaf directory 'a/b/c'.
        - Empty result for another leaf directory 'a/d'.
        """
        with test_repo as repo:
            fn = repo.select_descendants_where_path
            assert same_rows(fn("a/b"), [(3,)])
            assert same_rows(fn("a"), [(2,), (4,), (5,), (3,)])
            assert same_rows(fn("a", depth=99), [(2,), (4,), (5,), (3,)])
            assert same_rows(fn("a", depth=1), [(2,), (4,), (5,)])
            assert same_rows(fn("f"), [(7,), (8,)])
            assert same_rows(fn("a/b/c"), [])
            assert same_rows(fn("a/d"), [])

    def testWhereId(self, test_repo):
        """
        Test DirRepo.select_descendants_where_id() with different IDs and depths.

        Verifies:
        - Descendant row IDs for ID 2.
        - Descendant rows for ID 1 with no depth limit.
        - Same result for ID 1 with a high depth limit.
        - Descendant rows for ID 1 with depth=1.
        - Empty result for leaf directory with ID 4.
        """
        with test_repo as repo:
            fn = repo.select_descendants_where_id
            assert same_rows(fn(2), [(3,)])
            assert same_rows(fn(1), [(2,), (4,), (5,), (3,)])
            assert same_rows(fn(1, depth=99), [(2,), (4,), (5,), (3,)])
            assert same_rows(fn(1, depth=1), [(2,), (4,), (5,)])
            assert same_rows(fn(4), [])


class TestGetOne:
    def testPrefersId(self, test_repo):
        """Prioritizes id over all other args."""
        with test_repo as repo:
            dir = repo.getone(id=1, path="a/b/c", dir=Dir(id=6, path="f/h"))
            assert dir == Dir(id=1, path=repo.db.root / "a")

    def testPrefersPath(self, test_repo):
        """Prioritizes path arg over all others when id is None."""
        with test_repo as repo:
            dir = repo.getone(path="a/b/c", dir=Dir(id=6, path="f/h"))
            assert dir == Dir(id=3, path=repo.db.root / "a/b/c")

    def testPrefersDirId(self, test_repo):
        """Prioritizes dir arg's id over its path member when id & path are None."""
        with test_repo as repo:
            dir = repo.getone(dir=Dir(id=6, path="f/h"))
            assert dir == Dir(id=6, path=repo.db.root / "f")

    def testPrefersDirPath(self, test_repo):
        """Prioritizes dir arg's path only when all other args are None."""
        with test_repo as repo:
            dir = repo.getone(dir=Dir(path="f/h"))
            assert dir == Dir(id=8, path=repo.db.root / "f/h")

    def testRaises(self, base_repo):
        """Raises ValueError when no args and
        DBPathOutsideTargetError when path outside repo are given."""
        with base_repo as repo:
            with pytest.raises(ValueError):
                repo.getone()
            with pytest.raises(DBPathOutsideTargetError):
                repo.getone(path="/not/in/repo")

    def testDirNotFound(self, test_repo):
        """Returns None when no dir is found."""
        with test_repo as repo:
            assert repo.getone(path="noexist") is None
            assert repo.getone(id=42) is None

    def testRelAndAbsPathSame(self, test_repo):
        """Returns same result for relative and absolute paths."""
        with test_repo as repo:
            root = repo.db.root
            assert repo.getone(path=root / "a/b/c") == repo.getone(path="a/b/c")
            assert repo.getone(path=root / "f/g") == repo.getone(path="f/g")


class TestGetAncestors:
    """DirRepo.get_ancestors() method tests"""

    def testPrefersId(self, test_repo):
        """Prioritizes id over all other args."""
        with test_repo as repo:
            dir = repo.get_ancestors(id=1, path="a/b/c", dir=Dir(id=6, path="f/h"))
            assert dir == []

    def testPrefersPath(self, test_repo):
        """Prioritizes path arg over all others when id is None."""
        with test_repo as repo:
            expect = Dirs(repo.db.root)
            dir = repo.get_ancestors(path="a/b/c", dir=Dir(id=6, path="f/h"))
            assert dir == [expect.b, expect.a]

    def testPrefersDirId(self, test_repo):
        """Prioritizes dir arg's id over its path member when id & path are None."""
        with test_repo as repo:
            dir = repo.get_ancestors(dir=Dir(id=6, path="f/h"))
            assert dir == []

    def testPrefersDirPath(self, test_repo):
        """Prioritizes dir arg's path only when all other args are None."""
        with test_repo as repo:
            dir = repo.get_ancestors(dir=Dir(path="f/h"))
            expect = Dirs(repo.db.root)
            assert dir == [expect.f]

    def testRaises(self, base_repo):
        """Raises ValueError when no args and
        DBPathOutsideTargetError when path outside repo are given."""
        with base_repo as repo:
            with pytest.raises(ValueError):
                repo.get_ancestors()
            with pytest.raises(DBPathOutsideTargetError):
                repo.get_ancestors(path="/not/in/repo")

    def testAbsAndRelPathsEqual(self, test_repo):
        """Returns same result for relative and absolute paths."""
        with test_repo as repo:
            expect = Dirs(repo.db.root)
            ancestors = repo.get_ancestors(path="a/b/c")
            assert ancestors == [expect.b, expect.a]
            ancestors = repo.get_ancestors(path="f/g")
            assert ancestors == [expect.f]

    def testDepthWorks(self, test_repo):
        """Returns correct descendants with depth limit."""
        with test_repo as repo:
            expect = Dirs(repo.db.root)
            dir = repo.get_ancestors(path="a/b/c", depth=1)
            assert dir == [expect.b]
            dir = repo.get_descendants(path="f", depth=0)
            assert dir == []


class TestGetDescendants:
    """DirRepo.get_descendants() method tests"""

    def testPrefersId(self, test_repo):
        """Prioritizes id over all other args and returns test_repo dirs in order."""
        with test_repo as repo:
            dir = repo.get_descendants(id=1, path="a/b/c", dir=Dir(id=6, path="f/h"))
            expect = Dirs(repo.db.root)
            assert dir == [expect.b, expect.d, expect.e, expect.c]

    def testPrefersPath(self, test_repo):
        """Prioritizes path arg over all others when id is None and returns expected empty list."""
        with test_repo as repo:
            dir = repo.get_descendants(path="a/b/c", dir=Dir(id=6, path="f/h"))
            assert dir == []

    def testPrefersDirId(self, test_repo):
        """Prioritizes dir arg's id over its path member when id & path are None."""
        with test_repo as repo:
            dir = repo.get_descendants(dir=Dir(id=6, path="f/h"))
            expect = Dirs(repo.db.root)
            assert dir == [expect.g, expect.h]

    def testPrefersDirPath(self, test_repo):
        """Prioritizes dir arg's path only when all other args are None."""
        with test_repo as repo:
            dir = repo.get_descendants(dir=Dir(path="f/h"))
            assert dir == []

    def testRaises(self, base_repo):
        """Raises ValueError when no args and
        DBPathOutsideTargetError when path outside repo are given."""
        with base_repo as repo:
            with pytest.raises(ValueError):
                repo.get_descendants()
            with pytest.raises(DBPathOutsideTargetError):
                repo.get_descendants(path="/not/in/repo")

    def testAbsAndRelPathsEqual(self, test_repo):
        """Returns same result for relative and absolute paths."""
        with test_repo as repo:
            expect = Dirs(repo.db.root)
            ancestors = repo.get_descendants(path="a")
            assert ancestors == [expect.b, expect.d, expect.e, expect.c]
            ancestors = repo.get_descendants(path="f")
            assert ancestors == [expect.g, expect.h]

    def testDepthWorks(self, test_repo):
        """Returns correct descendants with depth limit."""
        with test_repo as repo:
            expect = Dirs(repo.db.root)
            dir = repo.get_descendants(path="a", depth=1)
            assert dir == [expect.b, expect.d, expect.e]
            dir = repo.get_descendants(path="f", depth=0)
            assert dir == []
