from contextlib import contextmanager
import os
from pathlib import PurePath as PP
import pytest
import sqlite3 as sql
import tempfile
from unittest.mock import patch, MagicMock

from lib.handler.db_connector import (
    DBConnector,
    DBConnectorError,
    DBNotInDirError,
    DBFileOccupiedError,
    DBRootNotDirError,
)
from lib.model.dir import Dir

MOD_BASE = "lib.handler.db_connector.DBConnector"


@contextmanager
def temp_dir_context():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield PP(temp_dir)


@pytest.fixture
@contextmanager
def bare_db():
    with temp_dir_context() as dp:
        path_root = dp / "root"
        path_db = dp / "test.db"
        os.mkdir(path_root)
        db = DBConnector(path_db, path_root)
        yield db


@pytest.fixture
def mock_db_conn():
    with patch.object(DBConnector, "__init__", return_value=None) as mock:
        mock = DBConnector()  # type: ignore
        mock.path = PP("/test/.scout.db")
        mock.root = PP("/test/root")
        yield mock


# TODO: Give comment including filetree contents
@pytest.fixture
@contextmanager
def fake_files_dir():
    """Creates below filetree fixture
    temp_dir/
    ├── dir/ # Empty directory for root property to point to
    ├── test.txt # Non-db file
    ├── test.db # Sqlite db file, but not a scout db file
    ├── base.scout.db # A bare scout db file with root pointing to temp_dir/dir
    └── noroot.db # A scout db file with no root property
    """
    with temp_dir_context() as temp_dir:
        os.mkdir(temp_dir / "dir")
        with open(temp_dir / "test.txt", "w") as f:
            f.write("Hello World!")
        with sql.connect(temp_dir / "test.db") as conn:
            conn.execute("CREATE TABLE foobar (id TEXT PRIMARY KEY, txt TEXT);")
            conn.execute("INSERT INTO foobar (id, txt) VALUES ('foo', 'bar');")
            conn.commit()
        with sql.connect(temp_dir / "base.scout.db") as conn:
            q = "CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);"
            conn.execute(q)
            q = f"INSERT INTO fs_meta (property, value) VALUES ('root', '{temp_dir}/dir');"
            conn.execute(q)
            conn.commit()
        with sql.connect(temp_dir / "noroot.db") as conn:
            q = "CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);"
            conn.execute(q)
            q = "INSERT INTO fs_meta (property, value) VALUES ('noroot', '/a/b');"
            conn.execute(q)
            conn.commit()
        yield temp_dir


class TestFixtures:
    """Tests fixtures used in DBConnector tests."""

    def testFakeFilesDirFs(self, fake_files_dir):
        with fake_files_dir as dp:
            assert os.path.isdir(dp / "dir")
            assert os.path.exists(dp / "test.txt")
            assert os.path.exists(dp / "test.db")
            assert os.path.exists(dp / "base.scout.db")
            with open(dp / "test.txt") as f:
                assert f.read() == "Hello World!"
            with sql.connect(dp / "test.db") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM foobar;")
                assert cursor.fetchone() == ("foo", "bar")
            with sql.connect(dp / "base.scout.db") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM fs_meta;")
                assert cursor.fetchone() == ("root", str(dp / "dir"))
            with sql.connect(dp / "noroot.db") as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM fs_meta;")
                assert cursor.fetchone() == ("noroot", "/a/b")

    def testFakeFilesDirClean(self, fake_files_dir):
        """fake_files_dir dir and contents should be cleaned after context."""
        with fake_files_dir as dp:
            assert os.path.exists(dp)
        assert not os.path.exists(dp)

    def testBareDb(self, bare_db):
        with bare_db as db:
            root = PP()
            with sql.connect(db.path) as conn:
                c = conn.cursor()
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                root = PP(c.fetchone()[0])
            assert db.root == root
            assert db.path == db.root.parent / "test.db"


class TestErrors:
    """Test this module's custom error classes."""

    def testDBNotInDir(self):
        """Test type and message of DBNotInDir error."""
        e = None
        with pytest.raises(DBNotInDirError) as e:
            raise DBNotInDirError("foobar")
        assert isinstance(e.value, DBNotInDirError)
        assert isinstance(e.value, DBConnectorError)
        assert "foobar" in str(e.value)
        assert "foobar" in e.value.message

    def testDBFileOccupied(self):
        """Test type and message of DBFileOccupied error."""
        e = None
        with pytest.raises(DBFileOccupiedError) as e:
            raise DBFileOccupiedError("foobar")
        assert isinstance(e.value, DBFileOccupiedError)
        assert isinstance(e.value, DBConnectorError)
        assert "foobar" in str(e.value)
        assert "foobar" in e.value.message

    def testDBRootNotDir(self):
        """Test type and message of DBRootNotDir error."""
        e = None
        with pytest.raises(DBRootNotDirError) as e:
            raise DBRootNotDirError("foobar")
        assert isinstance(e.value, DBRootNotDirError)
        assert isinstance(e.value, DBConnectorError)
        assert "foobar" in str(e.value)
        assert "foobar" in e.value.message


class TestInitValid:
    """Tests the validation of the __init__ arguments only through
    the class methods that get used during argument validation."""

    def testIsDBFile(self, fake_files_dir):
        """Returns true for sqlite3 file, false otherwise"""
        with fake_files_dir as dp:
            assert DBConnector.is_db_file(dp / "test.db")
            assert DBConnector.is_db_file(dp / "base.scout.db")
            assert DBConnector.is_db_file(dp / "noroot.db")
            assert not DBConnector.is_db_file(dp / "test.txt")

    def testIsScoutDBFile(self, fake_files_dir):
        """Correctly returns bool for:
        True: Is both a db file and has fs_meta table
        False:
            - Not a db file (e.g. a txt file saying Hello World!)
            - A db file but doesn't have fs_meta table
        """
        with fake_files_dir as dp:  # dp = dir path to fake temp directory
            assert DBConnector.is_scout_db_file(dp / "base.scout.db")
            assert not DBConnector.is_scout_db_file(dp / "test.db")
            assert not DBConnector.is_scout_db_file(dp / "test.txt")
            assert not DBConnector.is_scout_db_file(dp / "noroot.db")

    @pytest.mark.parametrize(
        "path, expect",
        [
            ("base.scout.db", PP("base.scout.db")),
            (PP("base.scout.db"), PP("base.scout.db")),
        ],
        ids=["#1", "#2"],
    )
    def testValidArgPathReturn(self, fake_files_dir, path, expect):
        """
        DBConnector.validate_arg_root should:
        1. Return a PP object when a scout db file path given as str
        2. Return a PP object when a scout db file path given as PP
        """
        fn = DBConnector.validate_arg_path
        with fake_files_dir as dp:
            assert fn(f"{dp}/{path}") == PP(dp / expect)

    @pytest.mark.parametrize(
        "path, raises",
        [
            (1, TypeError),
            (None, TypeError),
            ("not/there", DBNotInDirError),
            ("test.db", DBFileOccupiedError),
            ("test.txt", DBFileOccupiedError),
            ("noroot.db", DBFileOccupiedError),
        ],
        ids=["#1", "#2", "#3", "#4", "#5", "#6"],
    )
    def testValidArgPathRaise(self, fake_files_dir, path, raises):
        """
        DBConnector.validate_arg_path should:
        1. Raise a TypeError when neither a str nor PP is given
        2. Raise a TypeError when None is given
        3. Raise a DBNotInDirError when root's parent is not a valid directory on FS
          - Note, sometimes we need a non-existing path to start a new DB file
        4. Raise a DBFileOccupiedError when path exists AND is NOT a scout db file
          - Dangerous error that could cause data loss outside of program scope
        5. Raise a DBFileOccupiedError when path exists and is not a db file at all
        6. Raise a DBFileOccupiedError when path exists and is db file,
           but doesnt contain the fs_meta.root marker
        """
        with fake_files_dir as dp:
            with pytest.raises(raises):
                DBConnector.validate_arg_path(dp / path)

    def testValidArgRootReturn(self, fake_files_dir):
        """
        DBConnector.validate_arg_root returns these when:
        - A valid path to the parent of the path arg when no/None root arg given
        - A valid path to a valid dir when a valid dir path arg given as PurePath
        - A valid path to a valid dir when a valid dir path arg given as str
        """
        fn = DBConnector.validate_arg_root
        basename = "base.scout.db"
        with fake_files_dir as dp:
            assert fn(dp / basename, None) == dp  # Default to path.parent
            assert fn(dp / basename, dp / "dir") == dp / "dir"
            assert fn(dp / basename, str(dp / "dir")) == dp / "dir"

    def testValidArgRootRaises(self, fake_files_dir):
        """
        DBConnector.validate_arg_root raises when:
        - TypeError when root arg given not of type PurePath, str, or None
        - DBRootNotDirError when root arg points to non-existing path
        - DBRootNotDirError when root arg points to non-dir path (txt file)
        - DBRootNotDirError when root arg points to non-dir path (scout file)
        """
        fn = DBConnector.validate_arg_root
        basename = "base.scout.db"
        with fake_files_dir as dp:
            with pytest.raises(TypeError):
                fn(dp / basename, 1)  # type: ignore
            with pytest.raises(DBRootNotDirError):
                fn(dp / basename, "/not/there")
            with pytest.raises(DBRootNotDirError):
                fn(dp / basename, dp / "test.txt")
            with pytest.raises(DBRootNotDirError):
                fn(dp / basename, dp / basename)


class TestInitSql:
    """Tests helper class methods used by
    __init__ to initialize or connect to the scout db file."""

    def testInitDbCreates(self, fake_files_dir):
        """The init_db class method should create:
        - A new sqlite file with the fs_meta table
        - First column is property TEXT PRIMARY KEY
        - Second column is value TEXT
        - Only row has values ('root', '/a/b')"""
        with fake_files_dir as dp:
            path = dp / "init.db"
            DBConnector.init_db(path, PP("/a/b"))
            assert DBConnector.is_db_file(path)
            assert DBConnector.is_scout_db_file(path)
            with sql.connect(path) as conn:
                c = conn.cursor()
                # Query & assert fs_meta table exists
                c.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = c.fetchall()
                assert ("fs_meta",) in tables
                # Query & assert fs_meta table has correct columns
                c.execute("PRAGMA table_info(fs_meta);")
                columns = c.fetchall()
                assert columns[0][1] == "property"
                assert columns[1][1] == "value"
                assert columns[0][2] == columns[1][2] == "TEXT"
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                assert c.fetchone()[0] == "/a/b"

    def testInitDbRaisesOnChange(self, fake_files_dir):
        """
        DBConnector.init_db should:
            - Raise an IntegrityError if the db file already has a root property
            - Not change the db file's contents
            - Not change the root property's value in the fs_meta table
        """
        with fake_files_dir as dp:
            # First read the old contents of the scout db file @ base.scout.db
            old_contents = b""
            new_contents = b""
            with open(dp / "base.scout.db", "rb") as f:
                old_contents = f.read()
            # Now that we know the old file contents,
            # check that sqlite's integryity error is raised when
            # trying to init the db file again.
            with pytest.raises(sql.IntegrityError):
                DBConnector.init_db(dp / "base.scout.db", PP("/f/g"))
            # With the init_db call, check binary contents of the file afterwards
            with open(dp / "base.scout.db", "rb") as f:
                new_contents = f.read()
            # Assert the binary contents did not change
            assert old_contents == new_contents
            # Now finally check for the root property's value column for old value
            with sql.connect(dp / "base.scout.db") as conn:
                c = conn.cursor()
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                assert c.fetchone()[0] == str(dp / "dir")

    @pytest.mark.parametrize(
        "path, root",
        [("b.db", "/a/b"), ("g.db", "/f/g"), ("c.db", "/a/b/c")],
    )
    def testReadRoot(self, fake_files_dir, path, root):
        """DBConnector.read_root returns:
        - Correct root property value from fs_meta table
        - Returns only PP values
        """
        with fake_files_dir as dp:
            path = dp / path
            DBConnector.init_db(path, root)
            assert DBConnector.read_root(path) == PP(root)
            assert isinstance(DBConnector.read_root(path), PP)

    def testReadRootRaisesNoRoot(self, fake_files_dir):
        """DBConnector.read_root raises an error when root property is not found.
        True for both no fs_meta and no root property in fs_meta."""
        with fake_files_dir as dp:
            # Check raises when no fs_meta table
            with pytest.raises(sql.OperationalError):
                DBConnector.read_root(dp / "test.db")
            # Check raises when no 'root' in property column
            with sql.connect(dp / "test.db") as conn:
                c = conn.cursor()
                q = "CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);"
                c.execute(q)
                conn.commit()
            with pytest.raises(sql.OperationalError):
                DBConnector.read_root(dp / "test.db")


class TestInit:
    """Tests init method only."""

    def testValidateArgFuncsCalled(self):
        """
        Should call validate_arg_path and validate_arg_root.
        We mock os.path.exists and init_db to eliminate side effects.
        """
        path, root = "path", "root"
        with (
            patch(f"{MOD_BASE}.validate_arg_path", return_value=path) as mock_path,
            patch(f"{MOD_BASE}.validate_arg_root", return_value=root) as mock_root,
            patch("os.path.exists", return_value=False),
            patch(f"{MOD_BASE}.init_db"),
            patch(f"{MOD_BASE}.is_scout_db_file", return_value=False),
        ):
            DBConnector(path, root)
            mock_path.assert_called_once_with(path)
            mock_root.assert_called_once_with(path, root)

    def testValidateArgPathSets(self):
        """Should set the path attribute with the return of validate_arg_path.
        Note that to ensure no other side_effects,
        we mock methods that raise errors.
        Mocks take the execution path to
        assume file doesnt exist and leads to init_db while checking path member.
        """
        path, root = "path", "root"
        with (
            patch(f"{MOD_BASE}.validate_arg_path", return_value=path),
            patch(f"{MOD_BASE}.validate_arg_root", return_value=root),
            patch("os.path.exists", return_value=False),
            patch(f"{MOD_BASE}.init_db"),
            patch(f"{MOD_BASE}.is_scout_db_file", return_value=False),
        ):
            db = DBConnector(path, root)
            assert db.path == path

    def testValidateArgRootSetsByNoFile(self):
        """
        Mocks validate_arg_{path,root}, is_scout_db_file & read_root to
        check that root is set by validate_arg_root when
        is_scout_db_file is FALSE, aka when you CANT read the root property from db.
        """
        path, root, override = "path", "root", "override"
        with (
            patch(f"{MOD_BASE}.validate_arg_path", return_value=path),
            patch(f"{MOD_BASE}.validate_arg_root", return_value=root),
            patch("os.path.exists", return_value=False),
            patch(f"{MOD_BASE}.init_db"),
            patch(f"{MOD_BASE}.is_scout_db_file", return_value=False),
            patch(f"{MOD_BASE}.read_root", return_value=override),
        ):
            db = DBConnector(path, root)
            assert db.root == root

    def testInitDBCalled(self):
        """
        Mocks all methods used by __init__, including os.path.exists to
        check whether init_db is called with members path & root when
        path is empty.
        Init_db raises when sqlite can't read or write to the file so
        there's no need to validate that functionality.
        And the TestInitSql class tests init_db functionality.
        We only care that initdb gets called in the right conditions.
        """
        path, root, override = "path", "root", "override"
        with (
            patch(f"{MOD_BASE}.validate_arg_path", return_value=path),
            patch(f"{MOD_BASE}.validate_arg_root", return_value=root),
            patch("os.path.exists", return_value=False) as mock_exist,
            patch(f"{MOD_BASE}.init_db") as mock_initdb,
            patch(f"{MOD_BASE}.is_scout_db_file", return_value=False),
            patch(f"{MOD_BASE}.read_root", return_value=override),
        ):
            DBConnector(path, root)
            mock_exist.assert_called_once_with(path)
            mock_initdb.assert_called_once_with(path, root)

    def testReadRootOverridesWhenFile(self):
        """
        Mocks validate_arg_{path,root}, is_scout_db_file & read_root to
        check that root is overriden by read_root when
        is_scout_db_file is TRUE, aka when you CAN read the root property from db.
        """
        path, root, override = "path", "root", "override"
        with (
            patch(f"{MOD_BASE}.validate_arg_path", return_value=path),
            patch(f"{MOD_BASE}.validate_arg_root", return_value=root),
            patch("os.path.exists", return_value=True) as mock_exist,
            patch(f"{MOD_BASE}.init_db") as mock_initdb,
            patch(f"{MOD_BASE}.is_scout_db_file", return_value=True) as mock_scout,
            patch(f"{MOD_BASE}.read_root", return_value=override) as mock_read,
        ):
            db = DBConnector(path, root)
            mock_exist.assert_called_once_with(path)
            mock_initdb.assert_not_called()
            mock_scout.assert_called_once_with(path)
            mock_read.assert_called_once_with(path)
            assert db.root == override

    def testRaisesTypeErrors(self, fake_files_dir):
        """Raises type errors on:
        1. path is not a PurePath or str: TypeError
        2. root is not a PurePath or str: TypeError"""
        with fake_files_dir as dp:
            with pytest.raises(TypeError):
                DBConnector(1)  # type: ignore
            with pytest.raises(TypeError):
                DBConnector(dp / ".scout.db", 1)  # type: ignore

    @pytest.mark.parametrize(
        "path, root, raises",
        [
            ("doesnt/exist", None, DBNotInDirError),
            (".scout.db", "doesnt/exist", DBRootNotDirError),
            ("test.txt", "dir", DBFileOccupiedError),
            ("test.db", "dir", DBFileOccupiedError),
        ],
        ids=["#1", "#2", "#3", "#4"],
    )
    def testRaises(self, fake_files_dir, path, root, raises):
        """
        These circumstances should raise an error.
        1. parent of path is not a dir: FileNotFoundError
        2. root is not a dir: DBRootNotDirError
        3. path exists and is not a sqlite file: DBFileOccupiedError
        4. path exists and is not a scout db file, but is sqlite: DBFileOccupiedError
        """
        with fake_files_dir as dp:
            with pytest.raises(raises):
                root = dp / root if root else None
                DBConnector(dp / path, root)

    def testSuccessInitDB(self, fake_files_dir):
        """
        Should successfully initialize the DBConnector object with
        a newly initialized scout db file.
        """
        with fake_files_dir as dp:
            path, root = dp / "new.db", dp / "dir"
            db = DBConnector(path, root)
            assert db.path == path
            assert db.root == root
            with sql.connect(path) as conn:
                c = conn.cursor()
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                assert c.fetchone()[0] == str(root)

    def testSuccessReadRoot(self, fake_files_dir):
        """
        Should successfully initialize the DBConnector object with
        an existing scout db file.
        """
        with fake_files_dir as dp:
            path, root = dp / "base.scout.db", dp / "dir"
            db = DBConnector(path, root)
            assert db.path == path
            assert db.root == root
            with sql.connect(path) as conn:
                c = conn.cursor()
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                assert c.fetchone()[0] == str(root)


class TestPathHelpers:
    def testNormDiffTypeInSameOut(self, mock_db_conn):
        """normalize_path should return same output for different input types."""
        fn = mock_db_conn.normalize_path
        assert fn("a/b") == fn(PP("a/b"))
        assert fn("/test/root/a/b") == fn(PP("/test/root/a/b"))
        assert fn(Dir("a/b")) == fn(PP("a/b"))
        assert fn(Dir("/test/root/a/b")) == fn(PP("/test/root/a/b"))

    # TODO: Stick to one input type and use test case to ensure diff types return same output
    @pytest.mark.parametrize(
        "path, expect",
        [
            ("a/b", PP("a/b")),  # 1
            (PP("a/b/c"), PP("a/b/c")),  # 2
            ("/test/root/a/b/c", PP("a/b/c")),  # 3
            (PP("/test/root/f/g"), PP("f/g")),  # 4
            ("", PP("")),  # 5
            (PP(""), PP("")),  # 6
            ("/test/root", PP("")),  # 7
            (PP("/test/root"), PP("")),  # 8
            ("/test/root/a", PP("a")),  # 9
            (PP("/test/root/f"), PP("f")),  # 10
        ],
        ids=["#1", "#2", "#3", "#4", "#5", "#6", "#7", "#8", "#9", "#10"],
    )
    def testNormReturn(self, mock_db_conn, path, expect):
        """
        Test that normalize_path correctly normalizes paths to root stored in db.
        Cases:
        1. Relative paths (str) are returned as is in (PP)
        2. Relative paths (PP) are returned as is in (PP)
        3. Absolute paths w/i root (str) returned relative to root (PP)
        4. Absolute paths w/i root (PP) returned relative to root (PP)
        5. Empty path (str) returns empty path (PP)
        6. Empty path (PP) returns empty path (PP)
        7. Absolute path (str) to root returns empty path (PP)
        8. Absolute path (PP) to root returns empty path (PP)
        9. Absolute path (str) to toplevel dir in root is correct relative path (PP)
        10. Absolute path (PP) to toplevel dir in root is correct relative path (PP)
        """
        assert mock_db_conn.normalize_path(path) == expect

    @pytest.mark.parametrize("path", ["/test", "/out/root", "../a"])
    def testNormRaise(self, mock_db_conn, path):
        """
        Test that normalize_path raises an error when path is not within root.
        While checking for inputs of...
            1. Ancestor paths to root
            2. Parallel to root
            3. Relative ancestor
        """
        with pytest.raises(ValueError):
            mock_db_conn.normalize_path(path)

    @pytest.mark.parametrize(
        "path, expect",
        [
            ("a/b/c", PP("/test/root/a/b/c")),  # 1
            (PP("f/g"), PP("/test/root/f/g")),  # 2
            ("/test/root/a/b", PP("/test/root/a/b")),  # 3
            (PP("/test/root/a/d"), PP("/test/root/a/d")),  # 4
            ("", PP("/test/root")),  # 5
            (PP(""), PP("/test/root")),  # 6
            ("/test/root", PP("/test/root")),  # 7
            (PP("/test/root"), PP("/test/root")),  # 8
            ("/test/root/a", PP("/test/root/a")),  # 9
            (PP("/test/root/f"), PP("/test/root/f")),  # 10
        ],
        ids=["#1", "#2", "#3", "#4", "#5", "#6", "#7", "#8", "#9", "#10"],
    )
    def testDenormReturn(self, mock_db_conn, path, expect):
        """
        Test that normalize_path correctly normalizes paths to root stored in db.
        Cases:
        1. Relative paths (str) are returned as concated to root abs. paths (PP)
        2. Relative paths (PP) are returned as concated to root abs. paths (PP)
        3. Absolute paths w/i root (str) returned as is (PP)
        4. Absolute paths w/i root (PP) returned as is (PP)
        5. Root path (str) returns empty path (PP)
        6. Root path (PP) returns empty path (PP)
        7. Absolute path (str) to root returns same abs. path (PP)
        8. Absolute path (PP) to root returns same abs. path (PP)
        9. Absolute path (str) to toplevel dir in root is correct absolute path (PP)
        10. Absolute path (PP) to toplevel dir in root is correct absolute path (PP)
        """
        assert mock_db_conn.denormalize_path(path) == expect

    @pytest.mark.parametrize("path", ["/test", "/out/root", "../a"])
    def testDenormRaise(self, mock_db_conn, path):
        """
        Test that denormalize_path raises an error when path is not within root.
        1. Ancestor paths to root
        2. Parallel to root
        3. Relative ancestor
        """
        with pytest.raises(ValueError):
            mock_db_conn.denormalize_path(path)

    @pytest.mark.parametrize(
        "path,exp",
        [
            ("a/b/c", ["a", "a/b", "a/b/c"]),
            ("f/h", ["f", "f/h"]),
            ("f", ["f"]),
            ("", []),
        ],
    )
    def testAncPathsRel(self, mock_db_conn, path, exp):
        """Method ancestor_paths should return a list of all ancestor paths to
        a given path in order of root to given path."""
        ppath, expect = PP(path), [PP(path) for path in exp]
        assert mock_db_conn.ancestor_paths(path) == expect
        assert mock_db_conn.ancestor_paths(ppath) == expect
        root = mock_db_conn.root
        path, ppath = str(root / path), root / ppath
        assert mock_db_conn.ancestor_paths(path) == expect
        assert mock_db_conn.ancestor_paths(ppath) == expect


class TestConnect:
    def testConnect(self, bare_db):
        with bare_db as db:
            with db.connect() as conn:
                c = conn.cursor()
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                root = PP(c.fetchone()[0])
                assert db.root == root
                assert db.path == root.parent / "test.db"

    def testConnectSQLExec(self, bare_db):
        with bare_db as db:
            with db.connect() as conn:
                conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, txt TEXT);")
                conn.execute("INSERT INTO test (txt) VALUES ('Hello World!');")
                conn.execute("INSERT INTO test (txt) VALUES ('foobar');")
                conn.commit()
            with sql.connect(db.path) as conn:
                c = conn.cursor()
                c.execute("SELECT * FROM test;")
                assert c.fetchall() == [(1, "Hello World!"), (2, "foobar")]
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                assert PP(c.fetchone()[0]) == db.root


class TestTableExists:
    def testTableExists(self, bare_db):
        with bare_db as db:
            with db.connect() as conn:
                assert not db.table_exists("test")
                conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, txt TEXT);")
                conn.commit()
            assert db.table_exists("test")
            assert db.table_exists("fs_meta")
            assert not db.table_exists("foobar")
