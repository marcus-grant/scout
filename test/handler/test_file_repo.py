# TODO: updated column can be off by 1 second, make tests resilient to this
from contextlib import contextmanager
from datetime import datetime as dt
import os
from pathlib import PurePath as PP
import pytest
import sqlite3 as sql
import tempfile
from unittest.mock import patch

from scoutlib.handler.db_connector import DBConnector as DBC
from scoutlib.handler.file_repo import FileRepo
from scoutlib.handler.dir_repo import DirRepo
from scoutlib.model.file import File
from scoutlib.model.dir import Dir
from scoutlib.model.hash import HashMD5

MOD_DBC = "scoutlib.handler.db_connector.DBConnector"
MOD_FR = "scoutlib.handler.file_repo.FileRepo"


### Fixtures ###
# TODO: Move common fixtures to conftest.py
@contextmanager
def temp_dir_context():
    with tempfile.TemporaryDirectory() as tempdir:
        yield PP(tempdir)


@pytest.fixture
@contextmanager
def base_dbconn():
    with temp_dir_context() as tempdir:
        db = DBC(tempdir / ".scout.db")
        yield db


@pytest.fixture
@contextmanager
def base_repo(base_dbconn):
    with base_dbconn as db:
        yield FileRepo(db), DirRepo(db)


# TestFixtures
class TestFixtures:
    """Tests this module's fixtures."""

    def testTempDirContext(self):
        """Tests the temp_dir_context fixture for read write operations with os module."""
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
        """Tests the base_dbconn fixture."""
        with base_dbconn as db:
            assert db.root == db.path.parent
            assert os.path.isfile(db.path)
            assert os.path.isdir(db.root)
            assert DBC.read_root(db.path) == db.root
            assert DBC.is_scout_db_file(db.path)

    def testBaseRepo(self, base_repo):
        """Tests the base_repo fixture."""
        with base_repo as (fr, dr):
            assert fr.db == dr.db
            assert fr.db is not None
            assert fr.db.path is not None
            assert fr.db.root is not None
            assert fr.db.path == fr.db.root / ".scout.db"


# TestInitUtils
class TestInitUtils:
    """Tests FileRepo.__init__ helper methods. Does NOT test __init__ itself."""

    def testCreateFileTable(self, base_dbconn):
        """Tests existance of and schema of the 'file' table."""
        # Assemble
        query_table = "SELECT name FROM sqlite_master WHERE type='table'"
        query_schema = "PRAGMA table_info(file)"
        expect = [
            # Expected schema is list for every column with tuple of:
            # (num: int, name: str, dtype: str, notnull: bool, prime_key: bool)
            # Bools are represented as 0|1
            (0, "id", "INTEGER", 1, None, 1),
            (1, "dir_id", "iINTEGER", 0, None, 0),
            (2, "name", "TEXT", 1, None, 0),
            (3, "md5", "TEXT", 0, None, 0),
            (4, "mtime", "INTEGER", 0, None, 0),
            (5, "updated", "INTEGER", 0, None, 0),
        ]

        with base_dbconn as db:
            FileRepo.create_file_table(db)  # Act
            with db.connect() as conn:
                c = conn.cursor()
                c.execute(query_table)
                assert ("file",) in c.fetchall()  # Assert Table exists

                # Assert schema
                schema = c.execute(query_schema).fetchall()
                assert len(schema) == len(expect)
                assert schema[0] == expect[0]  # Assert column 0
                assert schema[1] == expect[1]  # Assert column 1
                assert schema[2] == expect[2]  # Assert column 2
                assert schema[3] == expect[3]  # Assert column 3
                assert schema[4] == expect[4]  # Assert column 4
                assert schema[5] == expect[5]  # Assert column 5


# TestInit
class TestInit:
    """Tests FileRepo.__init__ method."""

    def testSetsMembers(self, base_dbconn):
        """Tests that __init__ sets members."""
        with base_dbconn as db:
            fr = FileRepo(db)
            assert fr.db == db

    def testCallsTableExists(self, base_dbconn):
        """Tests that DBConnector.table_exists is called to check for table."""
        with base_dbconn as db:
            with patch(f"{MOD_DBC}.table_exists") as mock:
                FileRepo(db)
                mock.assert_called_once_with("file")

    def testCallsCreateTable(self, base_dbconn):
        """Tests that create_file_table is called when table does not exist."""
        with base_dbconn as db:
            with patch(f"{MOD_DBC}.table_exists", return_value=False):
                with patch(f"{MOD_FR}.create_file_table") as mock:
                    FileRepo(db)
                    mock.assert_called_once_with(db)


# TestSelectDirWhere
class TestSelectDirWhere:
    """Tests FileRepo.select_dir_query method."""

    def testIdReturns(self, base_repo):
        """Tests that when an id is supplied, the query returns the correct dir."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            dr.add(dir=Dir("bar"))
            assert fr.select_dir_where(id=2) == (2, "bar")

    def testPathReturns(self, base_repo):
        """Tests that when a path is supplied, the query returns the correct dir."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            dr.add(dir=Dir("bar"))
            assert fr.select_dir_where(path="foo") == (1, "foo")

    def testDirNotExists(self, base_repo):
        """Tests that when the dir does not exist, None is returned."""
        with base_repo as (fr, _):
            assert fr.select_dir_where(id=1) is None

    def testRaisesNoArgs(self, base_repo):
        """Tests that when no arguments are supplied, raises TypeError."""
        with base_repo as (fr, _):
            with pytest.raises(TypeError):
                fr.select_dir_where()


# TestSelectFileWhereQuery
class TestSelectFilesQuery:
    """Tests FileRepo.select_file_where_query builder method."""

    EXPECT = "SELECT * FROM file WHERE"

    def testRaiseOnNoArg(self, base_repo):
        """Tests that when no arguments are supplied, raises TypeError."""
        with base_repo as (fr, _):
            with pytest.raises(TypeError):
                fr.select_files_where_query()

    def testReturnOn1Args(self, base_repo):
        """Tests that when only one argument is supplied, the query is correct."""
        with base_repo as (fr, _):
            fn = fr.select_files_where_query
            assert fn(id=1) == f"{self.EXPECT} id = 1;"
            assert fn(dir_id=1) == f"{self.EXPECT} dir_id = 1;"
            assert fn(name="foo") == f"{self.EXPECT} name = 'foo';"
            assert fn(md5="DEADBEEF") == f"{self.EXPECT} md5 = 'DEADBEEF';"
            assert fn(mtime=42) == f"{self.EXPECT} mtime = 42;"
            assert fn(updated=69) == f"{self.EXPECT} updated = 69;"

    def testReturnAllArgs(self, base_repo):
        """Tests that when all arguments are supplied, the query is correct."""
        with base_repo as (fr, _):
            fn = fr.select_files_where_query
            query = fn(dir_id=1, name="foo", md5="CAFE", mtime=42, updated=69)
            expect = f"{self.EXPECT} dir_id = 1 AND name = 'foo' AND md5 = 'CAFE' AND mtime = 42 AND updated = 69;"
            assert query == expect

    def testReturnIdOverrides(self, base_repo):
        """Tests that when id is supplied, a single Where clause is returned."""
        with base_repo as (fr, _):
            fn = fr.select_files_where_query
            query = fn(id=1, dir_id=2, md5="CAFE", mtime=42, updated=69)
            assert query == f"{self.EXPECT} id = 1;"

    def testReturnSomeArgs(self, base_repo):
        """Tests that when some arguments are supplied, the query is correct."""
        with base_repo as (fr, _):
            fn = fr.select_files_where_query
            query = fn(dir_id=1, mtime=42, updated=69)
            expect = f"{self.EXPECT} dir_id = 1 AND mtime = 42 AND updated = 69;"
            assert query == expect
            query = fn(name="foo", mtime=42, md5="CAFE")
            expect = f"{self.EXPECT} name = 'foo' AND mtime = 42 AND md5 = 'CAFE';"


# Test InsertFileQuery
class TestInsertFileQuery:
    """Tests FileRepo.insert_file_query method."""

    EXP = "INSERT INTO file"

    def testReturnReqOnly(self, base_repo):
        """Tests returned query of only required arguments."""
        with base_repo as (fr, _):
            query = fr.insert_file_query(0, "foobar.txt")
            expect = f"{self.EXP} (dir_id, name) VALUES (0, 'foobar.txt');"
            assert query == expect

    def testReturnSomeOpts(self, base_repo):
        """Tests returned query of required and some optional arguments."""
        with base_repo as (fr, _):
            query = fr.insert_file_query(0, "foo.txt", mtime=42)
            expect = f"{self.EXP} (dir_id, name, mtime) VALUES (0, 'foo.txt', 42);"
            assert query == expect

    def testReturnAllOpts(self, base_repo):
        """Tests returned query of all optional arguments."""
        with base_repo as (fr, _):
            query = fr.insert_file_query(0, "foo.txt", "DEADBEEF", 42, 69)
            expect = f"{self.EXP} (dir_id, name, md5, mtime, updated) VALUES (0, 'foo.txt', 'DEADBEEF', 42, 69);"
            assert query == expect

    # TODO: Only do a few query executions to ensure syntax is good
    # Turn these into one case and assert all, some, and none of arguments

    def testReqArgsInDb(self, base_repo):
        """Tests method returns a string with required SQL syntax.
        When using only the required arguments."""
        with base_repo as (fr, _):
            query = fr.insert_file_query(0, "foobar.txt")
            with fr.db.connect() as conn:
                c = conn.cursor()
                c.execute(query)
                conn.commit()
                rows = c.execute("SELECT * FROM file").fetchall()
                assert len(rows) == 1
                assert rows[0] == (1, 0, "foobar.txt", None, None, None)

    def testAllArgsInDB(self, base_repo):
        """Tests if when all arguments supplied,
        returned query results in them in the database."""
        with base_repo as (fr, _):
            query = fr.insert_file_query(0, "foobar.txt", "DEADBEEF", 42, 69)
            with fr.db.connect() as conn:
                c = conn.cursor()
                c.execute(query)
                conn.commit()
                rows = c.execute("SELECT * FROM file").fetchall()
                assert len(rows) == 1
                assert rows[0] == (1, 0, "foobar.txt", "DEADBEEF", 42, 69)

    def testFKWorksWithDirRow(self, base_repo):
        """Tests that when a dir_id is supplied from real dir row,
        the query built from this method results in a valid file row."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            dr.add(dir=Dir("bar"))
            q1 = fr.insert_file_query(2, "foo.md")
            q2 = fr.insert_file_query(1, "bar.txt")
            with fr.db.connect() as conn:
                c = conn.cursor()
                c.execute(q1)
                c.execute(q2)
                conn.commit()
                rows = c.execute("SELECT * FROM file").fetchall()
            assert len(rows) == 2
            assert rows[0] == (1, 2, "foo.md", None, None, None)
            assert rows[1] == (2, 1, "bar.txt", None, None, None)


# TODO: Needs filter for path that results in dir_id and name
class TestAdd:
    """Tests FileRepo.add method."""

    def testAbsRelPathsSame(self, base_repo):
        """Tests that absolute and relative paths result in same file rows"""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            # Even indices are absolute paths, odd are relative
            files = [File("foo/bar.txt"), File(fr.db.root / "foo/bar.txt")]
            files += [File("foo/baz.txt"), File(fr.db.root / "foo/baz.txt")]
            stored_files = fr.add(files)
            assert [f.id for f in stored_files] == [1, 2, 3, 4]
            # Everything but the id & updated should be the same so set them equal
            stored_files[0].id = stored_files[1].id
            stored_files[2].id = stored_files[3].id
            stored_files[0].updated = stored_files[1].updated
            stored_files[2].updated = stored_files[3].updated
            assert stored_files[0] == stored_files[1]
            assert stored_files[2] == stored_files[3]
            # Now do the same with the associated rows in the database
            with fr.db.connect() as conn:
                c = conn.cursor()
                rows = c.execute("SELECT * FROM file").fetchall()
                assert len(rows) == 4
                # Again everything but the id & updated column should be the same
                assert rows[0][1:-1] == rows[1][1:-1]
                assert rows[2][1:-1] == rows[3][1:-1]

    def testWithDirIdGiven(self, base_repo):
        """Tests that file table correct when adding with dir_id given.
        NOTE: This means dir_id should override dir in parent of path of file."""
        with base_repo as (fr, dr):
            updated = int(dt.now().timestamp())
            dr.add(dir=Dir("test"))
            dr.add(dir=Dir("foo"))
            fr.add([File("foo/foo.txt", dir_id=2), File("test/test.txt", dir_id=1)])
            with fr.db.connect() as conn:
                c = conn.cursor()
                rows = c.execute("SELECT * FROM file").fetchall()
            assert len(rows) == 2
            assert rows[0] == (1, 2, "foo.txt", None, None, updated)
            assert rows[1] == (2, 1, "test.txt", None, None, updated)

    def testDirIdOverridesPath(self, base_repo):
        """Sometimes you might know the dir_id which saves querying the dir table.
        Thus it should override the dir in the path of the file."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            dr.add(dir=Dir("baz"))
            updated = int(dt.now().timestamp())
            file = fr.add([File("baz/bar.txt", dir_id=1)])  # Note baz dir is dir_id=2
            assert file[0].dir_id == 1
            assert file[0].path == fr.db.root / "foo/bar.txt"
            with fr.db.connect() as conn:
                dir_row = conn.execute("SELECT * FROM file").fetchone()
                assert dir_row == (1, 1, "bar.txt", None, None, updated)

    def testWithoutDirId(self, base_repo):
        """Tests that file table correct when adding without dir_id given."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("test"))
            updated = int(dt.now().timestamp())
            files = [File("root.txt"), File(fr.db.root / "hello"), File("test/foo.txt")]
            files = fr.add(files)
            with fr.db.connect() as conn:
                c = conn.cursor()
                rows = c.execute("SELECT * FROM file").fetchall()
            assert len(rows) == 3
            assert rows[0] == (1, 0, "root.txt", None, None, updated)
            assert rows[1] == (2, 0, "hello", None, None, updated)
            assert rows[2] == (3, 1, "foo.txt", None, None, updated)

    def testRootPath(self, base_repo):
        """Tests that when a file is added with root path (relative and absolute) the dir_id column is 0."""
        with base_repo as (fr, _):
            updated = int(dt.now().timestamp())
            fr.add([File(fr.db.root / "root.gpg"), File("hello.html")])
            with fr.db.connect() as conn:
                c = conn.cursor()
                rows = c.execute("SELECT * FROM file").fetchall()
            assert rows[0] == (1, 0, "root.gpg", None, None, updated)
            assert rows[1] == (2, 0, "hello.html", None, None, updated)

    def testSinlgeFileSameAsList(self, base_repo):
        """Tests that adding a single file results in same thing as
        a list of one file."""
        with base_repo as (fr, _):
            single_file = fr.add(File("foo.txt"))
            file_list = fr.add([File("foo.txt")])
            # Change File obect's id and updated members to match
            # They are expected to be different between successive adds
            single_file[0].id = file_list[0].id
            file_list[0].updated = single_file[0].updated
            assert single_file == file_list
            with fr.db.connect() as conn:
                c = conn.cursor()
                rows = c.execute("SELECT * FROM file").fetchall()
                assert len(rows) == 2
                # Same as before id (definitely) and updated (occasionally) are different
                assert rows[0][1:-1] == rows[1][1:-1]

    def testAllOptionalFileArgs(self, base_repo):
        """Tests that all optional arguments are added to the file table."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            # mtime should be epoch time integer for current time
            path = fr.db.root / "foo/foo.txt"
            mtime = dt.fromtimestamp(42)
            md5 = HashMD5(hex="CAFE")
            updated = int(dt.now().timestamp())
            fr.add(File(path, dir_id=1, size=1024, mtime=mtime, md5=md5))
            with fr.db.connect() as conn:
                c = conn.cursor()
                rows = c.execute("SELECT * FROM file").fetchall()
                assert len(rows) == 1
                assert rows[0] == (1, 1, "foo.txt", "cafe", 42, updated)


class TestGet:
    """Tests FileRepo.get method."""

    def testByName(self, base_repo):
        """Tests that get returns the correct file by name."""
        with base_repo as (fr, dr):
            fr.add([File("a"), File("b"), File("c")])
            assert fr.get(name="a")[0].id == 1
            assert fr.get(name="b")[0].id == 2
            assert fr.get(name__ne="c")[0].id == 1
            assert fr.get(name__ne="c")[1].id == 2

    def testById(self, base_repo):
        """Tests that get returns the correct file by id."""
        with base_repo as (fr, _):
            fr.add([File("a"), File("b"), File("c")])
            assert fr.get(id__ne=1)[0].path.name == "b"
            assert fr.get(id__ne=1)[1].path.name == "c"
            assert fr.get(id=2)[0].path.name == "b"
            assert fr.get(id=3)[0].path.name == "c"

    def testByDirId(self, base_repo):
        """Tests that get returns the correct file by dir_id foreign key."""
        """Essentially what you'd do to find files in same directory."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            dr.add(dir=Dir("bar"))
            fa, fc = File("a", dir_id=1), File("c", dir_id=1)
            fb, fx = File("b", dir_id=2), File("r", dir_id=0)
            fr.add([fa, fb, fc, fx])
            assert len(fr.get(dir_id=1)) == 2
            assert fr.get(dir_id=1)[0].path == fr.db.root / "foo/a"
            assert fr.get(dir_id=1)[1].path == fr.db.root / "foo/c"
            assert fr.get(dir_id=2)[0].path == fr.db.root / "bar/b"
            assert fr.get(dir_id=0)[0].path == fr.db.root / "r"
            assert fr.get(dir_id__ne=1)[0].path == fr.db.root / "bar/b"
            assert fr.get(dir_id__ne=1)[1].path == fr.db.root / "r"

    def testByMtimeAndGreaterThan(self, base_repo):
        """Tests that get returns the correct file by mtime.
        Also tests the __ge, __gt operators."""
        with base_repo as (fr, _):
            mtimes = [100, 200, 300, 400, 500, 600, 700, 800]
            mtimes = [dt.fromtimestamp(m) for m in mtimes]
            files = ["a", "b", "c", "d", "e", "f", "g", "h"]
            files = [File(f, mtime=m) for f, m in zip(files, mtimes)]
            fr.add(files)
            assert len(fr.get(mtime=99)) == 0
            assert len(fr.get(mtime=100)) == 1
            assert fr.get(mtime=100)[0].path.name == "a"
            assert fr.get(mtime=100)[0].id == 1
            assert fr.get(mtime=100)[0].mtime == dt.fromtimestamp(100)
            assert len(fr.get(mtime__ge=700)) == 2
            assert fr.get(mtime__ge=700)[0].id == 7
            assert fr.get(mtime__ge=700)[0].path.name == "g"
            assert fr.get(mtime__ge=700)[1].path.name == "h"
            assert fr.get(mtime__ge=700) == fr.get(mtime__gt=600)
            assert len(fr.get(mtime__gt=100)) == 7

    def testByUpdatedAndLessThan(self, base_repo):
        """Tests that get returns correct file objects by updated time column.
        Also tests the __le, __lt operators.
        Does so by adding files then altering the updated time column in sqlite."""
        ups = [100, 200, 300, 400, 500, 600, 700, 800]
        files = ["a", "b", "c", "d", "e", "f", "g", "h"]
        files = [File(f) for f in files]
        with base_repo as (fr, _):
            with fr.db.connect() as conn:
                fr.add(files)
                c = conn.cursor()
                for i, up in enumerate(ups):
                    c.execute(f"UPDATE file SET updated = {up} WHERE id = {i+1}")
                conn.commit()
            assert len(fr.get(updated=101)) == 0
            assert len(fr.get(updated=100)) == 1
            assert fr.get(updated=100)[0].path.name == "a"
            assert fr.get(updated=100)[0].id == 1
            assert len(fr.get(updated__le=300)) == 3
            assert fr.get(updated__le=300)[0].id == 1
            assert fr.get(updated__le=300)[0].path.name == "a"
            assert fr.get(updated__le=300)[1].path.name == "b"
            assert fr.get(updated__le=300)[2].path.name == "c"
            assert len(fr.get(updated__le=700)) == 7
            assert fr.get(updated__le=700) == fr.get(updated__lt=800)

    def testByMd5AndNull(self, base_repo):
        """Tests that get returns the correct file by md5 hash.
        Also tests the __null operator since this is a likely column to use it on."""
        with base_repo as (fr, _):
            md5s = ["deadbeef", "cafefeed", "00", "12345678"]
            files = [File(str(m), md5=HashMD5(hex=m)) for m in md5s]
            files += [File("none")]
            fr.add(files)
            assert len(fr.get(md5="deadbeef")) == 1
            assert fr.get(md5="deadbeef")[0].path.name == "deadbeef"
            assert fr.get(md5="cafefeed")[0].path.name == "cafefeed"
            assert fr.get(md5="00")[0].path.name == "00"
            assert fr.get(md5="12345678")[0].path.name == "12345678"
            assert fr.get(md5__null=True)[0].path.name == "none"
            assert len(fr.get(md5__null=False)) == 4

    def testNoFilter(self, base_repo):
        """Tests that get returns all files when no filters are applied."""
        with base_repo as (fr, _):
            fr.add([File("a"), File("b"), File("c")])
            assert len(fr.get()) == 3

    def testEmptyResult(self, base_repo):
        """Tests that queries with no matches returns empty list."""
        with base_repo as (fr, _):
            fr.add([File("a"), File("b"), File("c")])
            assert len(fr.get(name="d")) == 0
            assert len(fr.get(id=4)) == 0
            assert len(fr.get(updated=100)) == 0
            assert len(fr.get(md5="deadbeef")) == 0
            assert len(fr.get(dir_id=2)) == 0

    def testManyFilters(self, base_repo):
        """Tests that get returns correct file with many filters applied."""
        with base_repo as (fr, dr):
            dr.add(dir=Dir("foo"))
            dr.add(dir=Dir("bar"))
            dr.add(dir=Dir("baz"))
            up = int(dt.now().replace(microsecond=0).timestamp())
            fr.add(
                [
                    File("foo/a.txt"),
                    File("bar/b.txt"),
                    File("baz/c.txt"),
                    File("a.txt"),
                    File("b.txt"),
                ]
            )
            assert fr.get(name="a.txt", dir_id=1)[0].path.name == "a.txt"
            assert fr.get(name="b.txt", dir_id=2)[0].path.name == "b.txt"
            assert fr.get(name="c.txt", dir_id=3)[0].path.name == "c.txt"
            assert fr.get(name="a", dir_id=2) == []
            assert fr.get(name="b", dir_id=1) == []
            assert fr.get(name="c", dir_id=1) == []
            assert fr.get(name="a.txt", dir_id=0)[0].path == fr.db.root / "a.txt"
            assert fr.get(name="b.txt", dir_id=2)[0].path == fr.db.root / "bar/b.txt"
