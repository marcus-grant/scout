from contextlib import contextmanager
import os
from pathlib import PurePath as PP
import pytest
import tempfile
from unittest.mock import patch

from scoutlib.handler.db_connector import DBConnector as DBC
from scoutlib.handler.file_repo import FileRepo
from scoutlib.handler.dir_repo import DirRepo
from scoutlib.model.file import File
from scoutlib.model.dir import Dir

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


class TestInitUtils:
    """Tests FileRepo.__init__ helper methods. Does NOT test __init__ itself."""

    def testCreateFileTable(self, base_dbconn):
        """Tests existance of and schema of the 'file' table."""
        # Assemble
        query_table = "SELECT name FROM sqlite_master WHERE type='table'"
        query_schema = "PRAGMA table_info(file)"
        expect = [
            # Expected schema is list for every column with tuple of:
            # (num: int, name: str, dtype: str, nullable: bool, prime_key: bool)
            # Bools are represented as 0|1, but python evaluates them as False|True
            (0, "id", "INTEGER", 1, None, 1),
            (1, "name", "TEXT", 1, None, 0),
            (2, "md5", "TEXT", 0, None, 0),
            (3, "mtime", "INTEGER", 0, None, 0),
            (4, "updated", "INTEGER", 0, None, 0),
        ]

        with base_dbconn as db:
            FileRepo.create_file_table(db)  # Act
            with db.connect() as conn:
                c = conn.cursor()
                c.execute(query_table)
                assert ("file",) in c.fetchall()  # Assert Table exists

                # Assert schema
                schema = c.execute(query_schema).fetchall()
                assert len(schema) == 5
                assert schema[0] == expect[0]  # Assert column 0
                assert schema[1] == expect[1]  # Assert column 1
                assert schema[2] == expect[2]  # Assert column 2
                assert schema[3] == expect[3]  # Assert column 3
                assert schema[4] == expect[4]  # Assert column 4


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
