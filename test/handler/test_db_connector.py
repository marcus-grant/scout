from contextlib import contextmanager
import os
from pathlib import PurePath as PP
import pytest
import sqlite3 as sql
import tempfile
from typing import Union, Tuple, Generator
from unittest.mock import patch

from scoutlib.handler.db_connector import DBConnector


@contextmanager
def temp_dir_context():
    with tempfile.TemporaryDirectory() as temp_dir:
        yield PP(temp_dir)


class TestInit:
    """Tests around constructor and any of its helpers"""

    @pytest.fixture
    @contextmanager
    def fake_files_dir(self):
        with temp_dir_context() as temp_dir:
            with open(temp_dir / "test.txt", "w") as f:
                f.write("Hello World!")
            with sql.connect(temp_dir / "test.db") as conn:
                conn.execute("CREATE TABLE foobar (id TEXT PRIMARY KEY, txt TEXT);")
                conn.execute("INSERT INTO foobar (id, txt) VALUES ('foo', 'bar');")
                conn.commit()
            with sql.connect(temp_dir / "base.scout.db") as conn:
                q = "CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);"
                conn.execute(q)
                q = "INSERT INTO fs_meta (property, value) VALUES ('root', '/a/b');"
                conn.execute(q)
                conn.commit()
            yield temp_dir

    def test_fake_files_dir_files(self, fake_files_dir):
        with fake_files_dir as dp:
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
                assert cursor.fetchone() == ("root", "/a/b")

    def test_fake_files_dir_cleanup(self, fake_files_dir):
        """fake_files_dir dir and contents should be cleaned after context."""
        with fake_files_dir as dp:
            assert os.path.exists(dp)
        assert not os.path.exists(dp)

    def test_is_scout_db_file(self, fake_files_dir):
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

    def test_is_db_file(self, fake_files_dir):
        """Returns true for sqlite3 file, false otherwise"""
        with fake_files_dir as dp:
            assert DBConnector.is_db_file(dp / "test.db")
            assert DBConnector.is_db_file(dp / "base.scout.db")
            assert not DBConnector.is_db_file(dp / "test.txt")

    def test_init_db_creates(self, fake_files_dir):
        """Creates fs_meta table in a db file:
        - in provided path with
            - correct schema
            - root property"""
        with fake_files_dir as dp:
            path = dp / "init.db"
            DBConnector.init_db(path, PP("/a/b"))
            assert DBConnector.is_db_file(path)
            assert DBConnector.is_scout_db_file(path)
            with sql.connect(path) as conn:
                c = conn.cursor()
                c.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = c.fetchall()
                assert ("fs_meta",) in tables
                c.execute("SELECT value FROM fs_meta WHERE property='root';")
                assert c.fetchone()[0] == "/a/b"

    def test_init_db_raises_on_change(self, fake_files_dir):
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
                assert c.fetchone()[0] == "/a/b"

    @pytest.mark.parametrize(
        "path, root",
        [("b.db", "/a/b"), ("g.db", "/f/g"), ("c.db", "/a/b/c")],
    )
    def test_read_root(self, fake_files_dir, path, root):
        """DBConnector.read_root returns:
        - Correct root property value from fs_meta table
        - Returns only PP values
        """
        with fake_files_dir as dp:
            path = dp / path
            DBConnector.init_db(path, root)
            assert DBConnector.read_root(path) == PP(root)
            assert isinstance(DBConnector.read_root(path), PP)

    def test_read_root_raises_no_root(self, fake_files_dir):
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

    # def test_args_raise(self):
    #     """Test that these conditions raise a ValueError or TypeError.
    #     - path is not a PurePath or str
    #     - root is not a PurePath or str
    #     - parent of path is not a dir
    #     - root is not a dir
    #     """
    #     with mktempfile_context() as fake_db_file:
    #         with pytest.raises((ValueError, TypeError)):
    #             DBConnector(1)  # type: ignore
    #         with pytest.raises((ValueError, TypeError)):
    #             DBConnector(fake_db_file, 1)  # type: ignore
    #         with pytest.raises((ValueError, TypeError)):
    #             DBConnector(f"{fake_db_file}/parent/not/dir")
    #         with pytest.raises((ValueError, TypeError)):
    #             DBConnector(fake_db_file, fake_db_file)  # not a dir, it's the db file

    # TODO: Is this the right scout db file validation check?
    # def test_raises_not_scout_db_file(self):
    #     """Test that a ValueError is raised when the file is not a scout db file"""
    #     with mktempfile_context() as fp:
    #         with open(fp, "w") as f:
    #             f.write("Hello World!")
    #         with pytest.raises(ValueError):
    #             DBConnector(fp)

    # def test_is_scout_db_called(self):
    #     """__init__ should call is_scout_db_file"""
    #     fn_str = "scoutlib.handler.db_connector.DBConnector.is_scout_db_file"
    #     with mktempfile_context() as fp:
    #         with patch(fn_str) as mock:
    #             DBConnector(fp)
    #             mock.assert_called_once_with(fp)

    # def test_init_db_called(self):
    #     """__init__ should call init_db"""
    #     fn_str = "scoutlib.handler.db_connector.DBConnector._init_db"
    #     with mktempfile_context() as fp:
    #         with patch(fn_str) as mock:
    #             DBConnector(fp)
    #             mock.assert_called_once()
