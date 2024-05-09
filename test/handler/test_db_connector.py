from contextlib import contextmanager
import os
from pathlib import PurePath as PP
import pytest
import sqlite3 as sql
import tempfile
from typing import Union, Tuple, Generator
from unittest.mock import patch

from scoutlib.handler.db_connector import DBConnector


def mktemp_cleanly(suffix: str = ".db") -> Tuple[PP, PP]:
    file_desc, fake_file_path = tempfile.mkstemp(suffix=suffix)
    os.close(file_desc)
    return PP(fake_file_path)


def clean_tempfile(file_path: Union[PP, str]) -> None:
    os.unlink(file_path)


@contextmanager
def mktempfile_context(suffix: str = ".db"):
    file_desc, fake_file_path = tempfile.mkstemp(suffix=suffix)
    os.close(file_desc)
    yield PP(fake_file_path)
    os.unlink(fake_file_path)


class TestInit:
    """Tests around constructor and any of its helpers"""

    def test_args_raise(self):
        """Test that these conditions raise a ValueError or TypeError.
        - path is not a PurePath or str
        - root is not a PurePath or str
        - parent of path is not a dir
        - root is not a dir
        """
        with mktempfile_context() as fake_db_file:
            with pytest.raises((ValueError, TypeError)):
                DBConnector(1)  # type: ignore
            with pytest.raises((ValueError, TypeError)):
                DBConnector(fake_db_file, 1)  # type: ignore
            with pytest.raises((ValueError, TypeError)):
                DBConnector(f"{fake_db_file}/parent/not/dir")
            with pytest.raises((ValueError, TypeError)):
                DBConnector(fake_db_file, fake_db_file)  # not a dir, it's the db file

    @pytest.fixture
    @contextmanager
    def fake_db_file(self):
        with mktempfile_context() as fake_db_file:
            with sql.connect(fake_db_file) as conn:
                conn.execute("CREATE TABLE foobar (id TEXT PRIMARY KEY, txt TEXT);")
                conn.execute("INSERT INTO foobar (id, txt) VALUES ('foo', 'bar');")
                conn.commit()
            yield fake_db_file

    @pytest.fixture
    @contextmanager
    def fake_scout_db_file(self):
        with mktempfile_context() as fake_db_file:
            with sql.connect(fake_db_file) as conn:
                q = "CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);"
                conn.execute(q)
                q = "INSERT INTO fs_meta (property, value) VALUES ('root', '/a/b');"
                conn.execute(q)
                conn.commit()
            yield fake_db_file

    @pytest.fixture
    @contextmanager
    def fake_txt_file(self):
        with mktempfile_context() as fake_txt_file:
            with open(fake_txt_file, "w") as f:
                f.write("Hello World!")
            yield fake_txt_file

    def test_is_scout_db_file(self, fake_scout_db_file, fake_db_file, fake_txt_file):
        """Correctly returns bool for:
        True: Is both a db file and has fs_meta table
        False:
            - Not a db file (e.g. a txt file saying Hello World!)
            - A db file but doesn't have fs_meta table
        """
        with fake_scout_db_file as fp:
            assert DBConnector.is_scout_db_file(fp)
        with fake_db_file as fp:
            assert not DBConnector.is_scout_db_file(fp)
        with fake_txt_file as fp:
            assert not DBConnector.is_scout_db_file(fp)

    def test_is_db_file(self, fake_db_file, fake_scout_db_file, fake_txt_file):
        """Returns true for sqlite3 file, false otherwise"""
        with fake_db_file as fp:
            assert DBConnector.is_db_file(fp)
        with fake_scout_db_file as fp:
            assert DBConnector.is_db_file(fp)
        with fake_txt_file as fp:
            assert not DBConnector.is_db_file(fp)

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
