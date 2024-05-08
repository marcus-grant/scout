from contextlib import contextmanager
import os
from pathlib import PurePath as PP
import pytest
import sqlite3 as sql
import tempfile
from typing import Optional, Union, Generator

from scoutlib.handler.db_connector import DBConnector


def mktemp_cleanly(suffix: str = ".db") -> PP:
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
        with mktempfile_context() as fp:
            with open(fp, "w") as f:
                f.write("Hello World!")
            with pytest.raises(ValueError):
                DBConnector(fp)

    def test_is_scout_db_file(self):
        """Correctly returns bool for:
        True: Is both a db file and has fs_meta table
        False:
            - Not a db file (e.g. a txt file saying Hello World!)
            - A db file but doesn't have fs_meta table
        """
        with mktempfile_context() as fp:
            with sql.connect(fp) as conn:
                q1 = "CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);"
                q2 = "INSERT INTO fs_meta (property, value) VALUES ('root', '/a/b');"
                conn.execute(q1)
                conn.execute(q2)
                conn.commit()
            assert DBConnector.is_scout_db_file(fp)
        with mktempfile_context() as fp:
            with sql.connect(fp) as conn:
                conn.execute("CREATE TABLE hello (id INTEGER PRIMARY KEY, msg TEXT);")
                conn.execute("INSERT INTO hello (id, msg) VALUES (0, 'Hello World!');")
                conn.commit()
            assert not DBConnector.is_scout_db_file(fp)
        with mktempfile_context() as fp:
            with open(fp, "w") as f:
                f.write("Hello World!")
            assert not DBConnector.is_scout_db_file(fp)

    def test_raises_not_scout_db_file(self):
        """Test that a ValueError is raised when the file is not a scout db file"""
        with mktempfile_context() as fp:
            with open(fp, "w") as f:
                f.write("Hello World!")
            with pytest.raises(ValueError):
                DBConnector(fp)
