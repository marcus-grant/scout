import os
from pathlib import PurePath
import pytest
from unittest.mock import patch
import sqlite3
import tempfile

from scoutlib.handler.file_repo import FileRepo
from scoutlib.model.file import File

PP = PurePath


@pytest.fixture
def base_repo():
    """
    Fixture for a basic FileRepo when testing init or a fresh slate.
    Handled in a memory-based sqlite temp file.
    """
    fd, path = tempfile.mkstemp()  # Setup a temp file
    os.close(fd)  # Close the file descriptor (avoids leaks)
    repo = FileRepo(path)  # Init FileRepo
    yield repo  # Give up context to the base repo fixture
    os.unlink(path)  # Cleanup


class TestBaseRepoFixture:
    def test_table_file(self, base_repo):
        """
        Test that the table file is created in the db and
        that the schema is correct.
        """
        with sqlite3.connect(base_repo.path_db) as conn:
            # Ensure table 'file' present
            query = "SELECT name FROM sqlite_master WHERE type='table' AND name='file';"
            res = conn.execute(query).fetchone()
            assert res is not None, "Table 'file' not found in db."

            # Query for schema and assert its validity
            # Pragma schema queries come in the form of:
            # [(cid, name, type, notnull, dflt_value, key)] per column
            schema_query = "PRAGMA table_info(file);"
            real_schema = conn.execute(schema_query).fetchall()

            # Expected schema:
            expected_schema = [
                (0, "id", "INTEGER", 0, None, 1),
                (1, "parent_id", "INTEGER", 1, None, 0),
                (2, "name", "TEXT", 1, None, 0),
                (3, "md5", "TEXT", 0, None, 0),
                (4, "mtime", "INTEGER", 0, None, 0),
                (5, "updated", "INTEGER", 0, None, 0),
            ]

            # Assert column count
            assert len(real_schema) == len(expected_schema), "Column count mismatch."

            # Assert id column correct
            id_col, expected_col = real_schema[0], expected_schema[0]
            assert id_col == expected_col, f"Expected {expected_col}, got {id_col}"

            # Assert parent_id foreign key col
            parent_col, exp_col = real_schema[1], expected_schema[1]
            assert parent_col == exp_col, f"Expected {exp_col}, got {parent_col}"

            # Assert name column correct
            name_col, exp_col = real_schema[2], expected_schema[2]
            assert name_col == exp_col, f"Expected {exp_col}, got {name_col}"

            # Assert md5 column correct
            md5_col, exp_col = real_schema[3], expected_schema[3]
            assert md5_col == exp_col, f"Expected {exp_col}, got {md5_col}"

            # Assert mtime column correct
            mtime_col, exp_col = real_schema[4], expected_schema[4]
            assert mtime_col == exp_col, f"Expected {exp_col}, got {mtime_col}"

            # Assert updated column correct
            updated_col, exp_col = real_schema[5], expected_schema[5]
            assert updated_col == exp_col, f"Expected {exp_col}, got {updated_col}"

    def test_table_insert(self, base_repo):
        """Test transactions can run on base_repo & later that the row is gone."""
        with sqlite3.connect(base_repo.path_db) as conn:
            # Insert a row
            query = "INSERT INTO file (parent_id, name) VALUES (1, 'test');"
            conn.execute(query)
            conn.commit()
        with sqlite3.connect(base_repo.path_db) as conn:
            # Query for the row
            query = "SELECT * FROM file WHERE name='test';"
            res = conn.execute(query).fetchall()
            assert len(res) >= 1, "Row not found in db."

    def test_cleanup(self, base_repo):
        """Test that the base repo cleans up after itself."""
        with sqlite3.connect(base_repo.path_db) as conn:
            # Query that the table is there AND empty
            query = "SELECT * FROM file;"
            assert len(conn.execute(query).fetchall()) == 0, "Table not empty."
