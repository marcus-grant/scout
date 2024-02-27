import os
import pytest
import sqlite3
import tempfile

from scoutlib.handler.db import DirRepo
from db_helpers import assert_table_schema, assert_table_exists


@pytest.fixture
def dir_repo():
    """Fixture for DirRepo class wrapped in temporary SQLite db file"""
    # Setup a temp file for SQLite db
    fd, repo_path = tempfile.mkstemp(suffix=".db")
    # Close tempfile descriptor to avoid resource leak
    os.close(fd)  # DirRepo will open it as needed
    # Init DirRepo with temporary db file
    repo = DirRepo(repo_path)
    yield repo  # Provide fixture return so it gets used
    # Teardown, so we don't leave temp files around
    os.unlink(repo_path)


def test_dir_repo_directory_table_exists(dir_repo):
    conn = sqlite3.connect(dir_repo.repo_path)
    assert_table_exists(conn, "dir")


def test_dir_repo_directory_table_schema(dir_repo):
    """
    Test that the 'directory' table exists with the expected schema, including
    checks for primary keys, foreign keys, and nullability.
    """
    # Expected schema is list for every column with tuple of:
    # (col_name: str, dtype: str, nullable: bool, prime_key: bool)
    expected_schema = [
        ("id", "INTEGER", False, True),
        ("name", "TEXT", False, False),
        # Add more columns as needed
    ]

    with sqlite3.connect(dir_repo.repo_path) as conn:
        assert_table_schema(conn, "dir", expected_schema)


def test_dir_repo_connection_returns_context_manager(dir_repo):
    with dir_repo.connection() as conn:
        # Assert that we have a valid connection object
        assert conn is not None
        assert isinstance(conn, sqlite3.Connection)
        # Perform a simple operation to ensure the connection is open
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result is not None  # This should succeed if the connection is open

    # After exiting the with block, the connection should be closed
    # We test this by attempting an operation that should fail if the connection is closed
    what_exception = None
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        operation_successful = True
    except Exception as e:
        raise e

    raise LookupError(what_exception)
    assert (
        not operation_successful
    ), "Connection should be closed after connection context exits."
