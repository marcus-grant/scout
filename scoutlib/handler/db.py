# TODO: Consider adding name and/or description strs to members
# TODO: Create context manager for db connection to improve DRYness
import sqlite3
from typing import Optional

from scoutlib.model.fs import Directory


class DirRepo:
    """
    Repository pattern class for managing storage layer of
    Directory objects in a SQLite database.
    """

    def __init__(self, repo_path: str):
        self.repo_path = repo_path
        self._init_db()

    def _init_db(self):
        """Initialize db & create directory table if not there."""
        with sqlite3.connect(self.repo_path) as conn:
            query = """ CREATE TABLE IF NOT EXISTS dir (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            name TEXT NOT NULL
                        );"""
            conn.cursor().execute(query)
            query = """CREATE TABLE IF NOT EXISTS dir_ancestor (
                            dir_id INTEGER NOT NULL,
                            ancestor_id INTEGER NOT NULL,
                            depth INTEGER NOT NULL,
                            PRIMARY KEY (dir_id, ancestor_id),
                            FOREIGN KEY (dir_id) REFERENCES directory(id),
                            FOREIGN KEY (ancestor_id) REFERENCES directory(id)
                        );"""
            conn.cursor().execute(query)
            conn.commit()
            # id PRIMARY KEY (dir_id, ancestor_id),
            # FOREIGN KEY (dir_id) REFERENCES dir(id),
            # FOREIGN KEY (ancestor_id) REFERENCES dir(id),
            # depth INTEGER NOT NULL

            return conn

    # TODO: I don't know if this is the best way to do this
    def connection(self) -> sqlite3.Connection:
        """Yields a connection context manager for the SQLite db."""
        return sqlite3.connect(self.repo_path)
