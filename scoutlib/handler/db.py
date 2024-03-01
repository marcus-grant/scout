# TODO: Consider adding name and/or description strs to members
# TODO: Create context manager for db connection to improve DRYness
# TODO: Consider an index for for dir.name & adding regular col for dir.depth
# this could speed up join queries on paths
import sqlite3
from pathlib import Path, PurePath
from os import sep
from typing import Optional, Union

from scoutlib.model.fs import Directory


class DirRepo:
    """
    Repository pattern class for managing storage layer of
    Directory objects in a SQLite database.
    """

    path_db: PurePath
    path: PurePath

    def __init__(self, db_path: Union[str, PurePath], path: Optional[PurePath] = None):
        if isinstance(db_path, str):
            db_path = PurePath(db_path)
        self.path_db = db_path
        if path is None:  # Default behvior is to house db in dir of repo
            self.path = self.path_db.parent
        else:
            self.path = path
        self._init_db()

    def db_path(self):
        return self.path / "dir.db"

    def _init_db(self):
        """Initialize db & create directory table if not there."""
        with sqlite3.connect(self.path_db) as conn:
            query = """ CREATE TABLE IF NOT EXISTS dir (
                            id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                            path TEXT NOT NULL,
                            CONSTRAINT path_unique UNIQUE (path)
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
            return conn

    # TODO: I don't know if this is the best way to do this
    def connection(self) -> sqlite3.Connection:
        """Yields a connection context manager for the SQLite db."""
        return sqlite3.connect(self.path_db)

    def normalize_path(self, path: Union[str, PurePath]) -> PurePath:
        """
        Normalize a path string into a list of path components.
        Order determines depth, so the first element is the repo root.
        Returns None if the path is not within the repo.
        Returns an empty list if the path is the repo root.
        """
        path = PurePath(path) if isinstance(path, str) else path
        try:
            return path.relative_to(self.path)
        except ValueError:
            raise ValueError(f"Path, {path}, not within DirRepo!")

    # TODO: Needs testing
    def denormalize_path(self, path: Union[str, PurePath]) -> PurePath:
        """
        Denormalize a path string into a PurePath object.
        """
        path = PurePath(path) if isinstance(path, str) else path
        return self.path / path

    def get_path(self, path: Union[PurePath, str]) -> Optional[Directory]:
        path = self.normalize_path(path)
        dir_row = None
        with self.connection() as conn:
            query = "SELECT * FROM dir WHERE path = ?"
            dir_row = conn.execute(query, (str(path),)).fetchall()
            if len(dir_row) <= 0:
                return None
        dir_row = dir_row[0]
        return Directory(id=dir_row[0], path=self.denormalize_path(dir_row[1]))
