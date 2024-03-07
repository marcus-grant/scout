# TODO: Consider adding name and/or description strs to members
# TODO: Create context manager for db connection to improve DRYness
# TODO: Consider an index for for dir.name & adding regular col for dir.depth
# this could speed up join queries on paths
# TODO: Need to find a more normalized way to query against path;
# we may keep path for ease of offline analysis, but during operations,
# we should be using either a closure or self referencing parent FK.
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
                            name TEXT NOT NULL,
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

    # TODO: Test directory case
    def normalize_path(self, pathlike: Union[str, PurePath, Directory]) -> PurePath:
        """
        Normalize a path string into a list of path components.
        Order determines depth, so the first element is the repo root.
        Returns None if the path is not within the repo.
        Returns an empty list if the path is the repo root.
        """
        # If pathlike is directory assign path to pathlike.path
        # If it's a string, convert to PurePath
        # If it's already a purepath, assign to path
        if isinstance(pathlike, Directory):
            path = pathlike.path
        elif isinstance(pathlike, str):
            path = PurePath(pathlike)
        else:
            path = pathlike
        if not path.is_absolute():
            # Assume relative path to repo root
            path = self.path / path
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

    # TODO: Needs testing
    # TODO: Do we normalize the path?
    def select_dir_where_path(
        self, path: Union[Directory, PurePath, str]
    ) -> Optional[tuple[int, str]]:
        np = self.normalize_path(path)
        res = None  # Result
        with self.connection() as conn:
            query = "SELECT * FROM dir WHERE path = ?"
            res = conn.execute(query, (str(np),)).fetchone()
        # breakpoint()
        return res

    # TODO: Feels like wrong place for this
    def ancestor_paths(self, path: Union[PurePath, str]) -> list[PurePath]:
        current = self.normalize_path(path)
        ancestors = []
        while current != PurePath():  # while current path isn't relative root
            ancestors.append(current)  # add current path to ancestors
            current = current.parent  # then update current path to its parent
        return ancestors[::-1]  # Now desired list is reverse of ancestors

    def insert_into_dir(self, name: str, path: Union[PurePath, str]) -> Optional[int]:
        """
        Inserts a new record into the dir table.
        Takes its anema nd path as arguments.
        Returns the id of the new record if it was added,
        as there could already be a record with the same unique path.
        Otherwise returns None if it wasn't inserted.
        """
        np = self.normalize_path(path)
        try:
            with self.connection() as conn:
                query = "INSERT INTO dir (name, path) VALUES (?, ?)"
                cursor = conn.cursor()
                cursor.execute(query, (name, str(np)))
                conn.commit()
                return cursor.lastrowid
        except sqlite3.IntegrityError:
            return None

    # def add(self, dir: Directory):
    #     """
    #     Takes a directory, normalizes its path, and adds it to the database.
    #     This includes the dir table and the dir_ancestor table.
    #     It also adds the directory's ancestors to the dir_ancestor table.
    #     Since the path is unique, it will catch the exception if it already exists.
    #     """
    #     aps = self.ancestor_paths(dir.path)
    #     depth = 0
    #     last_id = None
    #     curr_id = None
    #     with self.connection() as conn:
    #         cursor = conn.cursor()
    #         for ap in aps:
    #             added = True
    #             try:
    #                 cursor.execute("INSERT INTO dir (name, path)",(ap,))
    #             except sqlite3.IntegrityError:
    #                 added = False
    #             curr_id = cursor.lastrowid
    #             if added and last_id is not None:
    #                 cursor.execute(
    #                     "INSERT INTO dir_ancestor (dir_id, ancestor_id, depth) VALUES (?, ?, ?)",
    #                     (curr_id, last_id, depth),
    #                 )

    def get(
        self, path: Optional[Union[PurePath, str]], id=Optional[int]
    ) -> Optional[Directory]:
        if path is not None:
            dir_row = self.query_path(path)
            if dir_row is None:
                return None
            return Directory(path=dir_row[1], id=dir_row[0])
        return None

    # TODO: Consider usage, what's to be done with directory that gets an ID?
    # Should we return the directory with the ID? Should we send a dir & mutate its ID?
    # def add(self, dir: Directory):
    #     path = self.normalize_path(dir)
    #     ancestor_paths = [PurePath(path.parts[: i + 1]) for i in range(len(path.parts))]
