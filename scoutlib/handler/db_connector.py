import os
from pathlib import PurePath as PP
import sqlite3 as sql
from typing import Optional, Union


class DBConnector:
    """
    A class that maintains info about the database file itself and
    the filesystem root.
    This means one class manages connecting to the database,
    maitaining consistent normalized paths into the database and
    normalized paths out of the database.
    Only concerned with the fs_meta table.
    """

    path: PP  # Path to the db file
    root: PP  # Path to the relative root of the db paths inside repos

    @classmethod
    def is_db_file(cls, path) -> bool:
        """
        Checks whether a file is a sqlite file and has the fs_meta table,
        indicating that this is a scout database file.
        """
        with open(path, "rb") as f:
            header = f.read(16)
            if header == b"SQLite format 3\x00":
                return True
        return False

    @classmethod
    def is_scout_db_file(cls, path) -> bool:
        """
        Checks whether a file is a scout database file.
        """
        if not cls.is_db_file(path):
            return False
        with sql.connect(path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            for table in tables:
                if "fs_meta" in table:
                    return True
            return False

    @classmethod
    def init_db(cls, path: PP, root: PP) -> None:
        """Initializes a sqlite file with the fs_meta table."""
        with sql.connect(path) as conn:
            q = """CREATE TABLE IF NOT EXISTS fs_meta (
                    property TEXT PRIMARY KEY, value TEXT);"""
            conn.execute(q)
            q = f"INSERT INTO fs_meta (property, value) VALUES ('root', '{root}');"
            conn.execute(q)
            conn.commit()

    @classmethod
    def read_root(cls, path: PP) -> PP:
        """Reads root property value from the fs_meta table."""
        with sql.connect(path) as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM fs_meta WHERE property='root';")
            res = c.fetchone()
            if res is None:
                raise sql.OperationalError("No root property in fs_meta table.")
            return PP(res[0])

    def __init__(
        self, path: Union[PP, str], root: Optional[Union[PP, str]] = None
    ) -> None:
        # Validate and set path arg
        if isinstance(path, str):
            self.path = PP(path)
        else:
            self.path = path
        # Raise errors for invalid paths
        if not isinstance(self.path, PP):
            raise TypeError(f"path {path} must be a PurePath or str")
        if not os.path.isdir(self.path.parent):
            raise ValueError(f"{self.path} must be in a valid directory.")

        # Check if path is empty but its parent is a valid dir
        if not os.path.exists(self.path):
            with sql.connect(self.path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "CREATE TABLE fs_meta (property TEXT PRIMARY KEY, value TEXT);"
                )
                conn.commit()

        # Now determine if a db file needs initialization or
        # if we're using an existing one along with its listed root path
