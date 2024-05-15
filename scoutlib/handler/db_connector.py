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
            tables = [t[0] for t in cursor.fetchall()]
            if "fs_meta" not in tables:
                return False
            cursor.execute("SELECT property FROM fs_meta;")
            for row in cursor.fetchall():
                if row[0] == "root":
                    return True
            return False

    @classmethod
    def validate_arg_path(cls, path: Union[PP, str]) -> PP:
        """Validates the 'path' argument for the constructor."""
        # Check type and convert to PurePath
        if isinstance(path, str):
            result = PP(path)
        elif isinstance(path, PP):
            result = path
        else:  # Raise error for anything but PurePath or str
            raise TypeError(f"path {path} must be a PurePath or str")

        # Validate what is at the path path and raise errors if needed
        if not os.path.isdir(result.parent):  # Check if parent dir is valid
            raise FileNotFoundError(f"{result} must be in a valid directory.")
        if os.path.exists(result) and not cls.is_scout_db_file(result):
            # If file exists, but isn't scout db,
            # trying to change irrelevant file could be dangerous.
            raise ValueError(f"{result} must be a valid scout db file or empty path.")

        return result

    @classmethod
    def validate_arg_root(cls, path: PP, root: Optional[Union[PP, str]]) -> PP:
        """Validates the 'root' argument for the constructor.
        if root is None:
            result = path.parent
        elif isinstance(root, str):
            result = PP(root)
        elif isinstance(root, PP):
            result = root
        else:
            msg = "DBConnector(path, root) (root) "
            msg += f"must be PurePath or str type, given {type(root)}"
            raise TypeError(msg)

        # Validate by filesystem to determine if raise needed
        if not os.path.isdir(result):
            msg = f"DBConnector(path, root) root must be a valid dir path, given {root}"
            raise FileNotFoundError(msg)
        return result

    @classmethod
    def read_root(cls, path: PP) -> PP:
        with sql.connect(path) as conn:
            c = conn.cursor()
            c.execute("SELECT value FROM fs_meta WHERE property='root';")
            res = c.fetchone()
            if res is None:
                raise sql.OperationalError("No root property in fs_meta table.")
            return PP(res[0])
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

    def __init__(
        self, path: Union[PP, str], root: Optional[Union[PP, str]] = None
    ) -> None:
        self.path = self.validate_arg_path(path)
        self.root = self.validate_arg_root(self.path, root)

        if not os.path.exists(self.path):
            # If path is empty, init a new scout db file.
            self.init_db(self.path, self.root)
        elif self.is_scout_db_file(self.path):
            self.root = DBConnector.read_root(self.path)
        else:  # The case where file exists but isn't scout db file
            raise ValueError(f"{self.path} must be empty or scout db file.")
