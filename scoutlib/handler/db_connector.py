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
        if not DBConnector.is_db_file(self.path):
            raise ValueError(f"{self.path} must be a valid scout database file.")

        # Set root arg
        if root is None:
            self.root = self.path.parent
        elif isinstance(root, str):
            self.root = PP(root)
        else:
            self.root = root
        if not isinstance(self.root, PP):
            raise TypeError(f"root {root} must be a PurePath or str")
        if not os.path.isdir(self.root):
            raise ValueError(f"{self.root} must be a valid directory.")
        if isinstance(root, PP):
            self.root = root

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

    def _init_db(self):
        """Initializes db file as sqlite db with fs_meta table.
        Should only be run when it's a db file and no fs_meta table exists.
        """
        pass
