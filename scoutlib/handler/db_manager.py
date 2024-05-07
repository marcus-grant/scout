# TODO: Init needs to check for preexisting db & use future CfgRepo to read it before other init steps.
#       This will also require extra rewrites of init.
import os
from pathlib import PurePath as PP
from typing import Optional, Union
import sqlite3 as sql

from scoutlib.model.dir import Dir
from scoutlib.model.file import File
from scoutlib.handler.dir_repo import DirRepo
from scoutlib.handler.file_repo import FileRepo


class DBManager:
    """
    The root class for managing the sqlite database.
    Sets up the database where it is configured to be.
    Then it sets up the Repo classes for their associated tables.
    """

    path: PP  # path to the db (can be separate from repo root)
    root: PP  # path to the repo root in filesystem

    def __init__(
        self,
        path_db: Union[str, PP],
        path_fs: Optional[Union[str, PP]] = None,
    ):
        # Ensure path_db is a PurePath object
        if isinstance(path_db, str):
            path_db = PP(path_db)  # Convert string to PurePath

        if not isinstance(path_db, PP):
            raise ValueError(f"Invalid path_db: {path_db}")
        self.path = path_db

        # Handle path_fs and ensure it's a PurePath object
        if path_fs is None:
            # Default repo root is the parent of the db path
            self.root = self.path.parent
        elif isinstance(path_fs, str):
            self.root = PP(path_fs)  # Convert string to PurePath
        elif isinstance(path_fs, PP):
            self.root = path_fs
        else:
            raise ValueError(f"Invalid path_fs: {path_fs}")

        # If no db file exists create it
        if not os.path.exists(self.path):
            self._init_db()
        else:
            if os.path.isdir(self.path.parent):
                # If it already exists but no fs_meta table, create it
                with sql.connect(self.path) as conn:
                    # Check if the fs_meta table exists
                    c = conn.cursor()
                    c.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    if ("fs_meta",) not in c.fetchall():
                        self._init_db()

    def _init_db(self):
        """Initialize the database with the filesystem table, only necessary one"""
        with sql.connect(self.path) as conn:
            try:
                query = """
                    CREATE TABLE fs_meta (
                        property TEXT PRIMARY KEY,
                        value TEXT
                    );
                """
                conn.cursor().execute(query)
                query = """INSERT INTO fs_meta (property, value) VALUES ('root', ?)"""
                conn.cursor().execute(query, (str(self.root),))
                conn.commit()
            except sql.Error as e:
                print("An error occurred:", e)
