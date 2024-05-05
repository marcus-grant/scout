# TODO: Init needs to check for preexisting db & use future CfgRepo to read it before other init steps.
#       This will also require extra rewrites of init.
import sqlite3 as sql
from pathlib import PurePath as PP
from typing import Optional, Union

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

    path_db: PP
    path_fs: PP

    def __init__(
        self, path_db: Union[str, PP], path_fs: Optional[Union[str, PP]] = None
    ):
        if isinstance(path_db, str):
            path_db = PP(path_db)
        self.path_db = path_db
        if path_fs is None:
            self.path_fs = path_db.parent
        else:
            self.path_fs = PP(path_fs)
        self._init_db()

    def _init_db(self):
        pass
