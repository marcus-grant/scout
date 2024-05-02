# TODO: SQL location and file handling should go to a separate module
#       That module should then call this and DirRepo to init tables.
import sqlite3
from pathlib import PurePath as PP
from typing import Optional, Union


class FileRepo:
    """
    Repository pattern class for managing sqlite storage layer of File objects.
    """

    path_db: PP
    path: PP

    def __init__(self, path_db: Union[str, PP], path: Optional[Union[str, PP]] = None):
        if isinstance(path_db, str):
            path_db = PP(path_db)
        self.path_db = path_db
        if path is None:
            self.path = path_db.parent
        else:
            self.path = PP(path)
        self._init_db()

    def _init_db(self):
        """Init db & create file table if not there."""
        with sqlite3.connect(self.path_db) as conn:
            # Create the file table with correct SQL for PRIMARY KEY and handling the 'update' keyword.
            query = """
                CREATE TABLE IF NOT EXISTS file (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    parent_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    md5 TEXT,
                    mtime INTEGER,
                    updated INTEGER,  -- update is a keyword updated is used instead
                    FOREIGN KEY (parent_id) REFERENCES dir(id)
                );
            """
            conn.execute(query)
            # Create an index on the md5 column
            index_query = """
                CREATE INDEX IF NOT EXISTS md5_idx ON file (md5);
            """
            conn.execute(index_query)
            conn.commit()
