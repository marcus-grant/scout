# TODO: SQL location and file handling should go to a separate module
#       That module should then call this and DirRepo to init tables.
from pathlib import PurePath as PP
from typing import Optional, Union

from scoutlib.handler.db_connector import DBConnector as DBC


class FileRepo:
    """
    Repository pattern class for managing sqlite storage layer of File objects.
    """

    db: DBC

    @classmethod
    def create_file_table(cls, db: DBC):
        """Create 'file' table in database within DBConnector."""
        query_schema = """
            CREATE TABLE IF NOT EXISTS file (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                name TEXT NOT NULL,
                md5 TEXT,
                mtime INTEGER,
                updated INTEGER
        );
        """
        with db.connect() as conn:
            c = conn.cursor()
            c.execute(query_schema)
            # TODO: Benchmark differences in size, memory, and speed when using indexes.
            # c.execute("CREATE INDEX IF NOT EXISTS file_md5 ON file (md5);")
            # c.execute("CREATE INDEX IF NOT EXISTS file_mtime ON file (mtime);")
            # c.execute("CREATE INDEX IF NOT EXISTS file_update ON file (update);")
            conn.commit()

    def __init__(self, db: DBC):
        """Initialize FileRepo with a DBConnector."""
        self.db = db
        if not self.db.table_exists("file"):
            self.create_file_table(self.db)

    ### SQL Query Methods ###
