# TODO: SQL location and file handling should go to a separate module
#       That module should then call this and DirRepo to init tables.
from pathlib import PurePath as PP
from typing import Optional, Union, Tuple, List

from scoutlib.handler.db_connector import DBConnector as DBC

FileRow = Tuple[int, int, str, Optional[str], Optional[int], Optional[int]]


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
                dir_id iINTEGER,
                name TEXT NOT NULL,
                md5 TEXT,
                mtime INTEGER,
                updated INTEGER,
                FOREIGN KEY (dir_id) REFERENCES dir(id)
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
    # TODO: Could be a query method that other method combines with insert query like below:
    # INSERT INTO file (dir_id, name, md5, mtime, updated) SELECT id, ?, ?, ?, ? FROM dir WHERE path = ?
    def select_dir_where(
        self, id: Optional[int] = None, path: Optional[str] = None
    ) -> Optional[Tuple[int, str]]:
        """
        Select a directory record from the 'dir' table by either its ID or path.

        This method executes a SQL query to fetch the `id` and `path` of a directory
        from the 'dir' table based on the provided ID or path. If both `id` and `path`
        are provided, only the `id` is used for the query.

        Args:
            id (Optional[int]): The ID of the directory to fetch.
            path (Optional[str]): The path of the directory to fetch.

        Returns:
            Optional[Tuple[int, str]]: A tuple containing the `id` and `path` of the directory,
            or None if no matching directory is found.

        Raises:
            TypeError: If neither `id` nor `path` is provided.
        """
        query = "SELECT id, path FROM dir WHERE "
        if id is not None:
            query += f"id = {id};"
        elif path is not None:
            query += f"path = '{path}';"
        else:
            raise TypeError("Must provide either 'id' or 'path' argument.")
        with self.db.connect() as conn:
            c = conn.cursor()
            result = c.execute(query).fetchone()
            return result

    def select_files_where_query(
        self,
        id: Optional[int] = None,
        dir_id: Optional[int] = None,
        name: Optional[str] = None,
        md5: Optional[str] = None,
        mtime: Optional[int] = None,
        updated: Optional[int] = None,
    ) -> str:
        args = {
            "id": id,
            "dir_id": dir_id,
            "name": name,
            "md5": md5,
            "mtime": mtime,
            "updated": updated,
        }
        noargs = True
        for arg in args:
            if args[arg] is not None:
                noargs = False
                break
        if noargs:
            raise TypeError("Must provide at least one WHERE predicate argument.")
        # Start query string with SELECT & FROM clauses that wont change.
        q = "SELECT * FROM file WHERE "
        # Return early if id is provided since id is unique.
        if id is not None:
            q += f"id = {id};"
            return q
        # Add each non-None arg as WHERE clauses with 'AND' between each.
        for arg in args:
            if args[arg] is None:
                continue  # If arg is None, continue to next arg.
            elif isinstance(args[arg], str):  # Str needs quotes
                q += f"{arg} = '{args[arg]}' AND "
            else:
                q += f"{arg} = {args[arg]} AND "
        # Remove trailing ' AND ' and add semicolon to close query string.
        q = q[:-5] + ";"
        return q

    # TODO: Needs uniqueness check for combo of dir_id and name
    def insert_file_query(
        self,
        dir_id: int,
        name: str,
        md5: Optional[str] = None,
        mtime: Optional[int] = None,
        updated: Optional[int] = None,
    ) -> str:
        """
        Build a SQL query string to insert a new file record into the 'file' table.

        This method constructs an SQL `INSERT` statement to add a new record to the
        'file' table with the provided directory ID, name, and optional MD5 hash,
        modification time, and update time.

        Args:
            dir_id (int): The ID of the directory to which the file belongs.
            name (str): The name of the file.
            md5 (Optional[str]): The MD5 hash of the file (default is None).
            mtime (Optional[int]): The modification time of the file (default is None).
            updated (Optional[int]): The last updated time of the file (default is None).

        Returns:
            str: An SQL query string to insert a new file record.
        """
        # Start with beginning query fragment that will always be needed.
        q = "INSERT INTO file (\n"
        q += "  dir_id, name"
        # Add optional column names to column list if they are not None.
        q += ", md5" if md5 is not None else ""
        q += ", mtime" if mtime is not None else ""
        q += ", updated" if updated is not None else ""
        # Close column list and open values list with required values.
        q += f")\nVALUES (\n  {dir_id}, '{name}'"
        q += f", '{md5}'" if md5 is not None else ""  # Optional value if there
        q += f", {mtime}" if mtime is not None else ""  # Same
        q += f", {updated}" if updated is not None else ""  # Same
        q += ");"  # Close values list and end query string.
        return q
