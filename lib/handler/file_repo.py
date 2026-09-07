# TODO: Add 'size' column as optional integer to the table
# TODO: SQL location and file handling should go to a separate module
#       That module should then call this and DirRepo to init tables.
from datetime import UTC
from datetime import datetime as dt
from pathlib import PurePath as PP
from typing import Any

from lib.handler.db_connector import DBConnector as DBC
from lib.model.file import File
from lib.model.hash import HashMD5

FileRow = tuple[int, int, str, str | None, int | None, int | None]


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
                size INTEGER,
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
        self, id: int | None = None, path: str | None = None
    ) -> tuple[int, str] | None:
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
        id: int | None = None,
        dir_id: int | None = None,
        name: str | None = None,
        md5: str | None = None,
        mtime: int | None = None,
        updated: int | None = None,
    ) -> str:
        """Build a SELECT on the file table from the non-None arguments.

        If id is given it is the only predicate, since id is unique.
        Raises TypeError when every argument is None.
        """
        args = {
            "id": id,
            "dir_id": dir_id,
            "name": name,
            "md5": md5,
            "mtime": mtime,
            "updated": updated,
        }
        if all(val is None for val in args.values()):
            raise TypeError("Must provide at least one WHERE predicate argument.")
        q = "SELECT * FROM file WHERE "
        if id is not None:
            q += f"id = {id};"
            return q
        for arg, val in args.items():
            if val is None:
                continue
            elif isinstance(val, str):
                q += f"{arg} = '{val}' AND "
            else:
                q += f"{arg} = {val} AND "
        q = q[:-5] + ";"
        return q

    ### Repo Action Methods ###
    # TODO: Test that all but dir_id & id stays the same on return
    # TODO: Give interface to DirRepo to get dir_id from path or dir_id
    def add(self, files: list[File] | File) -> list[File]:
        if not isinstance(files, list):
            files = [files]
        inserted_files = []
        for file in files:
            dir_id = file.dir_id
            path = self.db.normalize_path(file.path)
            parent = path.parent
            with self.db.connect() as conn:
                c = conn.cursor()
                if dir_id is None:
                    if parent == PP("."):  # Handle files in repo root
                        dir_id = 0
                    else:
                        q_sel = "SELECT id from dir WHERE path = ?;"
                        dir_id = conn.execute(q_sel, (str(parent),)).fetchone()
                        if dir_id is None:
                            raise ValueError(
                                f"Attempting to insert file with no directory @{parent}"
                            )
                        dir_id = dir_id[0]
                else:
                    if dir_id != 0:
                        q_sel = "SELECT path from dir WHERE id = ?;"
                        parent = c.execute(q_sel, (dir_id,)).fetchone()
                        if parent is None or len(parent) == 0:
                            msg = f"Trying to insert file with no directory @{dir_id}"
                            raise ValueError(msg)
                        parent = self.db.normalize_path(parent[0])
                        path = parent / path.name
                    else:
                        parent = PP(".")
                        path = parent / path.name
                updated = int(dt.now(UTC).timestamp())
                vals = (
                    dir_id,
                    path.name,
                    file.md5.hex if file.md5 is not None else None,
                    file.size,
                    int(file.mtime.timestamp()) if file.mtime is not None else None,
                    updated,
                )
                q_ins = "INSERT INTO file (dir_id, name, md5, size, mtime, updated) "
                q_ins += "VALUES (?, ?, ?, ?, ?, ?);"
                c.execute(q_ins, vals)
                id = c.lastrowid
                if id is None:
                    raise ValueError(f"Failed to insert file record of File {file}")
                # Now collect all attrs from file & dir_id & id to return the new File object
                path = self.db.denormalize_path(path)
                inserted_files.append(
                    File(
                        path,
                        id=id,
                        dir_id=dir_id,
                        md5=file.md5,
                        size=file.size,
                        mtime=file.mtime,
                        updated=dt.fromtimestamp(updated, tz=UTC),
                    )
                )
        return inserted_files

    # TODO: WHen more mature, add get methods for specific FileRepo interactions
    # TODO: Needs to query for dir_id and name based on a path filter
    def get(self, **filters: Any | None) -> list[File]:
        """
        Retrieve files from the 'file' table based on various filtering criteria.

        Args:
            **filters: Arbitrary keyword arguments corresponding to the columns in the 'file' table.
                        - NOTE: Every filter is an 'AND' condition with the others.
                        - size__lt: Gets files with size less than value passed keyword.
                        - md5__ne: Gets files that don't have the md5 hash value.
                        - dir_id: Gets default '=' operator when querying for dir_id.

        Returns:
            List[File]: A list of File objects representing the rows from the 'file' table that match the filters.

        Example:
            files = repo.get(name='example_file.txt', mtime__gt=1234567890)
            # This retrieves all files with name 'example_file.txt' and modification time greater than 1234567890.
        """
        query = (
            "SELECT f.id, f.dir_id, f.name, f.md5, f.size, f.mtime, f.updated, d.path "
        )
        query += "FROM file f "
        query += "LEFT JOIN dir d ON f.dir_id = d.id WHERE "
        conditions = []
        params = []
        path = filters.pop("path", None)

        # Path can be more useful to determine both f.name & d.path simultaneously
        # However, if dir_id exists a conflict of which foreign key to use exists
        if path is not None and "dir_id" not in filters:
            if not isinstance(path, (str, PP)):
                raise ValueError("Path filter must be a string or PurePath.")
            # If no dir_id use path.parent to get d.path of joined table and name
            path = self.db.normalize_path(path)
            filters["path"] = str(path.parent)  # Since it's path of parent
            filters["name"] = str(path.name)
        # Do nothing if dir_id in filters since path is already popped

        # MD5 could be a HashMD5 object, convert to hex string if so
        if (md5 := filters.get("md5")) and isinstance(md5, HashMD5):
            filters["md5"] = md5.hex
            del md5

        for key, value in filters.items():
            append_param = True
            if "__" in key:
                column, operator = key.split("__")
                column = f"f.{column}"
                if operator == "gt":
                    conditions.append(f"{column} > ?")
                elif operator == "lt":
                    conditions.append(f"{column} < ?")
                elif operator == "ge":
                    conditions.append(f"{column} >= ?")
                elif operator == "le":
                    conditions.append(f"{column} <= ?")
                elif operator == "ne":
                    conditions.append(f"{column} != ?")
                elif operator == "null":
                    append_param = False
                    if value:
                        conditions.append(f"{column} IS NULL")
                    else:
                        conditions.append(f"{column} IS NOT NULL")
                else:
                    raise ValueError(f"Unsupported operator: {operator}")
            else:
                conditions.append(f"f.{key} = ?")
            if append_param:
                params.append(value)
        query += " AND ".join(conditions) + ";"
        # Handle special case where path is the only selection in dir table
        query = query.replace("f.path", "d.path")

        query_all = (
            "SELECT f.id, f.dir_id, f.name, f.md5, f.size, f.mtime, f.updated, d.path "
        )
        query_all += "FROM file f LEFT JOIN dir d ON f.dir_id = d.id;"

        with self.db.connect() as conn:
            c = conn.cursor()
            if len(filters) == 0:
                c.execute(query_all)
            else:
                c.execute(query, params)
            rows = c.fetchall()

        # Marshal all paths and do paraticular assignment where None is root paths
        paths = [r[2] if r[7] is None else f"{r[7]}/{r[2]}" for r in rows]

        files = [
            File(
                path=self.db.denormalize_path(paths[i]),
                dir_id=row[1],
                id=row[0],
                md5=row[3],
                size=row[4],
                mtime=dt.fromtimestamp(row[5], tz=UTC) if row[5] is not None else None,
                updated=dt.fromtimestamp(row[6], tz=UTC)
                if row[6] is not None
                else None,
            )
            for i, row in enumerate(rows)
        ]
        return files
