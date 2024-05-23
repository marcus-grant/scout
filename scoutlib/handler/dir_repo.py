# NOTE: Consider an index for for dir.name & adding regular col for dir.depth
# this could speed up join queries on paths
# NOTE: Need to find a more normalized way to query against path;
# we may keep path for ease of offline analysis, but during operations,
# we should be using either a closure or self referencing parent FK.
# NOTE: If we go with materialized paths, they should probably be the PK.
# NOTE: The way we handle query building is messy, consider a query builder & refactor
# TODO: Ensure docstrings are on every method
# TODO: Refactor to use DBConnector instead of path and root & remove methods from it.
import sqlite3
from pathlib import PurePath as PP
from typing import Optional, Union, List, Tuple

from scoutlib.model.dir import Dir
from scoutlib.handler.db_connector import DBConnector as DBC


DIR_TABLE = "dir"
DIR_ANCESTOR_TABLE = "dir_ancestor"
DEFAULT_DEPTH = 2**31 - 1


class DirRepo:
    """
    Repository pattern class for managing storage layer of
    Dir objects in a SQLite database.
    """

    db: DBC

    @classmethod
    def create_dir_table(cls, db: DBC):
        """
        Create the dir table in the database.
        """
        query = """ CREATE TABLE IF NOT EXISTS dir (
                        id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                        path TEXT NOT NULL,
                        CONSTRAINT path_unique UNIQUE (path)
        );"""
        with db.connect() as conn:
            conn.execute(query)
            conn.commit()

    @classmethod
    def create_dir_ancestor_table(cls, db: DBC):
        """
        Create the dir_ancestor table in the database.
        """
        query = """CREATE TABLE IF NOT EXISTS dir_ancestor (
                        dir_id INTEGER NOT NULL,
                        ancestor_id INTEGER NOT NULL,
                        depth INTEGER NOT NULL,
                        PRIMARY KEY (dir_id, ancestor_id),
                        FOREIGN KEY (dir_id) REFERENCES dir(id),
                        FOREIGN KEY (ancestor_id) REFERENCES dir(id)
        );"""
        with db.connect() as conn:
            conn.execute(query)
            conn.commit()

    def __init__(self, db_connector: DBC):
        self.db = db_connector
        if not self.db.table_exists(DIR_TABLE):
            self.create_dir_table(self.db)
        if not self.db.table_exists(DIR_ANCESTOR_TABLE):
            self.create_dir_ancestor_table(self.db)

    #  ### SQL Query Helper Methods ###

    # TODO: Benchmark this, no server so latency not a concern, but could be slow
    def insert_dir(self, path: Union[PP, str]) -> Optional[int]:
        """
        Inserts a new record into the 'dir' table with the given path.

        Args:
            path (Union[PP, str]): The directory path to insert.

        Returns:
            Optional[int]: The ID of the new record if added, otherwise None.
        """
        np = self.db.normalize_path(path)
        id = None
        query_id = "SELECT id FROM dir WHERE path = ?"
        query_insert = "INSERT INTO dir (path) VALUES (?)"
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query_id, (str(np),))
            id = cursor.fetchone()
            if id:  # If it exists it's a duplicate just return the id
                return id[0]
            cursor.execute(query_insert, (str(np),))
            return cursor.lastrowid

    def insert_dir_ancestor(self, dir_ancestor_rows: list[tuple[int, int, int]]):
        """
        Inserts multiple records into the 'dir_ancestor' table.

        Args:
            dir_ancestor_rows (List[Tuple[int, int, int]]): List of tuples containing dir_id, ancestor_id, and depth.
        """
        with self.db.connect() as conn:
            c = conn.cursor()
            for row in dir_ancestor_rows:
                c.execute(
                    """INSERT INTO dir_ancestor (dir_id, ancestor_id, depth)
                    VALUES (?, ?, ?) ON CONFLICT DO NOTHING""",
                    row,
                )
            conn.commit()

    def select_dir_where_path(self, path: str) -> Optional[tuple[int, str]]:
        """Basic query execution helper that
        selects a 'dir' table row WHERE path = passed path"""
        res = None  # Result
        with self.db.connect() as conn:
            q = f"SELECT * FROM dir WHERE path = '{path}'"
            res = conn.execute(q).fetchone()
        return res

    def select_dir_where_id(self, id: int) -> Optional[tuple[int, str, str]]:
        """
        Selects a 'dir' table row where id matches the given id.

        Args:
            id (int): The directory ID to search for.

        Returns:
            Optional[Tuple[int, str]]: The matching row or None if no match found.
        """
        res = None  # Result
        with self.db.connect() as conn:
            q = f"SELECT * FROM dir WHERE id = {id}"
            res = conn.execute(q).fetchone()
        return res

    def select_ancestors_where_path(
        self,
        path: str,
        depth: Optional[int] = DEFAULT_DEPTH,
    ) -> List[Tuple[int, str]]:
        """
        Selects ancestor directories for a given path up to a specified depth.

        Args:
            path (str): The directory path to find ancestors for.
            depth (Optional[int]): The maximum depth of the ancestor search. Defaults to the maximum possible depth.

        Returns:
            List[Tuple[int, str]]: A list of tuples representing the ancestor directories.
        """
        if depth is None:
            depth = DEFAULT_DEPTH
        with self.db.connect() as conn:
            query = """
                SELECT ancestor_dirs.*
                FROM (
                    SELECT d.id AS target_dir_id
                    FROM dir d
                    WHERE d.path = ?
                ) AS target_dir
                JOIN dir_ancestor da ON target_dir.target_dir_id = da.dir_id
                JOIN dir ancestor_dirs ON da.ancestor_id = ancestor_dirs.id
                WHERE da.depth <= ? AND da.depth > 0
                ORDER BY da.depth
            """
            res = conn.execute(query, (path, depth)).fetchall()
        return res

    def select_ancestors_where_id(
        self, id: int, depth: Optional[int] = DEFAULT_DEPTH
    ) -> List[Tuple[int, str, str]]:
        """
        Selects ancestor directories for a given directory ID up to a specified depth.

        Args:
            id (int): The directory ID to find ancestors for.
            depth (Optional[int]): The maximum depth of the ancestor search. Defaults to the maximum possible depth.

        Returns:
            List[Tuple[int, str]]: A list of tuples representing the ancestor directories.
        """
        if depth is None:
            depth = DEFAULT_DEPTH
        with self.db.connect() as conn:
            query = """
                SELECT ancestor_dirs.*
                FROM (
                    SELECT d.id AS target_dir_id
                    FROM dir d
                    WHERE d.id = ?
                ) AS target_dir
                JOIN dir_ancestor da ON target_dir.target_dir_id = da.dir_id
                JOIN dir ancestor_dirs ON da.ancestor_id = ancestor_dirs.id
                WHERE da.depth <= ? AND da.depth > 0
                ORDER BY da.depth
            """
            res = conn.execute(query, (id, depth)).fetchall()
        return res

    def select_descendants_where_path(
        self,
        path: str,
        depth: Optional[int] = DEFAULT_DEPTH,
    ) -> List[Tuple[int, str, str]]:
        """
        Selects descendant directories for a given path up to a specified depth.

        Args:
            path (str): The directory path to find descendants for.
            depth (Optional[int]): The maximum depth of the descendant search. Defaults to the maximum possible depth.

        Returns:
            List[Tuple[int, str]]: A list of tuples representing the descendant directories.
        """
        if depth is None:
            depth = DEFAULT_DEPTH
        with self.db.connect() as conn:
            query = """
                SELECT descendant_dirs.*
                FROM (
                    SELECT d.id AS target_dir_id
                    FROM dir d
                    WHERE d.path = ?
                ) AS target_dir
                JOIN dir_ancestor da ON target_dir.target_dir_id = da.ancestor_id
                JOIN dir descendant_dirs ON da.dir_id = descendant_dirs.id
                WHERE da.depth <= ? AND da.depth > 0
                ORDER BY da.depth
            """
            res = conn.execute(query, (path, depth)).fetchall()
        return res

    # TODO: Fix depth checks not working as expected in test_get_descendandants_dirs #2
    def select_descendants_where_id(
        self, id: int, depth: Optional[int] = DEFAULT_DEPTH
    ) -> List[Tuple[int, str]]:
        """
        Selects descendant directories for a given directory ID up to a specified depth.

        Args:
            id (int): The directory ID to find descendants for.
            depth (Optional[int]): The maximum depth of the descendant search. Defaults to the maximum possible depth.

        Returns:
            List[Tuple[int, str]]: A list of tuples representing the descendant directories.
        """
        if depth is None:
            depth = DEFAULT_DEPTH
        res = []
        with self.db.connect() as conn:
            query = """
                SELECT descendant_dirs.*
                FROM (
                    SELECT d.id AS target_dir_id
                    FROM dir d
                    WHERE d.id = ?
                ) AS target_dir
                JOIN dir_ancestor da ON target_dir.target_dir_id = da.ancestor_id
                JOIN dir descendant_dirs ON da.dir_id = descendant_dirs.id
                WHERE da.depth <= ? AND da.depth > 0
                ORDER BY da.depth
            """
            res = conn.execute(query, (id, depth)).fetchall()
        return res

    def add(self, dir: Dir) -> list[Dir]:
        # TODO: Come back to this method later when we know more how to use it.
        # NOTE: There's a problem of how we handle ids here,
        # it might be better to allow raising errors on adding dirs without parent.
        """
        Adds a Dir object's data to the database.

        Args:
            dir (Dir): The directory object to add.

        Returns:
            List[Dir]: A list of Dir objects representing the added directories.
        """
        # Normalize Leaf Dir Path (lp) to repo
        lp = self.db.normalize_path(dir.path)
        aps = self.db.ancestor_paths(lp)  # Get ancestor paths (aps)
        # Add all ancestors to dir table noting that duplicates will be ignored
        ids = []
        for ap in aps:
            id = self.insert_dir(ap)
            ids.append(id)
        dir.id = ids[-1]  # Ensure last id on leaf dir id

        #       # Now we need to arrange the dir_ancestor rows (da_rows)
        da_rows = []
        for i, ap in enumerate(aps):
            for j in range(i, -1, -1):  # Reverse order from i to 0 of ids
                da_rows.append((ids[i], ids[j], i - j))
        self.insert_dir_ancestor(da_rows)  # Insert rows to dir_ancestor

        # Now create directories with assigned ids and other attrs given
        daps = [self.db.denormalize_path(ap) for ap in aps]
        dirs = [Dir(path=ap, id=ids[i]) for i, ap in enumerate(daps)]
        return dirs

    def getone(
        self,
        id: Optional[int] = None,
        path: Optional[Union[PP, str]] = None,
        dir: Optional[Dir] = None,
    ) -> Optional[Dir]:
        """
        Retrieves a single directory based on id, path, or Dir object.

        Args:
            id (Optional[int]): The directory ID.
            path (Optional[Union[PP, str]]): The directory path.
            dir (Optional[Dir]): The directory object.

        Returns:
            Optional[Dir]: The matching Dir object or None if not found.
        """
        id_used = None
        path_used = None
        if id is not None:
            id_used = id
        elif path is not None:
            path_used = path
        elif dir is not None:
            id_used = dir.id
            path_used = dir.path
        else:
            raise ValueError("Must provide either id, path, or dir argument.")
        res = None
        if id_used:
            res = self.select_dir_where_id(id_used)
        elif path_used:
            path_used = str(self.db.normalize_path(path_used))
            res = self.select_dir_where_path(path_used)
        if res is None:
            return None
        return Dir(id=res[0], path=self.db.denormalize_path(res[1]))

    def get_ancestors(
        self,
        id: Optional[int] = None,
        path: Optional[Union[PP, str]] = None,
        dir: Optional[Dir] = None,
        depth: int = DEFAULT_DEPTH,
    ) -> list[Dir]:
        """
        Retrieves ancestor directories up to a specified depth.

        Args:
            id (Optional[int]): The directory ID.
            path (Optional[Union[PP, str]]): The directory path.
            dir (Optional[Dir]): The directory object.
            depth (int): The maximum depth of the ancestor search. Defaults to the maximum possible depth.

        Returns:
            List[Dir]: A list of Dir objects representing the ancestor directories.
        """
        id_used = None
        path_used = None
        if id is not None:
            id_used = id
        elif path is not None:
            path_used = path
        elif dir is not None:
            id_used = dir.id
            path_used = dir.path
        else:
            raise ValueError("Must provide either id, path, or dir argument.")
        rows = []
        if id_used:
            rows = self.select_ancestors_where_id(id_used, depth)
        elif path_used:
            path_used = str(self.db.normalize_path(path_used))
            rows = self.select_ancestors_where_path(path_used, depth)
        else:
            raise ValueError("Must provide either id or path argument.")

        fn_dp = self.db.denormalize_path
        dirs = [Dir(id=r[0], path=fn_dp(r[1])) for r in rows]
        return dirs

    def get_descendants(
        self,
        id: Optional[int] = None,
        path: Optional[Union[PP, str]] = None,
        dir: Optional[Dir] = None,
        depth: int = DEFAULT_DEPTH,
    ) -> list[Dir]:
        """
        Retrieves descendant directories up to a specified depth.

        Args:
            id (Optional[int]): The directory ID.
            path (Optional[Union[PP, str]]): The directory path.
            dir (Optional[Dir]): The directory object.
            depth (int): The maximum depth of the descendant search. Defaults to the maximum possible depth.

        Returns:
            List[Dir]: A list of Dir objects representing the descendant directories.
        """
        id_used = None
        path_used = None
        if id is not None:
            id_used = id
        elif path is not None:
            path_used = path
        elif dir is not None:
            id_used = dir.id
            path_used = dir.path
        else:
            raise ValueError("Must provide either id, path, or dir argument.")
        rows = []
        if id_used:
            rows = self.select_descendants_where_id(id_used, depth)
        elif path_used:
            path_used = str(self.db.normalize_path(path_used))
            rows = self.select_descendants_where_path(path_used, depth)
        else:
            raise ValueError(
                "Must provide either id or path argument individually or in a dir object."
            )

        fn_dp = self.db.denormalize_path
        dirs = [Dir(id=r[0], path=fn_dp(r[1])) for r in rows]
        return dirs
