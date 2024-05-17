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
from typing import Optional, Union

from scoutlib.model.dir import Dir
from scoutlib.handler.db_connector import DBConnector as DBC


DIR_TABLE = "dir"
DIR_ANCESTOR_TABLE = "dir_ancestor"


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

    # TODO: Benchmark this, less round trip latency than server conn, but could be slow
    def insert_dir(self, path: Union[PP, str]) -> Optional[int]:
        """
        Inserts a new record into the dir table with the given name and path.
        Returns the id of the new record if added, or None if not inserted due to
        an existing record with the same path.
        Disk I/O latencies are the primary concern, not round trip considerations.
        :param path: Path to insert into the dir table.
        :return: ID of the new record if added, otherwise None.
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
            else:
                cursor.execute(query_insert, (str(np),))
                return cursor.lastrowid


#  def insert_into_dir_ancestor(
#      self,
#      dir_ancestor_rows: list[tuple[int, int, int]],  # dir_id, ancestor_id, depth
#  ):
#      """
#      Inserts a dir_ancestor records into the dir_ancestor table.
#      """
#      ""
#      with self.connection() as conn:
#          cursor = conn.cursor()
#          for row in dir_ancestor_rows:
#              cursor.execute(
#                  """
#                  INSERT INTO dir_ancestor (dir_id, ancestor_id, depth)
#                  VALUES (?, ?, ?) ON CONFLICT DO NOTHING""",
#                  row,
#              )
#          conn.commit()

#  def select_dir_where_path(
#      self, path: Union[Dir, PurePath, str]
#  ) -> Optional[tuple[int, str, str]]:
#      np = self.normalize_path(path)
#      res = None  # Result
#      with self.connection() as conn:
#          query = "SELECT * FROM dir WHERE path = ?"
#          res = conn.execute(query, (str(np),)).fetchone()
#      return res

#  def select_dir_where_id(self, id: int) -> Optional[tuple[int, str, str]]:
#      """Returns the dir row tuple with matching id, or None if no match."""
#      res = None  # Result
#      with self.connection() as conn:
#          query = "SELECT * FROM dir WHERE id = ?"
#          res = conn.execute(query, (id,)).fetchone()
#      return res

#  def ancestor_dirs_where_path(
#      self,
#      path: Union[PurePath, str],
#      depth: Optional[int] = 2**31 - 1,
#  ) -> list[tuple[int, str, str]]:
#      np = self.normalize_path(path)
#      if depth is None:
#          depth = 2**31 - 1
#      with self.connection() as conn:
#          query = """
#              SELECT ancestor_dirs.*
#              FROM ( -- query for dir.id with given path
#                  SELECT d.id AS target_dir_id
#                  FROM dir d
#                  WHERE d.path = ?
#              ) AS target_dir -- target_dir.id now holds dir.id of target path
#              JOIN dir_ancestor da ON target_dir.target_dir_id = da.dir_id
#              JOIN dir ancestor_dirs ON da.ancestor_id = ancestor_dirs.id
#              WHERE da.depth <= ? and da.depth > 0
#              ORDER BY da.depth
#          """
#          res = conn.execute(query, (str(np), depth)).fetchall()
#          return res

#  def ancestor_dirs_where_id(
#      self, id: int, depth: Optional[int] = 2**31 - 1
#  ) -> list[tuple[int, str, str]]:
#      if depth is None:
#          depth = 2**31 - 1
#      with self.connection() as conn:
#          query = """
#          SELECT ancestor_dirs.*
#          FROM ( -- query for dir.id with given id
#              SELECT d.id AS target_dir_id
#              FROM dir d
#              WHERE d.id = ?
#          ) AS target_dir -- target_dir.id now holds dir.id of target path
#          JOIN dir_ancestor da ON target_dir.target_dir_id = da.dir_id
#          JOIN dir ancestor_dirs ON da.ancestor_id = ancestor_dirs.id
#          WHERE da.depth <= ? and da.depth > 0
#          ORDER BY da.depth
#          """
#          res = conn.execute(query, (id, depth)).fetchall()
#          return res

#  def descendant_dirs_where_path(
#      self,
#      path: Union[PurePath, str],
#      depth: Optional[int] = 2**31 - 1,
#  ) -> list[tuple[int, str, str]]:
#      if depth is None:
#          depth = 2**31 - 1
#      np = self.normalize_path(path)
#      res = []
#      with self.connection() as conn:
#          query = """
#              SELECT descendant_dirs.*
#              FROM ( -- query for dir.id with given path
#                  SELECT d.id AS target_dir_id
#                  FROM dir d
#                  WHERE d.path = ?
#              ) AS target_dir -- target_dir.id now holds dir.id of target path
#              JOIN dir_ancestor da ON target_dir.target_dir_id = da.ancestor_id
#              JOIN dir descendant_dirs ON da.dir_id = descendant_dirs.id
#              WHERE da.depth <= ? and da.depth > 0
#              ORDER BY da.depth
#          """
#          res = conn.execute(query, (str(np), depth)).fetchall()
#      return res

#  # TODO: Fix depth checks not working as expected in test_get_descendandants_dirs #2
#  def descendant_dirs_where_id(
#      self, id: int, depth: Optional[int] = 2**31 - 1
#  ) -> list[tuple[int, str, str]]:
#      if depth is None:
#          depth = 2**31 - 1
#      res = []
#      with self.connection() as conn:
#          query = """
#          SELECT descendant_dirs.*
#          FROM ( -- query for dir.id with given id
#              SELECT d.id AS target_dir_id
#              FROM dir d
#              WHERE d.id = ?
#          ) AS target_dir -- target_dir.id now holds dir.id of target path
#          JOIN dir_ancestor da ON target_dir.target_dir_id = da.ancestor_id
#          JOIN dir descendant_dirs ON da.dir_id = descendant_dirs.id
#          WHERE da.depth <= ? and da.depth > 0
#          ORDER BY da.depth
#          """
#          res = conn.execute(query, (id, depth)).fetchall()
#      return res

#  ### Repo Actions ###
#  def add(self, dir: Dir) -> list[Dir]:
#      # TODO: Come back to this method later when we know more how to use it.
#      # NOTE: There's a problem of how we handle ids here,
#      # it might be better to allow raising errors on adding dirs without parent.
#      """
#      Adds a Dir object's data to the database.
#      This includes:
#          - Splitting ancestors in path into separate records
#          - Adding the directory itself
#          - Adding to dir_ancestor table for each ancestor
#          - In-place updating the passed directory object with its id
#      """
#      # Normalize Leaf Dir Path (lp) to repo
#      lp = self.normalize_path(dir.path)
#      aps = self.ancestor_paths(lp)  # Get ancestor paths (aps)
#      # Add all ancestors to dir table noting that duplicates will be ignored
#      ids = []
#      for ap in aps:
#          id = self.insert_into_dir(ap.name, ap)
#          ids.append(id)
#      dir.id = ids[-1]  # Ensure last id on leaf dir id

#      # Now we need to arrange the dir_ancestor rows (da_rows)
#      da_rows = []
#      for i, ap in enumerate(aps):
#          for j in range(i, -1, -1):  # Reverse order from i to 0 of ids
#              da_rows.append((ids[i], ids[j], i - j))
#      self.insert_into_dir_ancestor(da_rows)  # Insert rows to dir_ancestor

#      # Now create directories with assigned ids and other attrs given
#      daps = [self.denormalize_path(ap) for ap in aps]
#      dirs = [Dir(path=ap, id=ids[i]) for i, ap in enumerate(daps)]
#      return dirs

#  def getone(
#      self,
#      id: Optional[int] = None,
#      path: Optional[Union[PurePath, str]] = None,
#      dir: Optional[Dir] = None,
#  ) -> Optional[Dir]:
#      id_used = None
#      path_used = None
#      if id is not None:
#          id_used = id
#      elif path is not None:
#          path_used = path
#      elif dir is not None:
#          id_used = dir.id
#      else:
#          raise ValueError("Must provide either id, path, or dir argument.")
#      res = None
#      if id_used:
#          res = self.select_dir_where_id(id_used)
#      elif path_used:
#          res = self.select_dir_where_path(self.normalize_path(path_used))
#      if res:
#          return Dir(id=res[0], path=self.denormalize_path(res[2]))
#      return None

#  def get_ancestors(
#      self,
#      id: Optional[int] = None,
#      path: Optional[Union[PurePath, str]] = None,
#      dir: Optional[Dir] = None,
#      depth: int = 2**31 - 1,
#  ) -> list[Dir]:
#      """
#      Gets ancestor directories from repo of a given directory's id or path.
#      Also limits results to a given depth from the given directory.
#      """
#      given_id = dir.id if dir else None
#      given_id = id if id else given_id
#      given_path = str(dir.path) if dir else None
#      given_path = path if path else given_path
#      rows = []

#      if given_id:
#          rows = self.ancestor_dirs_where_id(given_id, depth)
#      elif given_path:
#          rows = self.ancestor_dirs_where_path(given_path, depth)
#      else:
#          raise ValueError("Must provide either id or path argument.")

#      fn_dp = self.denormalize_path
#      dirs = [Dir(id=r[0], path=fn_dp(r[2])) for r in rows]
#      return dirs

#  def get_descendants(
#      self,
#      id: Optional[int] = None,
#      path: Optional[Union[PurePath, str]] = None,
#      dir: Optional[Dir] = None,
#      depth: int = 2**31 - 1,
#  ) -> list[Dir]:
#      """
#      Gets descendant directories from repo of a given directory's id or path.
#      Also limits results to a given depth from the given directory.
#      """
#      given_id = dir.id if dir else None
#      given_id = id if id else given_id
#      given_path = str(dir.path) if dir else None
#      given_path = path if path else given_path
#      rows = []

#      if given_id:
#          rows = self.descendant_dirs_where_id(given_id, depth)
#      elif given_path:
#          rows = self.descendant_dirs_where_path(given_path, depth)
#      else:
#          raise ValueError(
#              "Must provide either id or path argument individually or in a dir object."
#          )

#      fn_dp = self.denormalize_path
#      dirs = [Dir(id=r[0], path=fn_dp(r[2])) for r in rows]
#      return dirs

#      # fn_dp = self.denormalize_path
#      # dirs = [Dir(id=r[0], path=fn_dp(r[2])) for r in rows]
#      # return dirs
#      # )
