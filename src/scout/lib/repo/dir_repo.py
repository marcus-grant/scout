# src/scout/lib/repo/dir_repo.py
"""DirRepo: the dir table, paths relative to root, queried by prefix.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.models import Dir
from scout.lib.repo.db_connector import DBConnector


class DirRepo:
    """Owns dir(id, path, gone); every query hides gone rows."""

    SCHEMA = """CREATE TABLE IF NOT EXISTS dir (
        id INTEGER PRIMARY KEY,
        path TEXT NOT NULL UNIQUE,
        gone INTEGER REFERENCES scan(started)
    ); INSERT OR IGNORE INTO dir (id, path) VALUES (0, '.');"""

    _SELECT = """SELECT id, path, gone FROM dir
                    WHERE GONE IS NULL AND ({}) ORDER BY path;"""

    _UPSERT = """INSERT INTO dir (path) VALUES (?)
                    ON CONFLICT(path) DO UPDATE SET gone = NULL;"""

    _UPDATE_GONE = "UPDATE dir SET gone = ? WHERE {};"

    def __init__(self, db: DBConnector) -> None:
        """Bind to the manifest db shares."""
        self.db = db

    @staticmethod
    def _check(path: PPP) -> str:
        """Return path as text or raise Err.NotUnderRoot if it leaves root."""
        if path.is_absolute() or ".." in path.parts:
            raise Err.NotUnderRoot(f"not under root: {path}", path=path)
        return str(path)

    @staticmethod
    def _row_to_dir(row: tuple[int, str, int | None]) -> Dir:
        """Build a Dir from one (id, path, gone) row."""
        return Dir(id=row[0], path=PPP(row[1]), gone=row[2])

    @staticmethod
    def _rows_to_dirs(rows: list[tuple[int, str, int | None]]) -> list[Dir]:
        """Map a list of dir rows (id, path, gone) to a list of Dir."""
        return [DirRepo._row_to_dir(r) for r in rows]

    @staticmethod
    def _where_descendants(parent: str) -> tuple[str, tuple[str, ...]]:
        """WHERE clause and params selecting dirs strictly under _path."""
        if parent == ".":
            return "id != 0", ()
        return "path >= ? AND path < ?", (f"{parent}/", f"{parent}0")

    @staticmethod
    def _where_descendants_or_parent(parent: str) -> tuple[str, tuple[str, ...]]:
        """WHERE clause and params selecting _path itself and dirs under it."""
        where, params = DirRepo._where_descendants(parent)
        return f"path = ? OR ({where})", (parent, *params)

    def _select_dirs(self, where: str, params: tuple = ()) -> list[Dir]:
        """Run _SELECT with where & params: id, path, gone FROM dir, ordered by path."""
        rows = self.db.conn.execute(self._SELECT.format(where), params).fetchall()
        return DirRepo._rows_to_dirs(rows)

    def add(self, path: PPP) -> Dir:
        """Upsert path as live and return its Dir; a gone row is revived."""
        _path = self._check(path)
        for p in (*reversed(path.parents), path):
            if p == PPP("."):
                continue
            self.db.conn.execute(self._UPSERT, (self._check(p),))
        dirs = self._select_dirs(where="path = ?", params=(_path,))
        assert len(dirs) == 1, f"add lost its own row: {_path}"
        return dirs[0]

    def get(self, path: PPP) -> Dir | None:
        """Return the live Dir at path, or None."""
        _path = DirRepo._check(path)
        dirs = self._select_dirs(where="path = ?", params=(_path,))
        return dirs[0] if dirs else None

    def descendants(self, path: PPP) -> list[Dir]:
        """Return live dirs strictly under path, ordered by path."""
        _path = self._check(path)
        where, params = "path >= ? AND path < ?", (f"{_path}/", f"{_path}0")
        if _path == ".":
            where, params = "id != 0", ()
        return self._select_dirs(where, params)

    def mark_gone(self, path: PPP, started: int) -> None:
        """Set gone to started on path and every dir under it."""
        _path = self._check(path)
        where, params = DirRepo._where_descendants_or_parent(_path)
        params = (started, *params)
        self.db.conn.execute(self._UPDATE_GONE.format(where), params)
