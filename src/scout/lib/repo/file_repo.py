# src/scout/lib/repo/file_repo.py
"""FileRepo: the file table, keyed on (dir_id, name).
Author: Marcus
Created: 2026-09-09
License: AGPL-3.0-or-later
"""

from scout.lib.model.file import File
from scout.lib.model.hash import Hash
from scout.lib.repo.db_connector import DBConnector


class FileRepo:
    """Owns file(dir_id, name, hash, size, mtime, hashed, gone); hides gone rows."""

    SCHEMA = """CREATE TABLE IF NOT EXISTS file (
        dir_id INTEGER NOT NULL REFERENCES dir(id),
        name TEXT NOT NULL,
        hash TEXT,
        size INTEGER NOT NULL,
        mtime INTEGER NOT NULL,
        hashed INTEGER REFERENCES scan(started),
        gone INTEGER REFERENCES scan(started),
        PRIMARY KEY (dir_id, name)
    ) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS file_hash ON file(hash);"""

    _SELECT = """
    SELECT dir_id, name, hash, size, mtime, hashed, gone FROM file
    WHERE gone IS NULL AND ({}) ORDER BY dir_id, name;"""

    _UPSERT = """
    INSERT INTO file (dir_id, name, hash, size, mtime, hashed)
    VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(dir_id, name)
    DO UPDATE SET
        hash = excluded.hash, size = excluded.size, mtime = excluded.mtime,
        hashed = excluded.hashed, gone = NULL;"""

    _UPDATE_GONE = "UPDATE file set gone = ? WHERE gone IS NULL AND dir_id IN ({});"

    _UPDATE_GONE_ONE = """UPDATE file set gone = ?
                            WHERE (dir_id = ? AND name = ? AND gone IS NULL);"""

    def __init__(self, db: DBConnector) -> None:
        """Bind to the manifest db shares."""
        self.db = db

    @staticmethod
    def _row_to_file(r: tuple) -> File:
        """Build a File from one row in _SELECT column order."""
        h = r[2] if r[2] is None else Hash(r[2])
        return File(r[0], r[1], r[3], r[4], hash=h, hashed=r[5], gone=r[6])

    @staticmethod
    def _rows_to_files(rows: list[tuple]) -> list[File]:
        """Build a list of Files from rows in _SELECT column order."""
        return [FileRepo._row_to_file(r) for r in rows]

    @staticmethod
    def _file_to_params(f: File) -> tuple:
        """Six insert bindings for f: dir_id, name, hash code, size, mtime, hashed"""
        h = None if f.hash is None else f.hash.code
        return (f.dir_id, f.name, h, f.size, f.mtime, f.hashed)

    def _select_files(self, where: str, params: tuple = ()) -> list[File]:
        """Select live files WHERE where, bound from params, by dir_id and name."""
        rows = self.db.conn.execute(self._SELECT.format(where), params).fetchall()
        return FileRepo._rows_to_files(rows)

    def add(self, file: File) -> File:
        """Upsert file on (dir_id, name), writing content and gone = NULL."""
        self.db.conn.execute(self._UPSERT, self._file_to_params(file))
        where, params = "dir_id = ? AND name = ?", (file.dir_id, file.name)
        files = self._select_files(where, params)
        assert len(files) == 1, f"add lost its own row: {params}"
        return files[0]

    def get(self, dir_id: int, name: str) -> File | None:
        """Return the live File at (dir_id, name), or None."""
        where, params = "dir_id = ? AND name = ?", (dir_id, name)
        files = self._select_files(where, params)
        return None if len(files) <= 0 else files[0]

    def in_dir(self, dir_id: int) -> list[File]:
        """Return live files directly in dir_id, ordered by name."""
        return self._select_files("dir_id = ?", (dir_id,))

    def by_hash(self, hash: Hash) -> list[File]:
        """Return live files whose hash is hash, ordered by dir_id then name."""
        return self._select_files("hash = ?", (hash.code,))

    def mark_gone(self, dir_ids: list[int], started: int) -> None:
        """Set gone to started on every live file in the listed dirs."""
        dids = ", ".join("?" * len(dir_ids))
        self.db.conn.execute(self._UPDATE_GONE.format(dids), (started, *dir_ids))

    def mark_gone_one(self, dir_id: int, name: str, started: int) -> None:
        """Set gone to started on the live file at (dir_id, name), if any."""
        self.db.conn.execute(self._UPDATE_GONE_ONE, (started, dir_id, name))
