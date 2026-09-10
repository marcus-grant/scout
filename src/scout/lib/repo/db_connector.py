# src/scout/lib/repo/db_connector.py
"""DBConnector: opens and validates one manifest file and owns its connection.
Author: Marcus
Created: 2026-09-10
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err


class DBConnector:
    """One autocommit sqlite3 connection to a validated manifest at path."""

    path: Path
    conn: sql.Connection

    def __init__(self, path: Path) -> None:
        """Open path; raise NoManifest if absent, NotAManifest if not a manifest."""
        self.path = path
        self.conn = self._validate(self.path)

    @staticmethod
    def _validate(path: Path) -> sql.Connection:
        """Return an autocommit connection to path or raise the matching error."""
        if not path.exists():
            msg = f"no manifest file at {path}"
            raise Err.NoManifest(msg, path=PPP(path.as_posix()))
        with path.open("rb") as f:
            if f.read(16) != b"SQLite format 3\x00":  # SQLite3 magic bytes check
                msg = f"not a SQLite file: {path}"
                raise Err.NotAManifest(msg, path=PPP(path.as_posix()))
        q = "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fs_meta'"
        if sql.connect(path, isolation_level=None).execute(q).fetchone() is None:
            msg = f"no fs_meta table: {path}"
            raise Err.NotAManifest(msg, path=PPP(path.as_posix()))
        return sql.connect(path, isolation_level=None)

    def begin(self) -> None:
        """Start a transaction; raise NestedTransaction if one is open."""
        if self.conn.in_transaction:
            msg = f"transaction already open on {self.path}"
            raise Err.NestedTransaction(msg, path=PPP(self.path.as_posix()))
        self.conn.execute("BEGIN;")

    def commit(self) -> None:
        """End the open transaction, keeping its writes."""
        self.conn.execute("COMMIT;")

    def rollback(self) -> None:
        """End the open transaction, discarding its writes."""
        self.conn.execute("ROLLBACK;")
