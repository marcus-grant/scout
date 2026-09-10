# src/scout/lib/repo/scan_repo.py
"""ScanRepo: the activity log, one row per scan run.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import time

from scout.lib.repo.db_connector import DBConnector


class ScanRepo:
    """Owns the scan table; started is the key and every timestamp is int ns."""

    SCHEMA = """CREATE TABLE IF NOT EXISTS scan (
        started INTEGER PRIMARY KEY,
        finished INTEGER,
        files_seen INTEGER
    ) WITHOUT ROWID;"""

    _SELECT = "SELECT max(started) FROM scan WHERE finished IS NOT NULL;"

    _INSERT = "INSERT INTO scan (started) VALUES (?);"

    _UPDATE_FINISHED = "UPDATE scan SET finished = ?, files_seen = ? WHERE started = ?;"

    def __init__(self, db: DBConnector) -> None:
        """Bind to the manifest db shares."""
        self.db = db

    def start(self) -> int:
        """Insert a row started now and return its started value."""
        started = time.time_ns()
        self.db.conn.execute(self._INSERT, (started,))
        return started

    def finish(self, started: int, files_seen: int) -> None:
        """Set finished to now and files_seen on the row keyed by started."""
        params = (time.time_ns(), files_seen, started)
        self.db.conn.execute(self._UPDATE_FINISHED, params)

    def last_finished(self) -> int | None:
        """Return the newest started whose finished is set, or None."""
        return self.db.conn.execute(self._SELECT).fetchone()[0]
