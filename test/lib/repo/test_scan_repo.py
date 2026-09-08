# test/lib/repo/test_scan_repo.py
"""Pin ScanRepo, the activity log of scan runs.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
import time

from scout.lib.repo.db_connector import DBConnector
from scout.lib.repo.scan_repo import ScanRepo


def mk_scans(db: DBConnector) -> ScanRepo:
    """Create the scan table on db and return a ScanRepo over it."""
    sql.connect(db.path).executescript(ScanRepo.SCHEMA)
    return ScanRepo(db)


class TestScanRepo:
    """ScanRepo appends one row per scan run, keyed on started."""

    def test_start_returns_nanoseconds_now(self, db: DBConnector) -> None:
        """start returns an int between time_ns before and after the call."""
        before = time.time_ns()
        started = mk_scans(db).start()
        after = time.time_ns()
        assert before <= started <= after

    def test_start_inserts_unfinished_row(self, db: DBConnector) -> None:
        """After start, one row exists with that started and null finished."""
        started = mk_scans(db).start()
        q = "SELECT started, finished FROM scan"
        result = sql.connect(db.path).execute(q).fetchall()
        assert result == [(started, None)]

    def test_finish_sets_finished_and_count(self, db: DBConnector) -> None:
        """finish fills finished and files_seen on the started row only."""
        a, b = (scans := mk_scans(db)).start(), scans.start()
        scans.finish(a, 7)
        q = "SELECT started, finished IS NOT NULL, files_seen FROM scan ORDER BY started"
        assert sql.connect(db.path).execute(q).fetchall() == [(a, 1, 7), (b, 0, None)]

    def test_last_finished_none_on_fresh(self, db: DBConnector) -> None:
        """A manifest with no scans has no last_finished."""
        assert mk_scans(db).last_finished() is None

    def test_last_finished_ignores_unfinished(self, db: DBConnector) -> None:
        """A started but unfinished scan is not a sighting."""
        (scans := mk_scans(db)).start()
        assert scans.last_finished() is None

    def test_last_finished_is_newest(self, db: DBConnector) -> None:
        """With two finished scans, last_finished is the later started."""
        scans = mk_scans(db)
        a, b = scans.start(), scans.start()
        scans.finish(a, 1)
        scans.finish(b, 1)
        assert scans.last_finished() == b
