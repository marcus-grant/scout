# test/lib/repo/test_scan_repo.py
"""Pin ScanRepo, the activity log of scan runs.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
import time

from scout.lib.manifest import Manifest


class TestScanRepo:
    """ScanRepo appends one row per scan run, keyed on started."""

    def test_start_returns_nanoseconds_now(self, manifest: Manifest) -> None:
        """start returns an int between time_ns before and after the call."""
        before = time.time_ns()
        started = manifest.scans.start()
        after = time.time_ns()
        assert before <= started <= after

    def test_start_inserts_unfinished_row(self, manifest: Manifest) -> None:
        """After start, one row exists with that started and null finished."""
        started = manifest.scans.start()
        q = "SELECT started, finished FROM scan"
        result = sql.connect(manifest.db.path).execute(q).fetchall()
        assert result == [(started, None)]

    def test_finish_sets_finished_and_count(self, manifest: Manifest) -> None:
        """finish fills finished and files_seen on the started row only."""
        a, b = (scans := manifest.scans).start(), scans.start()
        scans.finish(a, 7)
        q = "SELECT started, finished IS NOT NULL, files_seen FROM scan ORDER BY started"
        expect = [(a, 1, 7), (b, 0, None)]
        assert sql.connect(manifest.db.path).execute(q).fetchall() == expect

    def test_last_finished_none_on_fresh(self, manifest: Manifest) -> None:
        """A manifest with no scans has no last_finished."""
        assert manifest.scans.last_finished() is None

    def test_last_finished_ignores_unfinished(self, manifest: Manifest) -> None:
        """A started but unfinished scan is not a sighting."""
        (scans := manifest.scans).start()
        assert scans.last_finished() is None

    def test_last_finished_is_newest(self, manifest: Manifest) -> None:
        """With two finished scans, last_finished is the later started."""
        scans = manifest.scans
        a, b = scans.start(), scans.start()
        scans.finish(a, 1)
        scans.finish(b, 1)
        assert scans.last_finished() == b


class TestFinishReturn:
    """finish returns the finished timestamp it wrote."""

    def test_finish_returns_the_written_timestamp(self, manifest: Manifest) -> None:
        """finish's return equals the finished column on that row."""
        started = manifest.scans.start()

        finished = manifest.scans.finish(started, 3)

        with sql.connect(manifest.db.path) as conn:
            q = "SELECT finished, files_seen FROM scan WHERE started = ?;"
            assert conn.execute(q, (started,)).fetchone() == (finished, 3)
