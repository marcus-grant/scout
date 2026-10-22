# test/lib/scan/test_tally.py
"""Pin ScanTally: counting events and freezing a Summary.
Author: Marcus
Created: 2026-10-15
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP

import factory
import pytest

import scout.lib.error as Err
from scout.lib import event
from scout.lib.models import RecordChange
from scout.lib.scan.tally import ScanTally, Summary

mk_frec = factory.mk_file_record


class TestScanTally:
    """ScanTally over constructed events; no manifest, no disk."""

    @pytest.fixture
    def tally(self) -> ScanTally:
        return ScanTally()

    def test_file_scanned_counts_by_change(self, tally: ScanTally) -> None:
        """One FileScanned per change gives added, updated, matched of 1 each."""
        for change in RecordChange:
            tally.see(event.FileScanned(PPP("a.txt"), mk_frec(), change))

        assert tally.summary(1, 2) == Summary(1, 2, 1, 1, 1, 0, 0)

    def test_gone_and_errors_count(self, tally: ScanTally) -> None:
        """A RecordGone counts as gone; a ReadFailed counts as an error."""
        tally.see(event.RecordGone(PPP("b")))
        err = Err.Unreadable("denied", path=PPP("c"), errno=13)
        tally.see(event.ReadFailed(PPP("c"), err))
        assert tally.summary(1, 2) == Summary(1, 2, 0, 0, 0, 1, 1)

    def test_access_lost_counts_nothing(self, tally: ScanTally) -> None:
        """An AccessLost changes no count."""
        tally.see(event.AccessLost(PPP("b")))
        assert tally.summary(1, 2) == Summary(1, 2, 0, 0, 0, 0, 0)

    def test_files_seen_sums_every_change(self, tally: ScanTally) -> None:
        """files_seen is the sum of added, updated and matched."""
        for change in RecordChange:
            tally.see(event.FileScanned(PPP("a.txt"), mk_frec(), change))

        tally.see(event.RecordGone(PPP("b")))
        assert tally.files_seen() == 3
