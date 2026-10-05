# test/lib/scan/test_missing.py
"""Tests for reconcile_missing_files over plain values.
Author: Marcus
Created: 2026-10-02
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.fs.walk import WalkedDir
from scout.lib.models import FileRecord, FileStat
from scout.lib.scan.missing import reconcile_missing_files


class TestReconcileMissingFiles:
    """reconcile_missing_files, no fixture and no disk."""

    def test_unlisted_record_is_missing(self) -> None:
        """A record not listed in the WalkedDir is missing; a listed one is not."""
        walked = WalkedDir(PPP("b"), (), (FileStat("a.txt", 5, 100),), ())
        records = [
            FileRecord(1, FileStat("a.txt", 5, 100)),
            FileRecord(1, FileStat("old.txt", 5, 100)),
        ]

        assert reconcile_missing_files(walked, records) == ("old.txt",)

    def test_failed_entry_is_not_missing(self) -> None:
        """A record whose entry failed in the WalkedDir is not missing."""
        errs = (Err.Unreadable("Permission denied", path=PPP("b/a.txt"), errno=13),)
        walked = WalkedDir(PPP("b"), (), (), errs)
        records = [FileRecord(1, FileStat("a.txt", 5, 100))]

        assert reconcile_missing_files(walked, records) == ()
