# test/lib/scan/test_reconcile.py
"""Tests for reconcile_missing_files over plain values.
Author: Marcus
Created: 2026-10-15
License: AGPL-3.0-or-later
"""

from dataclasses import replace
from pathlib import PurePosixPath as PPP

import pytest
from b3c32 import code_from_chunks

import scout.lib.error as Err
from scout.lib import models
from scout.lib.fs.walk import WalkedDir, WalkedPathSets
from scout.lib.scan.hashing import HashingPolicy
from scout.lib.scan.reconcile import (
    reconcile_file_stat,
    reconcile_missing_dirs,
    reconcile_missing_files,
)

# Shortened aliases for HashingPolicy & RecordChange
OFF = HashingPolicy.OFF
NEED = HashingPolicy.NEEDED
ALL = HashingPolicy.ALL
ADDED = models.RecordChange.ADDED
UPDATED = models.RecordChange.UPDATED
MATCHED = models.RecordChange.MATCHED

# Plain FileStat object that gets reused for all tests that don't need variation
_ST = models.FileStat("a.txt", 5, 100)
# Hashed FileRecord for testing hash policy decisions based on hashed records
_HASH = models.Hash(code_from_chunks([b"a"], models.DEFAULT_BITS))
_HASHED = models.FileRecord(0, _ST, _HASH, 1)
# Same but for unhashed
_UNHASHED = models.FileRecord(0, _ST)


class TestReconcileMissingFiles:
    """reconcile_missing_files, no fixture and no disk."""

    def test_unlisted_record_is_missing(self) -> None:
        """A record not listed in the WalkedDir is missing; a listed one is not."""
        walked = WalkedDir(PPP("b"), (), (models.FileStat("a.txt", 5, 100),), ())
        records = [
            models.FileRecord(1, models.FileStat("a.txt", 5, 100)),
            models.FileRecord(1, models.FileStat("old.txt", 5, 100)),
        ]

        assert reconcile_missing_files(walked, records) == ("old.txt",)

    def test_failed_entry_is_not_missing(self) -> None:
        """A record whose entry failed in the WalkedDir is not missing."""
        errs = (Err.Unreadable("Permission denied", path=PPP("b/a.txt"), errno=13),)
        walked = WalkedDir(PPP("b"), (), (), errs)
        records = [models.FileRecord(1, models.FileStat("a.txt", 5, 100))]

        assert reconcile_missing_files(walked, records) == ()


class TestReconcileMissingDirs:
    """reconcile_missing_dirs, no fixture and no disk."""

    def test_unwalked_record_is_missing(self) -> None:
        """A record whose path was not walked is returned, in record order."""
        walked_sets = WalkedPathSets(walked={PPP("a")})
        records = [models.DirRecord(1, PPP("a")), models.DirRecord(2, PPP("b"))]

        assert reconcile_missing_dirs(walked_sets, records) == (records[1].path,)


class TestReconcileFileStat:
    """reconcile_file_stat over every outcome, no fixture and no disk."""

    @pytest.mark.parametrize("hash_policy", [OFF, NEED, ALL])
    def test_no_record_is_added(self, hash_policy: HashingPolicy) -> None:
        """No record: ADDED, whatever the hashing hash_policy."""
        assert reconcile_file_stat(_ST, None, hash_policy=hash_policy) == ADDED

    @pytest.mark.parametrize("hash_policy", [OFF, NEED, ALL])
    @pytest.mark.parametrize("stored", [{"size": 6}, {"mtime": 101}])
    def test_changed_stat_is_updated(
        self,
        stored: dict[str, int],
        hash_policy: HashingPolicy,
    ) -> None:
        """A record whose stat differs in size or mtime: UPDATED."""
        record = models.FileRecord(0, replace(_ST, **stored))
        assert reconcile_file_stat(_ST, record, hash_policy=hash_policy) == UPDATED

    def test_rehash_of_a_match_is_updated(self) -> None:
        """A matching hashed record with rehash set: UPDATED."""
        assert reconcile_file_stat(_ST, _HASHED, hash_policy=ALL) == UPDATED

    def test_unhashed_match_is_updated_when_hashing(self) -> None:
        """A matching record with no hash, hashing on: UPDATED."""
        assert reconcile_file_stat(_ST, _UNHASHED, hash_policy=NEED) == UPDATED

    def test_unhashed_match_is_matched_without_hashing(self) -> None:
        """A matching record with no hash, hashing off: MATCHED."""
        assert reconcile_file_stat(_ST, _UNHASHED, hash_policy=OFF) == MATCHED

    def test_hashed_match_is_matched(self) -> None:
        """A matching hashed record, hashing on, no rehash: MATCHED."""
        assert reconcile_file_stat(_ST, _HASHED, hash_policy=NEED) == MATCHED
