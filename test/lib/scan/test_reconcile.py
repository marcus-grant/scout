# test/lib/scan/test_reconcile.py
"""Tests for scan reconcile over plain values: coverage, missing rows, change.
Author: Marcus
Created: 2026-10-15
Revised: [2026-10-21]
License: AGPL-3.0-or-later
"""

from dataclasses import replace
from pathlib import PurePosixPath as PPP

import pytest
from b3c32 import code_from_chunks

import scout.lib.error as Err
from scout.lib import models
from scout.lib.fs.walk import WalkedDir
from scout.lib.scan import reconcile
from scout.lib.scan.hashing import HashingPolicy

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


class TestWalkedPathSets:
    """WalkedPathSets.see over constructed WalkedDir values."""

    def test_records_walked_path(self) -> None:
        """A dir with no errors adds its path to walked only."""
        walked = reconcile.WalkedPathSets()
        walked.add(WalkedDir(PPP("a"), (), (), ()))

        assert walked.walked == {PPP("a")}
        assert walked.unreadable == set()

    def test_records_excluded_paths(self) -> None:
        """A dir's excluded names land in excluded as paths under it."""
        walked = reconcile.WalkedPathSets()
        walked.add(WalkedDir(PPP("b"), (), (), (), excluded=("c",)))
        assert walked.excluded == {PPP("b/c")}

    def test_records_error_paths_as_unreadable(self) -> None:
        """Each error's path is added to unreadable."""
        err = Err.Unreadable("denied", path=PPP("a/x"), errno=13)
        walk_with_error = WalkedDir(PPP("a"), (), (), (err,))
        walked = reconcile.WalkedPathSets()

        walked.add(walk_with_error)
        assert walked.unreadable == {PPP("a/x")}

    def test_unwalked_path_is_absent(self) -> None:
        """A path in neither set and under no unreadable path is absent."""
        assert reconcile.WalkedPathSets().absent(PPP("a"))

    def test_walked_path_is_not_absent(self) -> None:
        """A walked path is not absent."""
        assert not reconcile.WalkedPathSets(walked={PPP("a")}).absent(PPP("a"))

    def test_unreadable_path_is_not_absent(self) -> None:
        """An unreadable path is not absent."""
        assert not reconcile.WalkedPathSets(unreadable={PPP("a")}).absent(PPP("a"))

    def test_path_under_unreadable_is_not_absent(self) -> None:
        """A path under an unreadable path is not absent."""
        assert not reconcile.WalkedPathSets(unreadable={PPP("a")}).absent(PPP("a/b"))

    def test_excluded_path_is_not_absent(self) -> None:
        """An excluded path is not confirmed absent as per util.is_excluded."""
        assert not reconcile.WalkedPathSets(excluded={PPP("x")}).absent(PPP("x"))


class TestMissingFiles:
    """missing_files, no fixture and no disk."""

    def test_unlisted_record_is_missing(self) -> None:
        """A record not listed in the WalkedDir is missing; a listed one is not."""
        walked = WalkedDir(PPP("b"), (), (models.FileStat("a.txt", 5, 9),), ())
        records = [
            models.FileRecord(1, models.FileStat("a.txt", 5, 9)),
            models.FileRecord(1, models.FileStat("old.txt", 5, 9)),
        ]

        assert reconcile.missing_files(walked, records) == ("old.txt",)

    def test_failed_entry_is_not_missing(self) -> None:
        """A record whose entry failed in the WalkedDir is not missing."""
        errs = (Err.Unreadable("Permission denied", path=PPP("b/a.txt"), errno=13),)
        walked = WalkedDir(PPP("b"), (), (), errs)
        records = [models.FileRecord(1, models.FileStat("a.txt", 5, 9))]
        assert reconcile.missing_files(walked, records) == ()

    def test_excluded_entry_is_not_missing(self) -> None:
        """A record whose entry was excluded in the WalkedDir is not missing."""
        walked = WalkedDir(PPP("b"), (), (), (), excluded=("a.txt",))
        records = [models.FileRecord(1, models.FileStat("a.txt", 5, 9))]
        assert reconcile.missing_files(walked, records) == ()


class TestMissingDirs:
    """missing_dirs, no fixture and no disk."""

    def test_unwalked_record_is_missing(self) -> None:
        """A record whose path was not walked is returned, in record order."""
        walked_sets = reconcile.WalkedPathSets(walked={PPP("a")})
        records = [models.DirRecord(1, PPP("a")), models.DirRecord(2, PPP("b"))]

        assert reconcile.missing_dirs(walked_sets, records) == (records[1].path,)


class TestChange:
    """change over every outcome, no fixture and no disk."""

    @pytest.mark.parametrize("hash_policy", [OFF, NEED, ALL])
    def test_no_record_is_added(self, hash_policy: HashingPolicy) -> None:
        """No record: ADDED, whatever the hashing hash_policy."""
        assert reconcile.change(_ST, None, hash_policy=hash_policy) == ADDED

    @pytest.mark.parametrize("hash_policy", [OFF, NEED, ALL])
    @pytest.mark.parametrize("stored", [{"size": 6}, {"mtime": 101}])
    def test_changed_stat_is_updated(
        self,
        stored: dict[str, int],
        hash_policy: HashingPolicy,
    ) -> None:
        """A record whose stat differs in size or mtime: UPDATED."""
        record = models.FileRecord(0, replace(_ST, **stored))
        assert reconcile.change(_ST, record, hash_policy=hash_policy) == UPDATED

    def test_rehash_of_a_match_is_updated(self) -> None:
        """A matching hashed record with rehash set: UPDATED."""
        assert reconcile.change(_ST, _HASHED, hash_policy=ALL) == UPDATED

    def test_unhashed_match_is_updated_when_hashing(self) -> None:
        """A matching record with no hash, hashing on: UPDATED."""
        assert reconcile.change(_ST, _UNHASHED, hash_policy=NEED) == UPDATED

    def test_unhashed_match_is_matched_without_hashing(self) -> None:
        """A matching record with no hash, hashing off: MATCHED."""
        assert reconcile.change(_ST, _UNHASHED, hash_policy=OFF) == MATCHED

    def test_hashed_match_is_matched(self) -> None:
        """A matching hashed record, hashing on, no rehash: MATCHED."""
        assert reconcile.change(_ST, _HASHED, hash_policy=NEED) == MATCHED
