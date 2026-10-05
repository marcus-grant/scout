# test/lib/scan/test_file_stats.py
"""Tests for reconcile_file_stat: every outcome over plain values.
Author: Marcus
Created: 2026-10-02
License: AGPL-3.0-or-later
"""

from dataclasses import replace

import pytest
from b3c32 import code_from_chunks

from scout.lib.models import DEFAULT_BITS, FileRecord, FileStat, Hash, RecordChange
from scout.lib.scan.file_stats import reconcile_file_stat
from scout.lib.scan.hashing import HashingPolicy

# Shorter aliases for RecordChange members
ADDED = RecordChange.ADDED
UPDATED = RecordChange.UPDATED
MATCHED = RecordChange.MATCHED

# Same for HashingPolicy enum members
OFF = HashingPolicy.OFF
NEEDED = HashingPolicy.NEEDED
ALL = HashingPolicy.ALL

# Plain FileStat object that gets reused for all tests that don't need variation
_ST = FileStat("a.txt", 5, 100)
# Hashed FileRecord for testing policy decisions based on hashed records
_HASHED = FileRecord(0, _ST, Hash(code_from_chunks([b"a"], DEFAULT_BITS)), 1)
# Same but for unhashed
_UNHASHED = FileRecord(0, _ST)


class TestReconcileFileStat:
    """reconcile_file_stat over every outcome, no fixture and no disk."""

    @pytest.mark.parametrize("policy", [OFF, NEEDED, ALL])
    def test_no_record_is_added(self, policy: HashingPolicy) -> None:
        """No record: ADDED, whatever the hashing policy."""
        assert reconcile_file_stat(_ST, None, policy=policy) == ADDED

    @pytest.mark.parametrize("policy", [OFF, NEEDED, ALL])
    @pytest.mark.parametrize("stored", [{"size": 6}, {"mtime": 101}])
    def test_changed_stat_is_updated(
        self,
        stored: dict[str, int],
        policy: HashingPolicy,
    ) -> None:
        """A record whose stat differs in size or mtime: UPDATED."""
        record = FileRecord(0, replace(_ST, **stored))
        assert reconcile_file_stat(_ST, record, policy=policy) == UPDATED

    def test_rehash_of_a_match_is_updated(self) -> None:
        """A matching hashed record with rehash set: UPDATED."""
        assert reconcile_file_stat(_ST, _HASHED, policy=ALL) == UPDATED

    def test_unhashed_match_is_updated_when_hashing(self) -> None:
        """A matching record with no hash, hashing on: UPDATED."""
        assert reconcile_file_stat(_ST, _UNHASHED, policy=NEEDED) == UPDATED

    def test_unhashed_match_is_matched_without_hashing(self) -> None:
        """A matching record with no hash, hashing off: MATCHED."""
        assert reconcile_file_stat(_ST, _UNHASHED, policy=OFF) == MATCHED

    def test_hashed_match_is_matched(self) -> None:
        """A matching hashed record, hashing on, no rehash: MATCHED."""
        assert reconcile_file_stat(_ST, _HASHED, policy=NEEDED) == MATCHED
