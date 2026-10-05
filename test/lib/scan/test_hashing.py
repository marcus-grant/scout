# test/lib/scan/test_hashing.py
"""Tests for hash_record: every hashing outcome over plain values.
Author: Marcus
Created: 2026-10-05
License: AGPL-3.0-or-later
"""

import errno
from pathlib import Path
from pathlib import PurePosixPath as PPP

from b3c32 import code_from_chunks

import scout.lib.error as Err
from scout.lib.models import DEFAULT_BITS, FileRecord, FileStat, Hash
from scout.lib.scan.hashing import HashingPolicy, hash_record

# Shorter aliases for HashingPolicy enum members
OFF = HashingPolicy.OFF
NEEDED = HashingPolicy.NEEDED

# Plain record and path reused by every test; no disk backs them
_RECORD = FileRecord(1, FileStat("a.txt", 5, 100))
_PATH = Path("/x/a.txt")


class TestHashRecord:
    """hash_record under each policy, no disk."""

    def test_off_returns_record_without_hashing(self) -> None:
        """Policy OFF: the record comes back unchanged and nothing is hashed."""

        def never(_: Path) -> Hash | Err.Unreadable:
            raise AssertionError("hash_path called under OFF")

        result = hash_record(_RECORD, _PATH, OFF, 7, never)

        assert result == _RECORD

    def test_hashes_record_under_needed(self) -> None:
        """Policy NEEDED: the record comes back with the hash and hashed = started."""
        expected = Hash(code_from_chunks([b"alpha"], DEFAULT_BITS))

        result = hash_record(_RECORD, _PATH, NEEDED, 7, lambda _: expected)

        assert isinstance(result, FileRecord)
        assert (result.hash, result.hashed) == (expected, 7)

    def test_unreadable_is_returned(self) -> None:
        """An Err.Unreadable from hash_path comes back as is."""
        unreadable = Err.Unreadable("Permission denied", path=PPP("a.txt"), errno=13)

        result = hash_record(_RECORD, _PATH, NEEDED, 7, lambda _: unreadable)

        assert result is unreadable
        assert unreadable.errno == errno.EACCES
