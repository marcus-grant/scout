# test/lib/test_models.py
"""Pin the model types: Hash validation, DirRecord and FileRecord construction, and the
hash and hashed pairing invariant.
Author: Marcus
Created: 2026-09-23
License: AGPL-3.0-or-later
"""

from dataclasses import FrozenInstanceError
from pathlib import PurePosixPath as PPP

import pytest
from b3c32 import CERTIFIED_BITS, CROCKFORD32_ALPHABET, hash_b32, verify_conformance

import scout.lib.error as Err
from scout.lib.models import DirRecord, FileRecord, Hash, RecordChange
from test.factory import mk_file_record, mk_stat

BITS = min(CERTIFIED_BITS)
CODE = hash_b32(b"scout", BITS)
BAD_SYMBOL = next(c for c in "ILOU" if c not in CROCKFORD32_ALPHABET)


class TestHash:
    """Hash wraps one certified-width Crockford Base32 code."""

    def test_b3c32_conformance(self) -> None:
        """The installed b3c32 still honors the contract Hash is built on."""
        verify_conformance()

    def test_str_is_code(self) -> None:
        """str(Hash(code)) returns the code unchanged."""
        assert str(Hash(CODE)) == hash_b32(b"scout", BITS)

    def test_bits_derived_from_length(self) -> None:
        """A 24-symbol code reports 120 bits; width is not stored."""
        assert Hash(CODE).bits == BITS

    def test_equal_by_code(self) -> None:
        """Two Hash objects with the same code are equal and hash alike."""
        assert Hash(CODE) == Hash(CODE)

    def test_rejects_bad_symbols(self) -> None:
        """A code outside the Crockford alphabet raises Err.BadHash."""
        bad = BAD_SYMBOL + CODE[1:]
        with pytest.raises(Err.BadHash) as exc:
            Hash(bad)
        assert exc.value.code == bad

    def test_rejects_uncertified_width(self) -> None:
        """A code whose width is not certified raises Err.BadHash."""
        bad = CODE[:-1]
        with pytest.raises(Err.BadHash) as exc:
            Hash(CODE[:-1])
        assert exc.value.code == bad


class TestDir:
    """DirRecord mirrors one dir row: id, path relative to root, gone."""

    def test_gone_defaults_to_none(self) -> None:
        """DirRecord(id, path) is live: gone is None."""
        assert DirRecord(7, PPP("a/b")).gone is None

    def test_equal_by_fields(self) -> None:
        """Two DirRecord with the same id, path, and gone are equal."""
        args = (7, PPP("a/b"), 42)
        assert DirRecord(*args) == DirRecord(*args)

    def test_frozen(self) -> None:
        """Assigning a field raises FrozenInstanceError."""
        dir = DirRecord(7, PPP("a/b"))
        with pytest.raises(FrozenInstanceError):
            dir.gone = 42  # type: ignore


class TestFileRecord:
    """FileRecord mirrors one file row keyed by (dir_id, name)."""

    def test_defaults_to_unhashed_and_live(self) -> None:
        """FileRecord(dir_id, factory.mk_stat(name, size, mtime)) has hash, hashed, and gone None."""
        rec, none_fields = FileRecord(1, mk_stat()), ("hash", "hashed", "gone")
        assert all(getattr(rec, fld) is None for fld in none_fields)

    def test_equal_by_fields(self) -> None:
        """Two FileRecord built from the same args are equal."""
        args = (1, mk_stat("foo", 42, 2**30))
        kw = {"hash": Hash("A" * 24), "hashed": (2**30) + 1}
        assert FileRecord(*args, **kw) == FileRecord(*args, **kw)

    def test_frozen(self) -> None:
        """Assigning a field raises FrozenInstanceError."""
        with pytest.raises(FrozenInstanceError):
            FileRecord(1, "foo", 42, 2**30).hash = Hash("A" * 24)  # type: ignore

    @pytest.mark.parametrize("kw", [{"hash": Hash("A" * 24)}, {"hashed": 7}])
    def test_rejects_hash_without_hashed(self, kw: dict) -> None:
        """hash and hashed must both be set or both be None."""
        with pytest.raises(Err.UnpairedHash):
            FileRecord(1, mk_stat(), **kw)


class TestRecordChange:
    """RecordChange.classify says what stat proves about a record."""

    RC = RecordChange  # Shortened alias

    def test_no_record_is_added(self) -> None:
        """classify(None, stat) is ADDED."""
        assert self.RC.classify(mk_stat()) is self.RC.ADDED

    @pytest.mark.parametrize("kw", [{"size": 2}, {"mtime": 7}, {"size": 2, "mtime": 7}])
    def test_size_or_mtime_change_is_updated(self, kw: dict) -> None:
        """recorded stat differs from FS stat in size, mtime, or both is UPDATED."""
        assert self.RC.classify(mk_stat(**kw), mk_file_record()) == self.RC.UPDATED

    def test_equal_stat_is_matched(self) -> None:
        """A record whose stat equals stat is MATCHED, whatever its hash.
        NOTE: default mk_stat & mk_file_record factories produce same FileStat."""
        assert self.RC.classify(mk_stat(), mk_file_record()) == self.RC.MATCHED
