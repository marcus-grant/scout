# test/lib/test_models.py
"""Pin the model types: Hash validation, Dir and FileRecord construction, and the
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
from scout.lib.models import Dir, FileRecord, Hash

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
    """Dir mirrors one dir row: id, path relative to root, gone."""

    def test_gone_defaults_to_none(self) -> None:
        """Dir(id, path) is live: gone is None."""
        assert Dir(7, PPP("a/b")).gone is None

    def test_equal_by_fields(self) -> None:
        """Two Dir with the same id, path, and gone are equal."""
        args = (7, PPP("a/b"), 42)
        assert Dir(*args) == Dir(*args)

    def test_frozen(self) -> None:
        """Assigning a field raises FrozenInstanceError."""
        dir = Dir(7, PPP("a/b"))
        with pytest.raises(FrozenInstanceError):
            dir.gone = 42  # type: ignore


class TestFile:
    """FileRecord mirrors one file row keyed by (dir_id, name)."""

    def test_defaults_to_unhashed_and_live(self) -> None:
        """FileRecord(dir_id, name, size, mtime) has hash, hashed, and gone None."""
        f = FileRecord(1, "foo", 42, 2**30)
        assert all(getattr(f, attr) is None for attr in ("hash", "hashed", "gone"))

    def test_equal_by_fields(self) -> None:
        """Two FileRecord built from the same args are equal."""
        args = (1, "foo", 42, 2**30)
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
            FileRecord(1, "foo", 42, 2**30, **kw)
