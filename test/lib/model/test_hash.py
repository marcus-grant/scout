# test/lib/model/test_hash.py
"""Pin the Hash value type in scout.lib.model.hash.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import pytest
from b3c32 import CROCKFORD32_ALPHABET, hash_b32, verify_conformance
from b3c32.core import _CERTIFIED_BITS

import scout.lib.error as Err
from scout.lib.model.hash import Hash

BITS = min(_CERTIFIED_BITS)
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
