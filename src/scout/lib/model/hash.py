# src/scout/lib/model/hash.py
"""Hash value type: one certified-width Crockford Base32 b3c32 code.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass

from b3c32 import CROCKFORD32_ALPHABET
from b3c32.core import _CERTIFIED_BITS

import scout.lib.error as Err


@dataclass(frozen=True)
class Hash:
    """A b3c32 code as stored in file.hash; width is derived from its length."""

    code: str

    def __post_init__(self) -> None:
        """Raise Err.BadHash unless every symbol is Crockford and the width is certified."""
        if any(c not in CROCKFORD32_ALPHABET for c in self.code):
            msg = f"not a Crockford Base32 code: {self.code}"
            raise Err.BadHash(msg, self.code)
        if self.bits not in _CERTIFIED_BITS:
            msg = f"uncertified hash width {self.bits} bits: {self.code}"
            raise Err.BadHash(msg, self.code)

    @property
    def bits(self) -> int:
        """Digest width in bits, five per symbol."""
        return len(self.code) * 5

    def __str__(self) -> str:
        """The code itself."""
        return self.code
