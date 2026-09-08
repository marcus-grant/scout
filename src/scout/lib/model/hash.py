# src/scout/lib/model/hash.py
"""Hash value type: one certified-width Crockford Base32 b3c32 code.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import hashlib
import os
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
class HashMD5:
    """
    Represents a hash of a file in the MD5 algorithm.
    Provides member & class methods to create and compare hashes.
    """

    bin: bytes

    @classmethod
    def from_path(cls, path: str, chunk_size: int = 4096) -> "HashMD5":
        """
        Creates a HashMD5 object from a file path.
        It hashes the file using the MD5 algorithm.
        Factory to create object with both kinds of hash representations.
        """
        if not os.path.isfile(path):
            raise ValueError(
                f"Path {path} not a regular or linked file (inside HashMD5.from_path)"
            )
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return cls(h.digest())

    def __init__(self, bin: bytes | None = None, hex: str | None = None):
        """
        Initialize a HashMD5 object with either bytes or hex string.
        """
        if bin is not None:
            self.bin = bin
        elif hex is not None:
            self.bin = bytes.fromhex(hex)
        else:
            raise ValueError(
                "Either bin or hex must be provided to HashMD5 constructor."
            )

    def __str__(self) -> str:
        """
        Returns the hex representation of the hash.
        """
        return self.bin.hex()

    def __repr__(self) -> str:
        """
        Returns the hex representation of the hash.
        """
        return f"HashMD5(hex={self.__str__()})"

    @property
    def hex(self) -> str:
        """
        Returns the hex representation of the hash.
        """
        return self.bin.hex()
