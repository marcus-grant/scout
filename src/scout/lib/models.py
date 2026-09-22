# src/scout/lib/models.py
"""Scout's vocabulary: what the fs shows, what the manifest records, and
the values both speak in.
Author: Marcus
Created: 2026-09-23
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass
from pathlib import PurePosixPath as PPP

from b3c32 import CERTIFIED_BITS, CROCKFORD32_ALPHABET

import scout.lib.error as Err

DEFAULT_BITS = 120


@dataclass(frozen=True)
class Hash:
    """A b3c32 code as stored in file.hash; width is derived from its length."""

    code: str

    def __post_init__(self) -> None:
        """Raise Err.BadHash unless every symbol is Crockford and the width is certified."""
        if any(c not in CROCKFORD32_ALPHABET for c in self.code):
            msg = f"not a Crockford Base32 code: {self.code}"
            raise Err.BadHash(msg, self.code)
        if self.bits not in CERTIFIED_BITS:
            msg = f"uncertified hash width {self.bits} bits: {self.code}"
            raise Err.BadHash(msg, self.code)

    @property
    def bits(self) -> int:
        """Digest width in bits, five per symbol."""
        return len(self.code) * 5

    def __str__(self) -> str:
        """The code itself."""
        return self.code


@dataclass(frozen=True)
class DirRecord:
    """A directory the manifest knows; path is relative to root."""

    id: int
    path: PPP
    gone: int | None = None


@dataclass(frozen=True)
class FileRecord:
    """One file row; name is the entry under dir_id, never a path."""

    dir_id: int
    name: str
    size: int
    mtime: int
    hash: Hash | None = None
    hashed: int | None = None
    gone: int | None = None

    def __post_init__(self) -> None:
        """Raise Err.UnpairedHash unless hash and hashed are both set or both None."""
        if (self.hash is None) != (self.hashed is None):
            code = None if self.hash is None else self.hash.code
            raise Err.UnpairedHash("hash and hashed must be set together", code=code)


@dataclass(frozen=True)
class FileStat:
    """What stat says about one regular file, but only fields our table records:
    its name, size, and mtime ns."""

    name: str
    size: int
    mtime: int
