# src/scout/lib/models.py
"""Scout's vocabulary: what the fs shows, what the manifest records, and
the values both speak in.
Author: Marcus
Created: 2026-09-23
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass
from enum import Enum
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
class FileStat:
    """What stat says about one regular file, but only fields our table records:
    its name, size, and mtime ns."""

    name: str
    size: int
    mtime: int


@dataclass(frozen=True)
class DirRecord:
    """A directory the manifest knows; path is relative to root."""

    id: int
    path: PPP
    gone: int | None = None


@dataclass(frozen=True)
class FileRecord:
    """One file row: where it sits, what stat said when the manifest last
    saw it, and what the manifest knows beyond stat."""

    dir_id: int
    stat: FileStat
    hash: Hash | None = None
    hashed: int | None = None
    gone: int | None = None

    def __post_init__(self) -> None:
        """Raise Err.UnpairedHash unless hash and hashed are both set or both None."""
        if (self.hash is None) != (self.hashed is None):
            code = None if self.hash is None else self.hash.code
            raise Err.UnpairedHash("hash and hashed must be set together", code=code)


class RecordChange(Enum):
    """What a real life filesystem scan with stat discovers that manifest record needs.
    Each member claims only what evidence about the filesystem proves is true.

    MATCHED: stat agrees, likely nothing changed, but not certain
    VERIFIED: hash confirms file unchanged (or 1 in ~sqrt(2**120) chance of collision)
    ADDED: file added since manifest recording
    """

    ADDED = "added"
    UPDATED = "updated"
    MATCHED = "matched"

    @classmethod
    def classify(
        cls, fs_stat: FileStat, record: FileRecord | None = None
    ) -> "RecordChange":
        """ADDED when record is None;
        UPDATED when record.stat differs from stat in size or mtime;
        MATCHED when otherwise.
        Pure: hashing policy choices since hashing is slow on slow drives."""
        if record is None:
            return cls.ADDED
        if record and (fs_stat != record.stat):
            return cls.UPDATED
        return cls.MATCHED
