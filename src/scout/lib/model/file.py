# src/scout/lib/model/file.py
"""File model: one row of the file table.
Author: Marcus
Created: 2026-09-09
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass

import scout.lib.error as Err
from scout.lib.model.hash import Hash


@dataclass(frozen=True)
class File:
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
