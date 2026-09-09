# src/scout/lib/model/dir.py
"""Dir model: one row of the dir table.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass
from pathlib import PurePosixPath as PPP


@dataclass(frozen=True)
class Dir:
    """A directory the manifest knows; path is relative to root."""

    id: int
    path: PPP
    gone: int | None = None
