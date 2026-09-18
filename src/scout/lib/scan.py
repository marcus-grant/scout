# src/scout/lib/scan.py
"""The scan verb: walk a manifest's root and bring its rows up to date.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

from enum import Enum

from scout.lib.fs.walk import FileStat
from scout.lib.model.file import File


class Outcome(Enum):
    """What the scan did to a file's row; each member claims only what stat
    can prove."""

    ADDED = "added"
    UPDATED = "updated"
    MATCHED = "matched"


def decide(row: File | None, stat: FileStat, *, force: bool = False) -> Outcome:
    """ADDED when row is None; UPDATED when force is set or row.size or
    row.mtime differ from stat; MATCHED when size and mtime both agree."""
    if row is None:
        return Outcome.ADDED
    if (row.size != stat.size) or (row.mtime != stat.mtime):
        return Outcome.UPDATED
    if force:
        return Outcome.UPDATED
    return Outcome.MATCHED
