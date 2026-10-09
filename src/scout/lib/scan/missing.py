# src/scout/lib/scan/missing.py
"""Decide which records are missing from what a FS walk saw.
Author: Marcus
Created: 2026-10-02
Revised: [2026-10-05]
License: AGPL-3.0-or-later
"""

from collections.abc import Iterable
from pathlib import PurePosixPath as PPP

from scout.lib.fs.walk import WalkedDir, WalkedPathSets
from scout.lib.models import DirRecord, FileRecord


def reconcile_missing_files(
    walked: WalkedDir, records: Iterable[FileRecord]
) -> tuple[str, ...]:
    """Return the names of records missing from walked, in record order.
    A name is present when it is among walked.files or walked.errors;
    a failed entry was listed, so it exists and only its stat is unknown.
    Pure: takes values, writes nothing."""
    present = {st.name for st in walked.files} | {e.path.name for e in walked.errors}
    return tuple(r.stat.name for r in records if r.stat.name not in present)


def reconcile_missing_dirs(
    walked_sets: WalkedPathSets, records: Iterable[DirRecord]
) -> tuple[PPP, ...]:
    """Return the paths of records the walk confirms absent, in record order."""
    return tuple(r.path for r in records if walked_sets.confirms_absent(r.path))
