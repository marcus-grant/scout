# src/scout/lib/scan/missing.py
"""Decide which recorded files are missing from a walked directory.
Author: Marcus
Created: 2026-10-02
License: AGPL-3.0-or-later
"""

from collections.abc import Iterable

from scout.lib.fs.walk import WalkedDir
from scout.lib.models import FileRecord


def reconcile_missing_files(
    walked: WalkedDir, records: Iterable[FileRecord]
) -> tuple[str, ...]:
    """Return the names of records missing from walked, in record order.
    A name is present when it is among walked.files or walked.errors;
    a failed entry was listed, so it exists and only its stat is unknown.
    Pure: takes values, writes nothing."""
    readable = {st.name for st in walked.files}
    unreadable = {e.path.name for e in walked.errors}
    present = readable | unreadable
    return tuple(r.stat.name for r in records if r.stat.name not in present)
