# src/scout/lib/scan/file_stats.py
"""Decide what a file's record needs from its stat and the hashing policy.
Author: Marcus
Created: 2026-10-02
License: AGPL-3.0-or-later
"""

from scout.lib.models import FileRecord, FileStat, RecordChange


def reconcile_file_stat(
    stat: FileStat, record: FileRecord | None, *, hash: bool, rehash: bool
) -> RecordChange:
    """Return what record needs given stat and the hashing policy.
    ADDED when record is None;
    UPDATED when the stat differs, when rehash is set,
    or when hashing is on and the record has no hash;
    MATCHED otherwise. Pure: takes values, writes nothing."""
    change = RecordChange.classify(stat, record)
    # Only a match can be changed by the hashing policy
    if change != RecordChange.MATCHED:
        return change

    assert record is not None, "MATCHED state can only happen with a record existing"
    needs_hash = rehash or (hash and record.hash is None)
    return RecordChange.UPDATED if needs_hash else RecordChange.MATCHED
