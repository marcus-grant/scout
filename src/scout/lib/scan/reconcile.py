# src/scout/lib/scan/reconcile.py
"""Decide which records are missing from what a FS walk saw.
Author: Marcus
Created: 2026-10-15
License: AGPL-3.0-or-later
"""

from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import PurePosixPath as PPP

from scout.lib.fs.walk import WalkedDir
from scout.lib.models import DirRecord, FileRecord, FileStat, RecordChange
from scout.lib.scan.hashing import HashingPolicy
from scout.lib.util import is_excluded


@dataclass
class WalkedPathSets:
    """What one walk covered: every walked dir's path,
    every path that could not be listed or read, and every path excluded."""

    walked: set[PPP] = field(default_factory=set)
    unreadable: set[PPP] = field(default_factory=set)
    excluded: set[PPP] = field(default_factory=set)

    def add(self, walked: WalkedDir) -> None:
        """Add walked's path to walked, each error's path to unreadable,
        each excluded entry's path to excluded."""
        self.walked.add(walked.path)
        self.excluded.update(walked.path / name for name in walked.excluded)
        for err in walked.errors:
            self.unreadable.add(err.path)

    def absent(self, path: PPP) -> bool:
        """True when the walk proves path absent: not walked, not unreadable,
        and under no unreadable path."""
        if is_excluded(path, self.excluded):
            return False
        if path in self.walked or path in self.unreadable:
            return False
        return self.unreadable.isdisjoint(path.parents)


def reconcile_missing_files(
    walked: WalkedDir, records: Iterable[FileRecord]
) -> tuple[str, ...]:
    """Return the names of records missing from walked, in record order.
    A name is present when it is among walked.files or walked.errors;
    a failed entry was listed, so it exists and only its stat is unknown.
    Pure: takes values, writes nothing."""
    stated = {st.name for st in walked.files}
    failed = {e.path.name for e in walked.errors}
    present = stated | failed | set(walked.excluded)
    return tuple(r.stat.name for r in records if r.stat.name not in present)


def reconcile_missing_dirs(
    walked_sets: WalkedPathSets, records: Iterable[DirRecord]
) -> tuple[PPP, ...]:
    """Return the paths of records the walk confirms absent, in record order."""
    return tuple(r.path for r in records if walked_sets.absent(r.path))


def reconcile_file_stat(
    stat: FileStat,
    record: FileRecord | None,
    hash_policy: HashingPolicy,
) -> RecordChange:
    """What record needs given stat and hash_policy; pure, writes nothing.
    ADDED with no record; UPDATED when the stat differs or the policy
    wants a hash the record lacks; MATCHED otherwise."""
    # Shortened aliases for HashingPolicy members
    _NEED, _ALL = HashingPolicy.NEEDED, HashingPolicy.ALL  # Shortened aliases

    # Classify determines change type; only a match can be changed by the hash policy
    change = RecordChange.classify(stat, record)
    if change != RecordChange.MATCHED:
        return change

    # Logic for which RecordChange to return when the stat matches and we have a record
    assert record is not None, "MATCHED state can only happen with a record existing"
    needs_hash = hash_policy is _ALL or (hash_policy is _NEED and record.hash is None)
    return RecordChange.UPDATED if needs_hash else RecordChange.MATCHED
