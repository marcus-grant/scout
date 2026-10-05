# src/scout/lib/scan/hashing.py
"""How a scan hashes files: the policy, and the step applying it.
Author: Marcus
Created: 2026-10-02
License: AGPL-3.0-or-later
"""

from collections.abc import Callable
from dataclasses import replace
from enum import Enum
from pathlib import Path

import scout.lib.error as Err
from scout.lib.models import FileRecord, Hash


class HashingPolicy(Enum):
    """Which files a scan hashes.
    OFF: none; changed and new records are written with no hash.
    NEEDED: new and changed files, and matches with no stored hash.
    ALL: every file, rehashing matches that already have a hash."""

    OFF = "off"
    NEEDED = "need"
    ALL = "all"


def hash_record(
    record: FileRecord,
    path: Path,
    policy: HashingPolicy,
    started: int,
    hash_path: Callable[[Path], Hash | Err.Unreadable],
) -> FileRecord | Err.Unreadable:
    """Return record ready to upsert under policy.
    OFF: record unchanged, with no hash.
    Otherwise: record with hash_path(path) and hashed = started,
    or the Err.Unreadable hash_path answered."""
    if policy is HashingPolicy.OFF:
        return record
    h = hash_path(path)
    if isinstance(h, Err.Unreadable):
        return h
    return replace(record, hash=h, hashed=started)
