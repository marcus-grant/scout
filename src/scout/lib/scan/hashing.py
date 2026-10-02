# src/scout/lib/scan/hashing.py
"""How a scan hashes files: the policy, and the step applying it.
Author: Marcus
Created: 2026-10-02
License: AGPL-3.0-or-later
"""

from enum import Enum


class HashingPolicy(Enum):
    """Which files a scan hashes.
    OFF: none; changed and new records are written with no hash.
    NEEDED: new and changed files, and matches with no stored hash.
    ALL: every file, rehashing matches that already have a hash."""

    OFF = "off"
    NEEDED = "need"
    ALL = "all"
