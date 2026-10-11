# src/scout/lib/scan/context.py
"""What one scan run holds fixed, passed down as one value.
Author: Marcus
Created: 2026-10-05
License: AGPL-3.0-or-later
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.manifest import Manifest
from scout.lib.models import Hash
from scout.lib.scan.hashing import HashingPolicy


@dataclass(frozen=True)
class ScanContext:
    """The manifest, start, hashing policy and hash call of one run."""

    manifest: Manifest
    started: int
    policy: HashingPolicy
    hash_path: Callable[[PPP], Hash | Err.Unreadable]
