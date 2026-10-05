# src/scout/lib/scan/context.py
"""What one scan run holds fixed, passed down as one value.
Author: Marcus
Created: 2026-10-05
License: AGPL-3.0-or-later
"""

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import scout.lib.error as Err
from scout.lib.manifest import Manifest
from scout.lib.models import Hash
from scout.lib.scan.hashing import HashingPolicy


@dataclass(frozen=True)
class ScanContext:
    """The manifest, root, start, hashing policy and hash call of one run."""

    manifest: Manifest
    root: Path
    started: int
    policy: HashingPolicy
    hash_path: Callable[[Path], Hash | Err.Unreadable]
