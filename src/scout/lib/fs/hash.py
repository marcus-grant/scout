# src/scout/lib/fs/hash.py
"""Hash one file's bytes into a Hash, or report why it could not be read.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

from collections.abc import Callable
from pathlib import Path
from pathlib import PurePosixPath as PPP

from b3c32 import code_from_path

import scout.lib.error as Err
from scout.lib.model.hash import DEFAULT_BITS, Hash


def hash_file(
    path: Path,
    bits: int = DEFAULT_BITS,
    *,
    on_progress: Callable[[int], None] | None = None,
    interval_ms: int = 1000,
) -> Hash | Err.Unreadable:
    """Return the b3c32 Hash of the bytes at path at the given width.
    on_progress, when given, is passed to b3c32 and called with the bytes
    processed so far, at most once per interval_ms.
    An OSError while opening or reading returns Unreadable carrying
    path's name and the errno; nothing is raised."""
    try:
        return Hash(
            code_from_path(path, bits, on_progress=on_progress, interval_ms=interval_ms)
        )
    except OSError as e:
        msg = e.strerror or "unreadable"
        return Err.Unreadable(msg, errno=e.errno, path=PPP(path.name))
