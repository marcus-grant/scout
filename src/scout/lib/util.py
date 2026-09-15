# src/scout/lib/util.py
"""Pure helpers shared across scout.lib; no I/O, no state.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err


def _make_rel_err(path: Path, root: Path) -> Err.NotUnderRoot:
    """Make the Err.NotUnderRoot error given the path and root used."""
    msg = f"path not under root {root.as_posix()}: {path.as_posix()}"
    return Err.NotUnderRoot(msg, PPP(path.as_posix()))


def to_rel(path: Path | str, root: Path) -> PPP:
    """Map path to a root-relative PPP; raise Err.NotUnderRoot on escape."""
    _path = Path(path) if isinstance(path, str) else path
    try:
        rel_str = _path.relative_to(root).as_posix()
    except ValueError as val_err:
        raise _make_rel_err(_path, root) from val_err
    if ".." in _path.parts:
        raise _make_rel_err(_path, root)
    return PPP(rel_str)
