# src/scout/lib/fs/walk.py
"""Walk a tree in sorted DFS order, yielding one WalkedDir per directory.
Author: Marcus
Created: 2026-09-17
License: AGPL-3.0-or-later
"""

import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.models import FileStat


@dataclass(frozen=True)
class WalkedDir:
    """One directory as walked: its root-relative path, its files in name
    order, and the errors met while reading it."""

    path: PPP
    files: tuple[FileStat, ...]
    errors: tuple[Err.Unreadable, ...]


def _read_dir(
    root: Path,
    rel: PPP,
    exclude: frozenset[PPP] = frozenset(),
) -> tuple[WalkedDir, list[PPP]]:
    """One step of a file tree walk at given root and relative-to-root path.
    Use that path with os.scandir to list os.DirEntry in that directory.
    Add
    """
    child_dirs: list[PPP] = []
    child_files: list[FileStat] = []
    try:
        with os.scandir(root / rel) as it:
            entries = sorted(it, key=lambda e: e.name)
    except OSError as e:
        msg = e.strerror or "unreadable"
        errs = (Err.Unreadable(msg, path=rel, errno=e.errno),)
        return (WalkedDir(rel, (), errs), child_dirs)
    for ent in entries:
        if ent.is_dir(follow_symlinks=False):
            child_dirs.append(rel / ent.name)
        elif ent.is_file(follow_symlinks=False) and (rel / ent.name) not in exclude:
            stat = ent.stat()
            child_files.append(FileStat(ent.name, stat.st_size, stat.st_mtime_ns))
    return (WalkedDir(rel, tuple(child_files), ()), child_dirs)


def walk(root: Path, exclude: frozenset[PPP] = frozenset()) -> Iterator[WalkedDir]:
    """Yields WalkedDir iterator for root & every directory under, in DFS order.
    Skips symlinks, files & directories whose root-relative path is in exclude.
    Unreadable directories yield a listing with no files and one error.
    That unreadable path is not descended into."""
    walk_stack: list[PPP] = [PPP(".")]
    while walk_stack:
        walking_rel = walk_stack.pop()
        listing, child_dirs = _read_dir(root, walking_rel, exclude)
        yield listing
        walk_stack.extend(reversed(child_dirs))
