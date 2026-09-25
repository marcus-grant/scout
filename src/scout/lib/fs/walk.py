# src/scout/lib/fs/walk.py
"""Walk a tree in sorted DFS order, yielding one WalkedDir per directory.
Author: Marcus
Created: 2026-09-17
License: AGPL-3.0-or-later
"""

import errno
import os
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.models import FileStat


@dataclass(frozen=True)
class WalkedDir:
    """DTO for directory as walked:
    Its root-relative path, its subdirectories,
    its files in name order, and the errors met while reading it.
    unlistable is True when this directory itself could not be listed; its
    subdirs and files are then empty because nothing inside it could be seen,
    not because it holds nothing, and errors holds that one failure.
    """

    path: PPP
    subdirs: tuple[str, ...]
    files: tuple[FileStat, ...]
    errors: tuple[Err.Unreadable, ...]
    unlistable: bool = False


def _unreadable(e: OSError, path: PPP) -> Err.Unreadable:
    """The Err.Unreadable for path from the OSError that reading it raised:
    strerror as message, or "unreadable" when there is none, and its errno."""
    msg = e.strerror or "unreadable"
    return Err.Unreadable(msg, path=path, errno=e.errno)


def _read_dir(
    root: Path,
    rel: PPP,
    exclude: frozenset[PPP] = frozenset(),
) -> WalkedDir:
    """One step of the walk: list root / rel with os.scandir,
    entries in name order, & return what was seen as a WalkedDir:
    subdirectory names & file stats, skipping symlinks & files in exclude.
    If the directory cannot be listed, return it flagged unlistable,
    with its one Unreadable in errors."""
    try:  # First check that the directory can be scanned
        with os.scandir(root / rel) as it:
            entries = sorted(it, key=lambda e: e.name)
    except OSError as e:  # If not, mark unlistable & return sparse WalkedDir
        return WalkedDir(rel, (), (), (_unreadable(e, rel),), unlistable=True)

    # Prepare to loop over scandir 'entries' & accumulate info about it
    child_dir_names: list[str] = []
    child_files: list[FileStat] = []
    errors: tuple[Err.Unreadable, ...] = ()
    for ent in entries:
        ent_rel = rel / ent.name
        if ent_rel in exclude:
            continue  # If entry is excluded, skip entirely
        try:  # Individual entries may fail at stat; if so report & skip
            if ent.is_dir(follow_symlinks=False):
                child_dir_names.append(ent.name)
            elif ent.is_file(follow_symlinks=False):
                stat = ent.stat()
                child_files.append(FileStat(ent.name, stat.st_size, stat.st_mtime_ns))
        except OSError as e:
            # ENOENT: it vanished after the listing; left out, scan marks it gone later.
            if e.errno != errno.ENOENT:
                errors += (_unreadable(e, ent_rel),)
    return WalkedDir(rel, tuple(child_dir_names), tuple(child_files), errors)


def walk(root: Path, exclude: frozenset[PPP] = frozenset()) -> Iterator[WalkedDir]:
    """Yields WalkedDir iterator for root & every directory under, in DFS order.
    Skips symlinks, files & directories whose root-relative path is in exclude.
    An unlistable directory yields its WalkedDir flagged unlistable and is
    not descended into."""
    walk_stack: list[PPP] = [PPP(".")]
    while walk_stack:
        walking_rel = walk_stack.pop()
        walked = _read_dir(root, walking_rel, exclude)
        yield walked
        # Stack pops last-in 1st; reverse so siblings get visited in name order.
        subdir_names = reversed(walked.subdirs)
        walk_stack.extend(walked.path / name for name in subdir_names)
