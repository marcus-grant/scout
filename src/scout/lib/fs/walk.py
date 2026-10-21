# src/scout/lib/fs/walk.py
"""Walk a tree in sorted DFS order, yielding one WalkedDir per directory.
Author: Marcus
Created: 2026-09-17
License: AGPL-3.0-or-later
"""

import errno
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.models import FileStat
from scout.lib.util import is_excluded


@dataclass(frozen=True)
class WalkedDir:
    """DTO for directory as walked:
    Its root-relative path, its subdirectories,
    its files in name order, and the errors met while reading it.
    unlistable is True when this directory itself could not be listed; its
    subdirs and files are then empty because nothing inside it could be seen,
    not because it holds nothing, and errors holds that one failure.
    excluded names the entries skipped because exclude covers them.
    """

    path: PPP
    subdirs: tuple[str, ...]
    files: tuple[FileStat, ...]
    errors: tuple[Err.Unreadable, ...]
    unlistable: bool = False
    excluded: tuple[str, ...] = ()


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


def _unreadable(e: OSError, path: PPP) -> Err.Unreadable:
    """The Err.Unreadable for path from the OSError that reading it raised:
    strerror as message, or "unreadable" when there is none, and its errno."""
    msg = e.strerror or "unreadable"
    return Err.Unreadable(msg, path=path, errno=e.errno)


def _read_entry(ent: os.DirEntry, rel: PPP) -> str | FileStat | Err.Unreadable | None:
    """Read one entry: a subdir's name, a file's stat, its error,
    or None for a symlink or an entry that vanished after the listing."""
    try:
        if ent.is_dir(follow_symlinks=False):
            return ent.name
        if ent.is_file(follow_symlinks=False):
            return FileStat(ent.name, (st := ent.stat()).st_size, st.st_mtime_ns)
        return None
    except OSError as e:
        return _unreadable(e, rel / ent.name) if e.errno != errno.ENOENT else None


def _read_dir(
    root: Path,
    rel: PPP,
    exclude: frozenset[PPP] = frozenset(),
) -> WalkedDir:
    """One step of the walk: list root / rel in name order into a WalkedDir,
    naming excluded entries and reading the rest; an unlistable dir is flagged."""
    try:  # First check that the directory can be scanned
        with os.scandir(root / rel) as it:
            entries = sorted(it, key=lambda e: e.name)
    except OSError as e:  # If not, mark unlistable & return sparse WalkedDir
        return WalkedDir(rel, (), (), (_unreadable(e, rel),), unlistable=True)

    # Name the excluded entries, read the rest, and file each answer by kind
    excluded = tuple(e.name for e in entries if is_excluded(rel / e.name, exclude))
    skip_set = set(excluded)
    read = tuple(_read_entry(e, rel) for e in entries if e.name not in skip_set)
    return WalkedDir(
        rel,
        tuple(e for e in read if isinstance(e, str)),
        tuple(e for e in read if isinstance(e, FileStat)),
        tuple(e for e in read if isinstance(e, Err.Unreadable)),
        excluded=excluded,
    )


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
