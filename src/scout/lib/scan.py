# src/scout/lib/scan.py
"""The scan verb: walk a manifest's root and bring its rows up to date.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.fs.hash import hash_file
from scout.lib.fs.walk import FileStat, Listing, walk
from scout.lib.manifest import Manifest
from scout.lib.model.file import File
from scout.lib.model.hash import DEFAULT_BITS
from scout.lib.util import to_rel


class Outcome(Enum):
    """What the scan did to a file's row; each member claims only what stat
    can prove."""

    ADDED = "added"
    UPDATED = "updated"
    MATCHED = "matched"


def decide(
    row: File | None, stat: FileStat, *, force: bool = False, hash: bool = False
) -> Outcome:
    """ADDED when row is None; UPDATED when force is set or row.size or
    row.mtime differ from stat; MATCHED when size and mtime both agree."""
    if row is None:
        return Outcome.ADDED
    if (row.size != stat.size) or (row.mtime != stat.mtime):
        return Outcome.UPDATED
    if hash and row.hash is None:
        return Outcome.UPDATED
    if force:
        return Outcome.UPDATED
    return Outcome.MATCHED


@dataclass(frozen=True)
class Scanned:
    """One file as the scan left it: its root-relative path, its row, and
    what the scan did to that row."""

    path: PPP
    file: File
    outcome: Outcome


@dataclass(frozen=True)
class Gone:
    """One row marked gone this scan: its root-relative path."""

    path: PPP


@dataclass(frozen=True)
class Summary:
    """What one scan did in total: its window and the count per outcome,
    plus the errors met and the rows marked gone."""

    started: int
    finished: int
    added: int
    updated: int
    matched: int
    errors: int
    gone: int


def _mark_dir_gone(manifest: Manifest, path: PPP, started: int) -> Iterator[Gone]:
    """Mark the live dir at path, every live dir under it, and every live
    file in them gone with started; yield one Gone per file and per dir,
    files of a dir before the dir, deepest dirs last."""
    if (top := manifest.dirs.get(path)) is None:
        return
    subtree = [top, *manifest.dirs.descendants(path)]
    for d in subtree:
        for row in manifest.files.in_dir(d.id):
            yield Gone(d.path / row.name)
    manifest.files.mark_gone([d.id for d in subtree], started)
    for d in subtree:
        yield Gone(d.path)
    manifest.dirs.mark_gone(path, started)


def _scan_file(
    manifest: Manifest,
    dir_id: int,
    dir_rel: PPP,
    dir_abs: Path,
    stat: FileStat,
    started: int,
    *,
    hash: bool = True,
    force: bool = False,
    bits: int = DEFAULT_BITS,
    on_progress: Callable[[int], None] | None = None,
) -> Scanned | Err.Unreadable:
    """Bring one file's row up to date and say what was done.
    Fetch the live row at (dir_id, stat.name); decide against stat.
    MATCHED: write nothing and return the row as found.
    ADDED or UPDATED with hash: hash_file(dir_abs / stat.name), write the
    row with that hash and hashed = started.
    ADDED or UPDATED without hash: write the row with hash and hashed null.
    An Unreadable from hash_file is returned and nothing is written."""
    row = manifest.files.get(dir_id, stat.name)
    outcome = decide(row, stat, force=force)
    if outcome is Outcome.MATCHED:
        assert row is not None, "MATCHED implies a row"
        return Scanned(dir_rel / stat.name, row, outcome)

    h, hashed = None, None
    if hash:
        h = hash_file(dir_abs / stat.name, bits, on_progress=on_progress)
        if isinstance(h, Err.Unreadable):
            return h
        hashed = started
    file = manifest.files.add(File(dir_id, stat.name, stat.size, stat.mtime, h, hashed))

    return Scanned(dir_rel / stat.name, file, outcome)


def _scan_dir(
    manifest: Manifest,
    root: Path,
    listing: Listing,
    started: int,
    *,
    hash: bool = True,
    force: bool = False,
    bits: int = DEFAULT_BITS,
    on_progress: Callable[[int], None] | None = None,
) -> Iterator[Scanned | Gone | Err.Unreadable]:
    """Bring one walked directory's rows current, yielding as it goes.
    dirs.add(listing.path) first; then one _scan_file per FileStat in
    listing order; then every live file row in this dir whose name is not
    in the listing is marked gone with started and yielded as Gone; last,
    each Unreadable the listing carried."""
    d = manifest.dirs.add(listing.path)
    for st in listing.files:
        yield _scan_file(
            manifest,
            d.id,
            listing.path,
            root / listing.path,
            st,
            started,
            hash=hash,
            force=force,
            bits=bits,
            on_progress=on_progress,
        )
    seen = {st.name for st in listing.files}
    for row in manifest.files.in_dir(d.id):
        if row.name not in seen:
            manifest.files.mark_gone_one(d.id, row.name, started)
            yield Gone(listing.path / row.name)
    yield from listing.errors


class _Batch:
    """Commit the manifest every size rows so an interrupted scan keeps
    its progress; scan calls tick once per row written."""

    def __init__(self, manifest: Manifest, size: int) -> None:
        """Hold the manifest and the batch size; no transaction is open yet."""
        self.manifest = manifest
        self.size = size

    def open(self) -> None:
        """Begin a transaction on the manifest and zero the counter."""
        self.count = 0
        self.manifest.__enter__()

    def tick(self) -> None:
        """Count one row; at size, commit, reopen, and zero the counter."""
        self.count += 1
        if self.count >= self.size:
            self.close()
            self.open()

    def close(self) -> None:
        """Commit whatever is pending and leave no transaction open."""
        self.count = 0
        self.manifest.__exit__(None, None, None)


def scan(
    manifest: Manifest,
    *,
    hash: bool = True,
    force: bool = False,
    bits: int = DEFAULT_BITS,
    batch_size: int = 500,
    on_progress: Callable[[int], None] | None = None,
) -> Iterator[Scanned | Gone | Err.Unreadable | Summary]:
    """Walk manifest's root and bring every row current, yielding records
    as they happen and one Summary last.
    started comes from scans.start(); rows commit every batch_size files.
    The manifest's own file is excluded from the walk.
    A listing whose own directory was unreadable is reported and skipped:
    nothing under it is written or marked gone.
    After the walk, each live dir not walked and not under an unreadable
    dir is marked gone with its subtree; then scans.finish."""
    root = Path(manifest.meta.root)
    try:
        exclude = frozenset({to_rel(manifest.db.path, root)})
    except Err.NotUnderRoot:
        exclude = frozenset()
    started = manifest.scans.start()
    counts = {Outcome.ADDED: 0, Outcome.UPDATED: 0, Outcome.MATCHED: 0}
    errors = gone = 0
    batch = _Batch(manifest, batch_size)
    batch.open()
    walked: set[PPP] = set()
    unreadable: set[PPP] = set()
    for listing in walk(root, exclude):
        walked.add(listing.path)
        if any(e.path == listing.path for e in listing.errors):
            unreadable.add(listing.path)
            errors += len(listing.errors)
            yield from listing.errors
            continue
        records_iter = _scan_dir(
            manifest,
            root,
            listing,
            started,
            hash=hash,
            force=force,
            bits=bits,
            on_progress=on_progress,
        )
        for record in records_iter:
            if isinstance(record, Scanned):
                counts[record.outcome] += 1
                batch.tick()
            elif isinstance(record, Gone):
                gone += 1
            else:
                errors += 1
            yield record
    for d in manifest.dirs.descendants(PPP(".")):
        under_unreadable = any(u == d.path or u in d.path.parents for u in unreadable)
        if d.path in walked or under_unreadable:
            continue
        for record in _mark_dir_gone(manifest, d.path, started):
            gone += 1
            yield record
    files_seen = sum(counts.values())
    finished = manifest.scans.finish(started, files_seen)
    batch.close()
    yield Summary(
        started,
        finished,
        counts[Outcome.ADDED],
        counts[Outcome.UPDATED],
        counts[Outcome.MATCHED],
        errors,
        gone,
    )
