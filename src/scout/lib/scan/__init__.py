# src/scout/lib/scan/__init__.py
"""The scan verb: walk a manifest's root and bring its rows up to date.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.fs.hash import hash_file
from scout.lib.fs.walk import FileStat, WalkedDir, walk
from scout.lib.manifest import Manifest
from scout.lib.models import DEFAULT_BITS, FileRecord, RecordChange
from scout.lib.util import to_rel


@dataclass(frozen=True)
class Scanned:
    """One file as the scan left it: its root-relative path, its row, and
    what the scan did to that row."""

    path: PPP
    file: FileRecord
    change: RecordChange


@dataclass(frozen=True)
class Gone:
    """One row marked gone this scan: its root-relative path."""

    path: PPP


@dataclass(frozen=True)
class Summary:
    """What one scan did in total: its window and the count per change,
    plus the errors met and the rows marked gone."""

    started: int
    finished: int
    added: int
    updated: int
    matched: int
    errors: int
    gone: int


def _scan_file(
    manifest: Manifest,
    dir_id: int,
    dir_rel: PPP,
    dir_abs: Path,
    stat: FileStat,
    started: int,
    *,
    hash: bool = True,
    rehash: bool = False,
    bits: int = DEFAULT_BITS,
    on_progress: Callable[[int], None] | None = None,
) -> Scanned | Err.Unreadable:
    """Bring one file's row up to date and say what was done.
    Fetch the live row at (dir_id, stat.name); classify against stat.
    MATCHED: write nothing and return the row as found, unless rehash is set
    or hashing is on and the row has no hash; then it is handled as UPDATED.
    ADDED or UPDATED with hash: hash_file(dir_abs / stat.name), write the
    row with that hash and hashed = started.
    ADDED or UPDATED without hash: write the row with hash and hashed null.
    An Unreadable from hash_file is returned and nothing is written."""
    row = manifest.files.get(dir_id, stat.name)
    change = RecordChange.classify(stat, row)
    if hash and row and row.hash is None:
        change = RecordChange.UPDATED  # TODO: Interim till _should_hash (scan restruct)
    if rehash and change is RecordChange.MATCHED:
        change = RecordChange.UPDATED  # TODO: Interim till _should_hash (scan restruct)
    if change is RecordChange.MATCHED:
        assert row is not None, "MATCHED implies a row"
        return Scanned(dir_rel / stat.name, row, change)

    h, hashed = None, None
    if hash:
        h = hash_file(dir_abs / stat.name, bits, on_progress=on_progress)
        if isinstance(h, Err.Unreadable):
            return Err.Unreadable(str(h), path=dir_rel / stat.name, errno=h.errno)
        hashed = started
    file = manifest.files.upsert(FileRecord(dir_id, stat, h, hashed))

    return Scanned(dir_rel / stat.name, file, change)


def _scan_dir(
    manifest: Manifest,
    root: Path,
    listing: WalkedDir,
    started: int,
    *,
    hash: bool = True,
    rehash: bool = False,
    bits: int = DEFAULT_BITS,
    on_progress: Callable[[int], None] | None = None,
) -> Iterator[Scanned | Gone | Err.Unreadable]:
    """Bring one walked directory's rows current, yielding as it goes.
    dirs.upsert(listing.path) first; then one _scan_file per FileStat in
    listing order; then every live file row in this dir whose name is not
    in the listing is marked gone with started and yielded as Gone; last,
    each Unreadable the listing carried."""
    d = manifest.dirs.upsert(listing.path)
    for st in listing.files:
        yield _scan_file(
            manifest,
            d.id,
            listing.path,
            root / listing.path,
            st,
            started,
            hash=hash,
            rehash=rehash,
            bits=bits,
            on_progress=on_progress,
        )

    # A row is marked gone only when its name is absent from the listing.
    # A failed entry was listed, so it exists; only its stat is unknown.
    file_names = {st.name for st in listing.files}
    failed_names = {e.path.name for e in listing.errors if e.path is not None}
    present = file_names | failed_names
    for row in manifest.files.in_dir(d.id):
        if row.stat.name not in present:
            manifest.files.mark_gone_one(d.id, row.stat.name, started)
            yield Gone(listing.path / row.stat.name)
    yield from listing.errors


def scan(
    manifest: Manifest,
    *,
    hash: bool = True,
    rehash: bool = False,
    bits: int = DEFAULT_BITS,
    batch_size: int = 256,
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
    counts = {enum_member: 0 for enum_member in RecordChange}
    errors = gone = 0
    with manifest.commit_every(batch_size):
        walked: set[PPP] = set()
        unreadable: set[PPP] = set()
        for listing in walk(root, exclude):
            walked.add(listing.path)

            if listing.unlistable:
                unreadable.add(listing.path)
                errors += len(listing.errors)
                yield from listing.errors
                continue

            for failed in listing.errors:
                if failed.path is not None:
                    unreadable.add(failed.path)

            records_iter = _scan_dir(
                manifest,
                root,
                listing,
                started,
                hash=hash,
                rehash=rehash,
                bits=bits,
                on_progress=on_progress,
            )
            for record in records_iter:
                if isinstance(record, Scanned):
                    counts[record.change] += 1
                    if record.change is not RecordChange.MATCHED:
                        manifest.wrote()
                elif isinstance(record, Gone):
                    gone += 1
                else:
                    errors += 1
                yield record

        for d in manifest.dirs.descendants(PPP(".")):
            under_unreadable = any(
                u == d.path or u in d.path.parents for u in unreadable
            )
            if d.path in walked or under_unreadable:
                continue
            for path in manifest.gone_subtree.mark(d.path, started):
                gone += 1
                yield Gone(path)

        files_seen = sum(counts.values())
        finished = manifest.scans.finish(started, files_seen)

    yield Summary(
        started,
        finished,
        counts[RecordChange.ADDED],
        counts[RecordChange.UPDATED],
        counts[RecordChange.MATCHED],
        errors,
        gone,
    )
