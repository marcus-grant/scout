# src/scout/lib/scan/__init__.py
"""The scan verb: walk a manifest's root and bring its rows up to date.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import functools
from collections.abc import Callable, Generator, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.event import AccessLost, FileScanned, ReadFailed, RecordGone
from scout.lib.fs.hash import hash_file
from scout.lib.fs.walk import FileStat, WalkedDir, walk
from scout.lib.manifest import Manifest
from scout.lib.models import DEFAULT_BITS, FileRecord, Hash, RecordChange
from scout.lib.scan import reconcile
from scout.lib.scan.hashing import HashingPolicy, hash_record
from scout.lib.scan.tally import ScanTally, Summary
from scout.lib.util import to_rel


@dataclass(frozen=True)
class ScanOptions:
    """The hashing hash_policy, hash width, commit batch and progress callback.
    scan unpacks it; nothing below scan sees it."""

    hash_policy: HashingPolicy = HashingPolicy.NEEDED
    on_progress: Callable[[int], None] | None = None
    exclude: frozenset[PPP] = frozenset()
    bits: int = DEFAULT_BITS
    batch_size: int = 256


@dataclass(frozen=True)
class ScanContext:
    """The manifest, start, hash policy and hash call of one run."""

    manifest: Manifest
    started: int
    hash_policy: HashingPolicy
    hash_path: Callable[[PPP], Hash | Err.Unreadable]


DEFAULT_OPTIONS = ScanOptions()


def _scan_file(
    ctx: ScanContext, dir_id: int, dir_path: PPP, stat: FileStat, row: FileRecord | None
) -> FileScanned | ReadFailed:
    """Bring one file's row current and say what was done.
    MATCHED writes nothing; ADDED and UPDATED hash under hash_policy and upsert.
    An unreadable file comes back as ReadFailed with nothing written."""
    rel = dir_path / stat.name
    change = reconcile.reconcile_file_stat(stat, row, hash_policy=ctx.hash_policy)
    if change is RecordChange.MATCHED:
        assert row is not None, "MATCHED implies a row"
        return FileScanned(rel, row, change)

    fresh = FileRecord(dir_id, stat)
    prepared = hash_record(fresh, rel, ctx.hash_policy, ctx.started, ctx.hash_path)
    if isinstance(prepared, Err.Unreadable):
        return ReadFailed(rel, prepared)
    upserted = ctx.manifest.files.upsert(prepared)
    ctx.manifest.wrote()

    return FileScanned(rel, upserted, change)


def _scan_dir(
    ctx: ScanContext, walked: WalkedDir
) -> Iterator[FileScanned | RecordGone | ReadFailed | AccessLost]:
    """Bring one walked directory's rows current, yielding as it goes.
    Unlistable: ReadFaild, then AccessLost if manifest claims subtree exists; no writes.
    Otherwise: upsert the dir, scan each listed file, mark the recorded
    files it no longer lists gone, then report its entry errors."""
    if walked.unlistable:
        yield from (ReadFailed(e.path, e) for e in walked.errors)
        claimed = ctx.manifest.claimed.counts(walked.path)
        if claimed.dirs or claimed.files:
            yield AccessLost(walked.path)
        return
    d = ctx.manifest.dirs.upsert(walked.path)
    records = ctx.manifest.files.in_dir(d.id)
    by_name = {r.stat.name: r for r in records}
    for st in walked.files:
        yield _scan_file(ctx, d.id, walked.path, st, by_name.get(st.name))

    for name in reconcile.reconcile_missing_files(walked, records):
        ctx.manifest.files.mark_gone_one(d.id, name, ctx.started)
        ctx.manifest.wrote()
        yield RecordGone(walked.path / name)
    yield from (ReadFailed(e.path, e) for e in walked.errors)


def update_from_walk(
    ctx: ScanContext, walked_dirs: Iterable[WalkedDir]
) -> Iterator[FileScanned | RecordGone | ReadFailed | AccessLost]:
    """Bring the manifest current with one walk's output, yielding as it goes.
    Runs _scan_dir on each WalkedDir in order;
    then each recorded dir the walk confirms absent is marked gone with its subtree,
    one RecordGone per path."""
    # Go through each walked dir, add to sets to track & yeild with _scan_dir of walked
    walked_sets = reconcile.WalkedPathSets()
    for walked in walked_dirs:
        walked_sets.add(walked)
        yield from _scan_dir(ctx, walked)

    # Determine missing dirs & mark them gone; yield RecordGone for each
    records = ctx.manifest.dirs.descendants(PPP("."))
    for missing in reconcile.reconcile_missing_dirs(walked_sets, records):
        for path in ctx.manifest.gone_subtree.mark(missing, ctx.started):
            ctx.manifest.wrote()
            yield RecordGone(path)


_SCANRESULT = FileScanned | RecordGone | ReadFailed | AccessLost | Summary


def scan(
    manifest: Manifest, tally: ScanTally, opt: ScanOptions = DEFAULT_OPTIONS
) -> Generator[_SCANRESULT, None, None]:
    """Walk manifest's root and bring every row current.
    Yields each event as it happens, seen by tally, then tally's Summary.
    Rows commit every batch_size writes; the manifest's own file is excluded.
    What a walk step covers, and what is marked gone, is update_from_walk's."""
    # Exclude the manifest's own file when it lives under root
    root = Path(manifest.meta.root)
    try:
        exclude = opt.exclude | frozenset({to_rel(manifest.db.path, root)})
    except Err.NotUnderRoot:
        exclude = opt.exclude

    # Open the scan and fix what the run holds
    ctx = ScanContext(
        manifest,
        started := manifest.scans.start(),
        opt.hash_policy,
        functools.partial(hash_file, root, bits=opt.bits, on_progress=opt.on_progress),
    )

    # Walk, counting every event, then close the scan row
    with manifest.commit_every(opt.batch_size):
        for record in update_from_walk(ctx, walk(root, exclude)):
            tally.see(record)
            yield record
        finished = manifest.scans.finish(started, tally.files_seen())

    yield tally.summary(started, finished)
