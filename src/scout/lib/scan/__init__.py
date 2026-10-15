# src/scout/lib/scan/__init__.py
"""The scan verb: walk a manifest's root and bring its rows up to date.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import functools
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.event import AccessLost, FileScanned, ReadFailed, RecordGone
from scout.lib.fs.hash import hash_file
from scout.lib.fs.walk import FileStat, WalkedDir, WalkedPathSets, walk
from scout.lib.manifest import Manifest
from scout.lib.models import DEFAULT_BITS, FileRecord, RecordChange
from scout.lib.scan.context import ScanContext
from scout.lib.scan.file_stats import reconcile_file_stat
from scout.lib.scan.hashing import HashingPolicy, hash_record
from scout.lib.scan.missing import reconcile_missing_dirs, reconcile_missing_files
from scout.lib.util import to_rel


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
    ctx: ScanContext, dir_id: int, dir_path: PPP, stat: FileStat, row: FileRecord | None
) -> FileScanned | ReadFailed:
    """Bring one file's row up to date and say what was done.
    Row is the file's live record, or None; reconcile_file_stat decides.
    MATCHED: write nothing and return the row as found.
    ADDED or UPDATED: hash_record prepares the record to write under policy.
    An Unreadable from it comes back as a ReadFailed; nothing is written."""
    rel = dir_path / stat.name
    change = reconcile_file_stat(stat, row, policy=ctx.policy)
    if change is RecordChange.MATCHED:
        assert row is not None, "MATCHED implies a row"
        return FileScanned(rel, row, change)

    fresh = FileRecord(dir_id, stat)
    prepared = hash_record(fresh, rel, ctx.policy, ctx.started, ctx.hash_path)
    if isinstance(prepared, Err.Unreadable):
        return ReadFailed(rel, prepared)
    upserted = ctx.manifest.files.upsert(prepared)
    ctx.manifest.wrote()

    return FileScanned(rel, upserted, change)


def _scan_dir(
    ctx: ScanContext, walked: WalkedDir
) -> Iterator[FileScanned | RecordGone | ReadFailed | AccessLost]:
    """Bring one walked directory's rows current, yielding as it goes.
    An unlistable WalkedDir yields its ReadFailed, then AccessLost when the
    manifest claims anything under it, and writes nothing.
    dirs.upsert(walked.path) first; then one _scan_file per FileStat in
    walked.files order; then every live file row in this dir whose name is not
    in the WalkedDir is marked gone with started & yielded as RecordGone;
    last, a ReadFailed for each Unreadable the WalkedDir carried."""
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

    for name in reconcile_missing_files(walked, records):
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
    walked_sets = WalkedPathSets()
    for walked in walked_dirs:
        walked_sets.add(walked)
        yield from _scan_dir(ctx, walked)

    # Determine missing dirs & mark them gone; yield RecordGone for each
    records = ctx.manifest.dirs.descendants(PPP("."))
    for missing in reconcile_missing_dirs(walked_sets, records):
        for path in ctx.manifest.gone_subtree.mark(missing, ctx.started):
            ctx.manifest.wrote()
            yield RecordGone(path)


def scan(
    manifest: Manifest,
    *,
    policy: HashingPolicy = HashingPolicy.NEEDED,
    bits: int = DEFAULT_BITS,
    batch_size: int = 256,
    on_progress: Callable[[int], None] | None = None,
) -> Iterator[FileScanned | RecordGone | ReadFailed | AccessLost | Summary]:
    """Walk manifest's root and bring every row current, yielding records
    as they happen and one Summary last.
    started comes from scans.start(); rows commit every batch_size files.
    The manifest's own file is excluded from the walk.
    A WalkedDir whose own directory was unreadable is reported and skipped:
    nothing under it is written or marked gone.
    After the walk, each live dir not walked and not under an unreadable
    dir is marked gone with its subtree; then scans.finish."""
    root = Path(manifest.meta.root)
    try:
        exclude = frozenset({to_rel(manifest.db.path, root)})
    except Err.NotUnderRoot:
        exclude = frozenset()
    started = manifest.scans.start()
    hash_path = functools.partial(hash_file, root, bits=bits, on_progress=on_progress)
    ctx = ScanContext(manifest, started, policy, hash_path)
    counts = {enum_member: 0 for enum_member in RecordChange}
    errors = gone = 0
    with manifest.commit_every(batch_size):
        for record in update_from_walk(ctx, walk(root, exclude)):
            if isinstance(record, FileScanned):
                counts[record.change] += 1
            elif isinstance(record, RecordGone):
                gone += 1
            elif isinstance(record, ReadFailed):
                errors += 1
            yield record

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
