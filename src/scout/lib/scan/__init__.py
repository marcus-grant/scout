# src/scout/lib/scan/__init__.py
"""The scan verb: walk a manifest's root and bring its rows up to date.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import functools
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.event import FileScanned, ReadFailed, RecordGone
from scout.lib.fs.hash import hash_file
from scout.lib.fs.walk import FileStat, WalkedDir, walk
from scout.lib.manifest import Manifest
from scout.lib.models import DEFAULT_BITS, FileRecord, RecordChange
from scout.lib.scan.file_stats import reconcile_file_stat
from scout.lib.scan.hashing import HashingPolicy, hash_record
from scout.lib.scan.missing_files import reconcile_missing_files
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
    manifest: Manifest,
    dir_id: int,
    dir_rel: PPP,
    dir_abs: Path,
    stat: FileStat,
    started: int,
    *,
    policy: HashingPolicy = HashingPolicy.NEEDED,
    bits: int = DEFAULT_BITS,
    on_progress: Callable[[int], None] | None = None,
) -> FileScanned | ReadFailed:
    """Bring one file's row up to date and say what was done.
    Fetch the live row at (dir_id, stat.name); reconcile_file_stat decides.
    MATCHED: write nothing and return the row as found.
    ADDED or UPDATED: hash_record prepares the record to write under policy.
    An Unreadable from it comes back as a ReadFailed; nothing is written."""
    row = manifest.files.get(dir_id, stat.name)
    change = reconcile_file_stat(stat, row, policy=policy)
    if change is RecordChange.MATCHED:
        assert row is not None, "MATCHED implies a row"
        return FileScanned(dir_rel / stat.name, row, change)

    record = hash_record(
        FileRecord(dir_id, stat),
        dir_abs / stat.name,
        policy,
        started,
        functools.partial(hash_file, bits=bits, on_progress=on_progress),
    )
    if isinstance(record, Err.Unreadable):
        err = Err.Unreadable(
            str(record), path=dir_rel / stat.name, errno=record.errno
        )
        return ReadFailed(dir_rel / stat.name, err)
    file = manifest.files.upsert(record)

    return FileScanned(dir_rel / stat.name, file, change)


def _scan_dir(
    manifest: Manifest,
    root: Path,
    walked: WalkedDir,
    started: int,
    *,
    policy: HashingPolicy = HashingPolicy.NEEDED,
    bits: int = DEFAULT_BITS,
    on_progress: Callable[[int], None] | None = None,
) -> Iterator[FileScanned | RecordGone | ReadFailed]:
    """Bring one walked directory's rows current, yielding as it goes.
    dirs.upsert(walked.path) first; then one _scan_file per FileStat in
    walked.files order; then every live file row in this dir whose name is not
    in the WalkedDir is marked gone with started & yielded as RecordGone;
    last, a ReadFailed for each Unreadable the WalkedDir carried."""
    d = manifest.dirs.upsert(walked.path)
    for st in walked.files:
        args = (manifest, d.id, walked.path, root / walked.path, st, started)
        yield _scan_file(
            *args,
            policy=policy,
            bits=bits,
            on_progress=on_progress,
        )

    for name in reconcile_missing_files(walked, manifest.files.in_dir(d.id)):
        manifest.files.mark_gone_one(d.id, name, started)
        yield RecordGone(walked.path / name)
    yield from (ReadFailed(e.path, e) for e in walked.errors)


def scan(
    manifest: Manifest,
    *,
    policy: HashingPolicy = HashingPolicy.NEEDED,
    bits: int = DEFAULT_BITS,
    batch_size: int = 256,
    on_progress: Callable[[int], None] | None = None,
) -> Iterator[FileScanned | RecordGone | ReadFailed | Summary]:
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
    counts = {enum_member: 0 for enum_member in RecordChange}
    errors = gone = 0
    with manifest.commit_every(batch_size):
        walked_paths: set[PPP] = set()
        unreadable: set[PPP] = set()
        for walked in walk(root, exclude):
            walked_paths.add(walked.path)

            if walked.unlistable:
                unreadable.add(walked.path)
                errors += len(walked.errors)
                yield from (ReadFailed(e.path, e) for e in walked.errors)
                continue

            for failed in walked.errors:
                if failed.path is not None:
                    unreadable.add(failed.path)

            records_iter = _scan_dir(
                manifest,
                root,
                walked,
                started,
                policy=policy,
                bits=bits,
                on_progress=on_progress,
            )
            for record in records_iter:
                if isinstance(record, FileScanned):
                    counts[record.change] += 1
                    if record.change is not RecordChange.MATCHED:
                        manifest.wrote()
                elif isinstance(record, RecordGone):
                    gone += 1
                else:
                    errors += 1
                yield record

        for d in manifest.dirs.descendants(PPP(".")):
            under_unreadable = any(
                u == d.path or u in d.path.parents for u in unreadable
            )
            if d.path in walked_paths or under_unreadable:
                continue
            for path in manifest.gone_subtree.mark(d.path, started):
                gone += 1
                yield RecordGone(path)

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
