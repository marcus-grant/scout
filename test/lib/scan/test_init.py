# test/lib/scan/test_init.py
"""Pin lib.scan: the per-file decision and the scan run.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import errno
import functools
import os
import shutil
import sqlite3 as sql
from collections.abc import Callable
from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest
from b3c32 import code_from_chunks

import scout.lib.error as Err
from scout.lib.event import AccessLost, FileScanned, ReadFailed, RecordGone
from scout.lib.fs.hash import hash_file
from scout.lib.fs.walk import FileStat, WalkedDir
from scout.lib.manifest import Manifest
from scout.lib.models import DEFAULT_BITS, Hash, RecordChange
from scout.lib.scan import Summary, _scan_dir, _scan_file, scan
from scout.lib.scan.context import ScanContext
from scout.lib.scan.hashing import HashingPolicy

# Alias for factory:
# Creates default file with override kwargs:
# FileRecord(dir_id=0, stat=FileStat("f", 1, 1), hash=None, hashed=None, gone=None)
mk_stat = factory.mk_stat
mk_frec = factory.mk_file_record

# Shortened alias for HashingPolicy, RecordChange enum members
OFF = HashingPolicy.OFF
ADDED = RecordChange.ADDED
MATCHED = RecordChange.MATCHED
UPDATED = RecordChange.UPDATED

# The tree fixture (test/conftest.py) is factory.mk_tree(tmp_path) of:
#   files: a.txt "alpha", b/b1.txt "bravo", b/b2.txt "alpha", b/c/empty.txt ""
#   dirs:  d (empty); implied by the files: ., b, b/c
# The manifest fixture is factory.mk_manifest(tmp_path): .scout.db in that root.
Tree = factory.Tree


def _mk_fstat(**overrides) -> FileStat:
    """Makes a default FileStat with optional overrides.
    Defaults: name:str="f", size:int=1, mtime:int=1"""
    default = {"name": "f", "size": 1, "mtime": 1}
    return FileStat(**{**default, **overrides})


def _st(tree: Tree, rel: str) -> FileStat:
    """FileStat of tree.root / rel as the walker would report it."""
    st = (tree.root / rel).stat()
    return FileStat(PPP(rel).name, st.st_size, st.st_mtime_ns)


def _ctx(
    manifest: Manifest,
    root: Path,
    policy: HashingPolicy = HashingPolicy.NEEDED,
    hash_path: Callable[[Path], Hash | Err.Unreadable] | None = None,
) -> ScanContext:
    """A ScanContext started at 7, hashing with hash_file unless hash_path is given."""
    hasher = hash_path or functools.partial(hash_file, bits=DEFAULT_BITS)
    return ScanContext(manifest, root, 7, policy, hasher)


class TestScanFile:
    """_scan_file on one file under the manifest fixture's root."""

    def test_new_file_is_added_with_hash(self, tree: Tree, manifest: Manifest) -> None:
        """a.txt w/ no row: ADDED, hash equals b3c32 of b"alpha" (tree's a.txt bytes),
        hashed equals started, and files.get finds the row."""
        with manifest:
            st = _st(tree, "a.txt")
            result = _scan_file(_ctx(manifest, tree.root), 0, PPP("."), st, None)

        assert isinstance(result, FileScanned)
        assert manifest.files.get(0, "a.txt") == result.record

    def test_matching_row_is_left_alone(self, tree: Tree, manifest: Manifest) -> None:
        """A row equal to a.txt's stat: MATCHED, the returned FileRecord is the
        stored one, hashed unchanged."""
        h = Hash(code_from_chunks([b"alpha"], DEFAULT_BITS))
        st = _st(tree, "a.txt")
        fmodel = mk_frec(name="a.txt", size=st.size, mtime=st.mtime, hash=h, hashed=3)
        stored = manifest.files.upsert(fmodel)

        result = _scan_file(_ctx(manifest, tree.root), 0, PPP("."), st, stored)

        assert isinstance(result, FileScanned)
        assert result.record == stored
        assert manifest.files.get(0, "a.txt") == stored

    def test_no_hash_writes_null_hash(self, tree: Tree, manifest: Manifest) -> None:
        """A stale row for a.txt, policy OFF: the stale hash is not carried over."""
        st = _st(tree, "a.txt")
        stale = Hash(code_from_chunks([b"old"], DEFAULT_BITS))
        fmodel = mk_frec(name="a.txt", size=1, mtime=st.mtime, hash=stale, hashed=3)
        stored = manifest.files.upsert(fmodel)

        act = _scan_file(_ctx(manifest, tree.root, OFF), 0, PPP("."), st, stored)

        assert isinstance(act, FileScanned)
        assert act.record.hash is None
        assert act.record.hashed is None
        assert act.record.stat.size == st.size
        assert manifest.files.get(0, "a.txt") == act.record

    def test_unreadable_file_returns_error(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """hash_file answering Unreadable: a ReadFailed for b/a.txt returns with
        errno EACCES, and no row is written."""
        st = _st(tree, "a.txt")
        unreadable = Err.Unreadable("Permission denied", path=PPP("a.txt"), errno=13)

        ctx = _ctx(manifest, tree.root, hash_path=lambda _p: unreadable)
        act = _scan_file(ctx, 0, PPP("b"), st, None)

        assert isinstance(act, ReadFailed)
        assert act.path == PPP("b/a.txt")
        assert manifest.files.get(0, "a.txt") is None


class TestScanDir:
    """_scan_dir on one WalkedDir of the default tree."""

    def test_adds_dir_and_files_in_order(self, tree: Tree, manifest: Manifest) -> None:
        """The WalkedDir for b: dirs.get(b) exists after; two FileScanned records
        ADDED, paths b/b1.txt then b/b2.txt; no RecordGone, no Unreadable."""
        st_b1, st_b2 = _st(tree, "b/b1.txt"), _st(tree, "b/b2.txt")
        walked = WalkedDir(PPP("b"), ("c",), (st_b1, st_b2), ())

        records = list(_scan_dir(_ctx(manifest, tree.root), walked))

        assert manifest.dirs.get(PPP("b")) is not None
        assert [type(r) for r in records] == [FileScanned, FileScanned]
        assert [r.path for r in records] == [PPP("b/b1.txt"), PPP("b/b2.txt")]

    def test_missing_name_is_marked_gone(self, tree: Tree, manifest: Manifest) -> None:
        """A stored row b/old.txt not in the WalkedDir: one RecordGone with path
        b/old.txt after the FileScanned records; the row's gone is started."""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="old.txt"))
        st_b1, st_b2 = _st(tree, "b/b1.txt"), _st(tree, "b/b2.txt")
        walked = WalkedDir(PPP("b"), ("c",), (st_b1, st_b2), ())

        records = list(_scan_dir(_ctx(manifest, tree.root), walked))

        assert len(records) == 3
        assert records[-1] == RecordGone(PPP("b/old.txt"))
        with sql.connect(manifest.db.path) as conn:
            q = "SELECT gone FROM file WHERE name = 'old.txt';"
            assert conn.execute(q).fetchone() == (7,)

    def test_failed_entry_keeps_its_row(self, tree: Tree, manifest: Manifest) -> None:
        """A stored row b/b1.txt whose entry failed in the WalkedDir: the
        WalkedDir for b has only b2.txt in files and an Unreadable for
        b/b1.txt in errors. No RecordGone is yielded, and the row's gone stays None."""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="b1.txt"))
        failed = PPP("b/b1.txt")
        unreadable = Err.Unreadable("Input/output error", path=failed, errno=errno.EIO)
        walked = WalkedDir(PPP("b"), ("c",), (_st(tree, "b/b2.txt"),), (unreadable,))

        records = list(_scan_dir(_ctx(manifest, tree.root), walked))

        assert not any(isinstance(r, RecordGone) for r in records)
        with sql.connect(manifest.db.path) as conn:
            q = "SELECT gone FROM file WHERE name = 'b1.txt';"
            assert conn.execute(q).fetchone() == (None,)

    def test_listing_errors_are_yielded_last(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """A WalkedDir for b/c with no files and one Unreadable: dirs.get(b/c)
        exists, the one record yielded is a ReadFailed carrying it."""
        unreadable = Err.Unreadable("Permission denied", path=PPP("b/c"), errno=13)
        walked = WalkedDir(PPP("b/c"), (), (), (unreadable,))

        records = list(_scan_dir(_ctx(manifest, tree.root), walked))

        assert manifest.dirs.get(PPP("b/c")) is not None
        assert records == [ReadFailed(PPP("b/c"), unreadable)]

    def test_unlistable_with_claims_reports_access_lost(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """An unlistable b with a file claimed in it: its ReadFailed, then AccessLost."""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="old.txt"))
        err = Err.Unreadable("denied", path=PPP("b"), errno=13)
        walked = WalkedDir(PPP("b"), (), (), (err,), unlistable=True)

        records = list(_scan_dir(_ctx(manifest, tree.root), walked))

        assert records == [ReadFailed(PPP("b"), err), AccessLost(PPP("b"))]

    def test_unlistable_without_claims_reports_only_read_failed(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """An unlistable b with nothing claimed under it: only its ReadFailed."""
        err = Err.Unreadable("denied", path=PPP("b"), errno=13)
        walked = WalkedDir(PPP("b"), (), (), (err,), unlistable=True)

        records = list(_scan_dir(_ctx(manifest, tree.root), walked))

        assert records == [ReadFailed(PPP("b"), err)]


class TestScan:
    """scan on the default tree under the manifest fixture: wiring only."""

    def test_first_scan_counts_and_order(self, tree: Tree, manifest: Manifest) -> None:
        """Four FileScanned in DFS path order, all ADDED; no RecordGone; no row for
        .scout.db; Summary added 4, others 0, finished >= started; the
        scan row has that finished and files_seen 4."""
        records = list(scan(manifest))
        summary = records[-1]
        scanned = [r for r in records if isinstance(r, FileScanned)]

        assert isinstance(summary, Summary)
        assert [r.path for r in scanned] == sorted(tree.files)
        assert not any(isinstance(r, RecordGone) for r in records)
        assert manifest.files.get(0, ".scout.db") is None
        counts = (summary.added, summary.updated, summary.matched)
        assert (counts, summary.errors, summary.gone) == ((4, 0, 0), 0, 0)
        assert summary.finished >= summary.started
        with sql.connect(manifest.db.path) as conn:
            q = "SELECT finished, files_seen FROM scan WHERE started = ?;"
            row = conn.execute(q, (summary.started,)).fetchone()
        assert row == (summary.finished, 4)

    def test_rescan_after_rmtree_marks_subtree_gone(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """Scan, remove b, scan again: RecordGone paths are exactly b/b1.txt,
        b/b2.txt, b/c/empty.txt, b/c, b; Summary gone 5, matched 1."""
        list(scan(manifest))
        shutil.rmtree(tree.root / "b")

        records = list(scan(manifest))

        summary = records[-1]
        assert isinstance(summary, Summary)
        expected = {
            PPP(p) for p in ("b/b1.txt", "b/b2.txt", "b/c/empty.txt", "b/c", "b")
        }
        assert {r.path for r in records if isinstance(r, RecordGone)} == expected
        assert (summary.gone, summary.matched) == (5, 1)

    def test_unreadable_dir_keeps_its_rows(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Scan, then os.scandir monkeypatched to fail on b: one ReadFailed
        and one AccessLost for b, counted as one error, no RecordGone, and the
        rows for b, b/c, and their files keep gone None."""
        list(scan(manifest))
        real = os.scandir

        def fake(path):
            if Path(path) == tree.root / "b":
                raise PermissionError(errno.EACCES, "Permission denied")
            return real(path)

        monkeypatch.setattr("scout.lib.fs.walk.os.scandir", fake)

        records = list(scan(manifest))

        unreadable = [r for r in records if isinstance(r, ReadFailed)]
        assert [u.path for u in unreadable] == [PPP("b")]
        assert AccessLost(PPP("b")) in records
        summary = records[-1]
        assert isinstance(summary, Summary) and summary.errors == 1
        assert not any(isinstance(r, RecordGone) for r in records)
        with sql.connect(manifest.db.path) as conn:
            gone_dirs = conn.execute("SELECT count(*) FROM dir WHERE gone IS NOT NULL;")
            gone_files = conn.execute(
                "SELECT count(*) FROM file WHERE gone IS NOT NULL;"
            )
            assert (gone_dirs.fetchone()[0], gone_files.fetchone()[0]) == (0, 0)

    def test_failed_subdir_entry_keeps_its_subtree(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Scan, then walk patched so the root's WalkedDir reports b as a failed
        entry: b is absent from the root's subdirs, and an Unreadable for b is
        in its errors. No RecordGone, and the rows for b, b/c, and their files keep
        gone None."""
        list(scan(manifest))
        unreadable = Err.Unreadable(
            "Input/output error", path=PPP("b"), errno=errno.EIO
        )
        root_walked = WalkedDir(PPP("."), ("d",), (_st(tree, "a.txt"),), (unreadable,))
        d_walked = WalkedDir(PPP("d"), (), (), ())
        monkeypatch.setattr(
            "scout.lib.scan.walk", lambda *_: iter([root_walked, d_walked])
        )

        records = list(scan(manifest))

        assert not any(isinstance(r, RecordGone) for r in records)
        with sql.connect(manifest.db.path) as conn:
            gone_dirs = conn.execute("SELECT count(*) FROM dir WHERE gone IS NOT NULL;")
            gone_files = conn.execute(
                "SELECT count(*) FROM file WHERE gone IS NOT NULL;"
            )
            assert (gone_dirs.fetchone()[0], gone_files.fetchone()[0]) == (0, 0)

    def test_abort_keeps_committed_batches(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """batch_size 2, hash_file raising RuntimeError on its third call:
        scan raises; a second connection sees two file rows; the scan row
        has finished None."""
        real, calls = hash_file, [0]

        def flaky(*args, **kwargs):
            calls[0] += 1
            if calls[0] == 3:
                raise RuntimeError("boom")
            return real(*args, **kwargs)

        monkeypatch.setattr("scout.lib.scan.hash_file", flaky)

        with pytest.raises(RuntimeError):
            list(scan(manifest, batch_size=2))

        with sql.connect(manifest.db.path) as conn:
            files = conn.execute("SELECT count(*) FROM file;").fetchone()[0]
            finished = conn.execute("SELECT finished FROM scan;").fetchone()[0]
        assert (files, finished) == (2, None)
