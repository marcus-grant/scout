# test/lib/scan/test_scan.py
"""Pin lib.scan: the per-file decision and the scan run.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import errno
import shutil
import sqlite3 as sql
from collections.abc import Callable
from pathlib import PurePosixPath as PPP

import factory
from b3c32 import code_from_chunks

import scout.lib.error as Err
from scout.lib.event import AccessLost, FileScanned, ReadFailed, RecordGone
from scout.lib.fs.walk import WalkedDir
from scout.lib.manifest import Manifest
from scout.lib.models import DEFAULT_BITS, Hash, RecordChange
from scout.lib.scan import (
    ScanContext,
    ScanOptions,
    _scan_dir,
    _scan_file,
    scan,
    update_from_walk,
)
from scout.lib.scan.hashing import HashingPolicy
from scout.lib.scan.tally import ScanTally, Summary

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

# TODO: DELETME
Tree = factory.Tree  # type: ignore[assignment]


def _fake_hash(rel: PPP) -> Hash:
    """The Hash of rel's own text: distinct per path, no disk read."""
    return Hash(code_from_chunks([str(rel).encode()], DEFAULT_BITS))


def _ctx(
    manifest: Manifest,
    hash_policy: HashingPolicy = HashingPolicy.NEEDED,
    hash_path: Callable[[PPP], Hash | Err.Unreadable] = _fake_hash,
) -> ScanContext:
    """A ScanContext started at 7, hashing with _fake_hash unless hash_path is given."""
    return ScanContext(manifest, 7, hash_policy, hash_path)


def _committed(manifest: Manifest, query: str) -> tuple | None:
    """Alias for returning a manifest's db query shortened."""
    with sql.connect(manifest.db.path) as conn:
        return conn.execute(query).fetchone()


def _ticked(manifest: Manifest, act: Callable[[], object], query: str) -> tuple | None:
    """The row a second connection sees after act runs under commit_every(1)."""
    with manifest.commit_every(1):
        act()
        return _committed(manifest, query)


class TestScanFile:
    """_scan_file on one file under the manifest fixture's root."""

    def test_new_file_is_added_with_hash(self, manifest: Manifest) -> None:
        """a.txt w/ no row: ADDED, hash equals b3c32 of b"alpha" (tree's a.txt bytes),
        hashed equals started, and files.get finds the row."""
        with manifest:
            st = mk_stat(name="a.txt")
            result = _scan_file(_ctx(manifest), 0, PPP("."), st, None)

        assert isinstance(result, FileScanned)
        assert manifest.files.get(0, "a.txt") == result.record
        assert result.record.hash == _fake_hash(PPP("a.txt"))
        assert result.record.hashed == 7

    def test_matching_row_is_left_alone(self, manifest: Manifest) -> None:
        """A row equal to a.txt's stat: MATCHED, the returned FileRecord is the
        stored one, hashed unchanged."""
        st = mk_stat(name="a.txt")
        h = Hash(code_from_chunks([b"alpha"], DEFAULT_BITS))
        frec = mk_frec(name="a.txt", hash=h, hashed=3)
        stored = manifest.files.upsert(frec)

        result = _scan_file(_ctx(manifest), 0, PPP("."), st, stored)

        assert isinstance(result, FileScanned)
        assert result.record == stored
        assert manifest.files.get(0, "a.txt") == stored

    def test_no_hash_writes_null_hash(self, manifest: Manifest) -> None:
        """A stale row for a.txt, hash_policy OFF: the stale hash is not carried over."""
        st = mk_stat(name="a.txt", size=2)
        stale = Hash(code_from_chunks([b"old"], DEFAULT_BITS))
        fmodel = mk_frec(name="a.txt", hash=stale, hashed=3)
        stored = manifest.files.upsert(fmodel)

        act = _scan_file(_ctx(manifest, OFF), 0, PPP("."), st, stored)

        assert isinstance(act, FileScanned)
        assert act.record.hash is None
        assert act.record.hashed is None
        assert act.record.stat.size == st.size
        assert manifest.files.get(0, "a.txt") == act.record

    def test_unreadable_file_returns_error(self, manifest: Manifest) -> None:
        """hash_file answering Unreadable: a ReadFailed for a.txt returns with
        errno EACCES, and no row is written."""
        st = mk_stat(name="a.txt")

        def unreadable(rel: PPP) -> Err.Unreadable:
            return Err.Unreadable("Permission denied", path=rel, errno=errno.EACCES)

        act = _scan_file(_ctx(manifest, hash_path=unreadable), 0, PPP("."), st, None)

        assert isinstance(act, ReadFailed)
        assert act.path == PPP("a.txt")
        assert act.error.path == act.path
        assert act.error.errno == errno.EACCES
        assert manifest.files.get(0, "a.txt") is None

    def test_write_ticks_the_manifest(self, manifest: Manifest) -> None:
        """A file marked gone committed on the tick following it."""

        def act() -> None:
            _scan_file(_ctx(manifest), 0, PPP("."), mk_stat(name="a.txt"), None)

        assert _ticked(manifest, act, "SELECT count(*) FROM file;") == (1,)


class TestScanDir:
    """_scan_dir on one WalkedDir of the default tree."""

    def test_adds_dir_and_files_in_order(self, manifest: Manifest) -> None:
        """The WalkedDir for b: dirs.get(b) exists after; two FileScanned records
        ADDED, paths b/b1.txt then b/b2.txt; no RecordGone, no Unreadable."""
        st_b1, st_b2 = mk_stat(name="b1.txt"), mk_stat(name="b2.txt")
        walked = WalkedDir(PPP("b"), ("c",), (st_b1, st_b2), ())

        records = list(_scan_dir(_ctx(manifest), walked))

        assert manifest.dirs.get(PPP("b")) is not None
        assert [type(r) for r in records] == [FileScanned, FileScanned]
        assert [r.path for r in records] == [PPP("b/b1.txt"), PPP("b/b2.txt")]

    def test_missing_name_is_marked_gone(self, manifest: Manifest) -> None:
        """A stored row absent from WalkedDir yields RecordGone last & is marked gone"""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="old.txt"))
        st_b1, st_b2 = mk_stat(name="b1.txt"), mk_stat(name="b2.txt")
        walked = WalkedDir(PPP("b"), ("c",), (st_b1, st_b2), ())

        records = list(_scan_dir(_ctx(manifest), walked))

        assert len(records) == 3
        assert records[-1] == RecordGone(PPP("b/old.txt"))
        q = "SELECT gone FROM file WHERE name = 'old.txt';"
        assert _committed(manifest, q) == (7,)

    def test_failed_entry_keeps_its_row(self, manifest: Manifest) -> None:
        """Stored row whose entry failed in WalkedDir isn't marked gone."""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="b1.txt"))
        failed = PPP("b/b1.txt")
        unreadable = Err.Unreadable("Input/output error", path=failed, errno=errno.EIO)
        walked = WalkedDir(PPP("b"), ("c",), (mk_stat(name="b2.txt"),), (unreadable,))

        records = list(_scan_dir(_ctx(manifest), walked))

        assert not any(isinstance(r, RecordGone) for r in records)
        q = "SELECT gone FROM file WHERE name = 'b1.txt';"
        assert _committed(manifest, q) == (None,)

    def test_listing_errors_are_yielded_last(self, manifest: Manifest) -> None:
        """A WalkedDir for b/c with no files and one Unreadable: dirs.get(b/c)
        exists, the one record yielded is a ReadFailed carrying it."""
        unreadable = Err.Unreadable("Permission denied", path=PPP("b/c"), errno=13)
        walked = WalkedDir(PPP("b/c"), (), (), (unreadable,))

        records = list(_scan_dir(_ctx(manifest), walked))

        assert manifest.dirs.get(PPP("b/c")) is not None
        assert records == [ReadFailed(PPP("b/c"), unreadable)]

    def test_unlistable_with_claims_reports_lost(self, manifest: Manifest) -> None:
        """An unlistable b with a file claimed in it: its ReadFailed, then AccessLost."""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="old.txt"))
        err = Err.Unreadable("denied", path=PPP("b"), errno=13)
        walked = WalkedDir(PPP("b"), (), (), (err,), unlistable=True)

        records = list(_scan_dir(_ctx(manifest), walked))

        assert records == [ReadFailed(PPP("b"), err), AccessLost(PPP("b"))]

    def test_unlistable_without_claims_reports_only_read_failed(
        self, manifest: Manifest
    ) -> None:
        """An unlistable b with nothing claimed under it: only its ReadFailed."""
        err = Err.Unreadable("denied", path=PPP("b"), errno=13)
        walked = WalkedDir(PPP("b"), (), (), (err,), unlistable=True)

        records = list(_scan_dir(_ctx(manifest), walked))

        assert records == [ReadFailed(PPP("b"), err)]

    def test_gone_mark_ticks_the_manifest(self, manifest: Manifest) -> None:
        """A file marked gone is committed on tick that follows it."""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="old.txt"))

        def act() -> None:
            list(_scan_dir(_ctx(manifest), WalkedDir(PPP("b"), (), (), ())))

        query = "SELECT gone FROM file WHERE name = 'old.txt';"
        assert _ticked(manifest, act, query) == (7,)


class TestUpdateFromWalk:
    """update_from_walk over constructed WalkedDir values: no disk, no patch."""

    def _gone_counts(self, manifest: Manifest) -> tuple[int, int]:
        """Counts of dir rows & file rows with 'gone' column set."""
        with sql.connect(manifest.db.path) as conn:
            gone_d = conn.execute("SELECT count(*) FROM dir WHERE gone IS NOT NULL;")
            gone_f = conn.execute("SELECT count(*) FROM file WHERE gone IS NOT NULL;")
            return (gone_d.fetchone()[0], gone_f.fetchone()[0])

    def test_unreadable_dir_keeps_its_rows(self, manifest: Manifest) -> None:
        """An unlistable b over rows for b, b/c and a file in b:
        nothing is marked gone."""
        d = manifest.dirs.upsert(PPP("b"))
        manifest.dirs.upsert(PPP("b/c"))
        manifest.files.upsert(mk_frec(dir_id=d.id, name="b1.txt"))
        err = Err.Unreadable("Permission denied", path=PPP("b"), errno=errno.EACCES)
        walked = WalkedDir(PPP("b"), (), (), (err,), unlistable=True)

        records = list(update_from_walk(_ctx(manifest), [walked]))
        assert not any(isinstance(r, RecordGone) for r in records)
        assert self._gone_counts(manifest) == (0, 0)

    def test_failed_subdir_entry_keeps_its_subtree(self, manifest: Manifest) -> None:
        """The root's listing reports b as failed entry over rows b & b/c: no gone."""
        manifest.dirs.upsert(PPP("b"))
        manifest.dirs.upsert(PPP("b/c"))
        unreadable = Err.Unreadable("I/O error", path=PPP("b"), errno=errno.EIO)
        walked = WalkedDir(PPP("."), (), (), (unreadable,))

        records = list(update_from_walk(_ctx(manifest), [walked]))
        assert not any(isinstance(r, RecordGone) for r in records)
        assert self._gone_counts(manifest) == (0, 0)

    def test_walked_file_is_scanned(self, manifest: Manifest) -> None:
        """A walk listing a.txt at the root yields one FileScanned for a.txt."""
        walked = WalkedDir(PPP("."), (), (mk_stat(name="a.txt"),), ())

        records = list(update_from_walk(_ctx(manifest), [walked]))
        assert [r.path for r in records if isinstance(r, FileScanned)] == [PPP("a.txt")]

    def test_unwalked_dir_is_marked_gone(self, manifest: Manifest) -> None:
        """A recorded dir b absent from a walk of the root yields RecordGone for b."""
        manifest.dirs.upsert(PPP("b"))
        walked = WalkedDir(PPP("."), (), (), ())

        records = list(update_from_walk(_ctx(manifest), [walked]))
        assert records == [RecordGone(PPP("b"))]

    def test_gone_subtree_ticks_the_manifest(self, manifest: Manifest) -> None:
        """A dir marked gone by the gone pass is committed on tick that follows it."""
        manifest.dirs.upsert(PPP("b"))

        def act() -> None:
            list(update_from_walk(_ctx(manifest), [WalkedDir(PPP("."), (), (), ())]))

        assert _ticked(manifest, act, "SELECT gone FROM dir WHERE path = 'b';") == (7,)


class TestScan:
    """scan on the default tree under the manifest fixture: wiring only."""

    def test_first_scan_counts_and_order(self, tree: Tree, manifest: Manifest) -> None:
        """Four FileScanned in DFS order, all ADDED, none for .scout.db;
        the given tally counts them and the scan row records finished and 4 seen."""
        records = list(scan(manifest, tally := ScanTally()))
        summary = records[-1]
        scanned = [r for r in records if isinstance(r, FileScanned)]

        assert isinstance(summary, Summary)
        assert [r.path for r in scanned] == sorted(tree.files)
        assert not any(isinstance(r, RecordGone) for r in records)
        assert manifest.files.get(0, ".scout.db") is None
        assert summary.finished >= summary.started
        with sql.connect(manifest.db.path) as conn:
            q = "SELECT finished, files_seen FROM scan WHERE started = ?;"
            row = conn.execute(q, (summary.started,)).fetchone()
        assert row == (summary.finished, 4)
        assert tally.summary(1, 2).added == 4

    def test_rescan_after_rmtree_marks_subtree_gone(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """Scan, remove b, scan again: RecordGone paths are exactly b/b1.txt,
        b/b2.txt, b/c/empty.txt, b/c, b; Summary gone 5, matched 1."""
        list(scan(manifest, ScanTally()))
        shutil.rmtree(tree.root / "b")

        records = list(scan(manifest, ScanTally()))

        summary = records[-1]
        assert isinstance(summary, Summary)
        paths = ("b/b1.txt", "b/b2.txt", "b/c/empty.txt", "b/c", "b")
        expected = {PPP(p) for p in paths}
        assert {r.path for r in records if isinstance(r, RecordGone)} == expected
        assert (summary.gone, summary.matched) == (5, 1)

    def test_options_reach_the_context(self, tree: Tree, manifest: Manifest) -> None:
        """ScanOptions(hash_policy=OFF) leaves every hash null."""
        list(scan(manifest, ScanTally(), ScanOptions(hash_policy=OFF)))
        query = "SELECT count(*) FROM file WHERE hash IS NOT NULL;"
        assert _committed(manifest, query) == (0,)

    def test_excluded_dir_is_not_scanned(self, tree: Tree, manifest: Manifest) -> None:
        """ScanOptions(exclude={b}) records nothing under b."""
        list(scan(manifest, ScanTally(), ScanOptions(exclude=frozenset({PPP("b")}))))
        query = "SELECT count(*) FROM dir WHERE path = 'b';"
        assert _committed(manifest, query) == (0,)

    def test_own_manifest_is_not_scanned(self, tree: Tree, manifest: Manifest) -> None:
        """A scan records no file row for the manifest's own .scout.db."""
        list(scan(manifest, ScanTally()))
        query = "SELECT count(*) FROM file WHERE name = '.scout.db';"
        assert _committed(manifest, query) == (0,)

    def test_abort_leaves_finished_null(self, tree: Tree, manifest: Manifest) -> None:
        """A scan closed after its first event leaves its scan row unfinished."""
        next(scan_iter := scan(manifest, ScanTally()))
        scan_iter.close()
        assert _committed(manifest, "SELECT finished FROM scan;") == (None,)
