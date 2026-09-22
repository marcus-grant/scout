# test/lib/test_scan.py
"""Pin lib.scan: the per-file decision and the scan run.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import errno
import os
import shutil
import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest
from b3c32 import code_from_chunks

import scout.lib.error as Err
from scout.lib.fs.hash import hash_file
from scout.lib.fs.walk import FileStat, Listing
from scout.lib.manifest import Manifest
from scout.lib.model.hash import DEFAULT_BITS, Hash
from scout.lib.scan import (
    Gone,
    Outcome,
    Scanned,
    Summary,
    _Batch,
    _mark_dir_gone,
    _scan_dir,
    _scan_file,
    decide,
    scan,
)

# Alias for factory:
# Creates default file with override kwargs:
# File(dir_id=0, name="f", size=1, mtime=1, hash=None, hashed=None, gone=None)
mk_fmodel = factory.mk_file_model


def _mk_fstat(**overrides) -> FileStat:
    """Makes a default FileStat with optional overrides.
    Defaults: name:str="f", size:int=1, mtime:int=1"""
    default = {"name": "f", "size": 1, "mtime": 1}
    return FileStat(**{**default, **overrides})


ADDED = Outcome.ADDED
MATCHED = Outcome.MATCHED
UPDATED = Outcome.UPDATED


class TestDecide:
    """decide maps a stored row and a fresh stat to one Outcome."""

    def test_no_row_is_added(self) -> None:
        """row None gives ADDED, with or without force."""
        assert decide(None, _mk_fstat()) == ADDED
        assert decide(None, _mk_fstat()) == ADDED

    def test_equal_stat_is_matched(self) -> None:
        """A row whose size and mtime equal the stat gives MATCHED."""
        assert decide(mk_fmodel(), _mk_fstat()) == MATCHED

    def test_size_or_mtime_change_is_updated(self) -> None:
        """A row differing from the stat in size only, or mtime only, gives UPDATED."""
        assert decide(mk_fmodel(size=2), _mk_fstat()) == UPDATED
        assert decide(mk_fmodel(mtime=2), _mk_fstat()) == UPDATED
        assert decide(mk_fmodel(size=2, mtime=2), _mk_fstat()) == UPDATED

    def test_force_updates_a_matching_row(self) -> None:
        """A row equal to the stat gives UPDATED when force is True."""
        assert decide(mk_fmodel(), _mk_fstat(), force=True) == UPDATED

    def test_null_hash_row_is_updated_when_hashing(self) -> None:
        """A row equal to the stat but with hash None gives UPDATED when
        hash is True and MATCHED when hash is False."""
        assert decide(mk_fmodel(), _mk_fstat(), hash=True) == UPDATED
        assert decide(mk_fmodel(), _mk_fstat(), hash=False) == MATCHED


# The tree fixture (test/conftest.py) is factory.mk_tree(tmp_path) of:
#   files: a.txt "alpha", b/b1.txt "bravo", b/b2.txt "alpha", b/c/empty.txt ""
#   dirs:  d (empty); implied by the files: ., b, b/c
# The manifest fixture is factory.mk_manifest(tmp_path): .scout.db in that root.
Tree = factory.Tree


def _fstat(tree: Tree, rel: str) -> FileStat:
    """FileStat of tree.root / rel as the walker would report it."""
    st = (tree.root / rel).stat()
    return FileStat(PPP(rel).name, st.st_size, st.st_mtime_ns)


class TestScanFile:
    """_scan_file on one file under the manifest fixture's root."""

    def test_new_file_is_added_with_hash(self, tree: Tree, manifest: Manifest) -> None:
        """a.txt w/ no row: ADDED, hash equals b3c32 of b"alpha" (tree's a.txt bytes),
        hashed equals started, and files.get finds the row."""
        expected = Hash(code_from_chunks([b"alpha"], DEFAULT_BITS))

        with manifest:
            fst = _fstat(tree, "a.txt")
            result = _scan_file(manifest, 0, PPP("."), tree.root, fst, started=7)

        assert isinstance(result, Scanned)
        assert result.outcome == ADDED
        assert result.file.hash == expected
        assert result.file.hashed == 7
        assert manifest.files.get(0, "a.txt") == result.file

    def test_matching_row_is_left_alone(self, tree: Tree, manifest: Manifest) -> None:
        """A row equal to a.txt's stat: MATCHED, the returned File is the
        stored one, hashed unchanged."""
        h = Hash(code_from_chunks([b"alpha"], DEFAULT_BITS))
        fst = _fstat(tree, "a.txt")
        fmodel = mk_fmodel(
            name="a.txt", size=fst.size, mtime=fst.mtime, hash=h, hashed=3
        )
        stored = manifest.files.add(fmodel)

        result = _scan_file(manifest, 0, PPP("."), tree.root, fst, started=7)

        assert isinstance(result, Scanned)
        assert result.outcome == MATCHED
        assert result.file == stored
        assert manifest.files.get(0, "a.txt") == stored

    def test_changed_file_is_updated_and_rehashed(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """A row for a.txt with size 1 and a stale hash: UPDATED, hash is the
        b3c32 of the current bytes, hashed equals started, size is current."""
        fst = _fstat(tree, "a.txt")
        new_h = Hash(code_from_chunks([b"alpha"], DEFAULT_BITS))
        stale = Hash(code_from_chunks([b"old"], DEFAULT_BITS))
        fmodel = mk_fmodel(name="a.txt", size=1, mtime=fst.mtime, hash=stale, hashed=3)
        manifest.files.add(fmodel)

        result = _scan_file(manifest, 0, PPP("."), tree.root, fst, started=7)

        assert isinstance(result, Scanned)
        assert result.outcome == UPDATED
        assert (result.file.hash, result.file.hashed) == (new_h, 7)
        assert result.file.size == fst.size

    def test_no_hash_writes_null_hash(self, tree: Tree, manifest: Manifest) -> None:
        """A row for a.txt with size 1, hash False: UPDATED, stored row has
        the true size, hash None, hashed None."""
        fst = _fstat(tree, "a.txt")
        stale = Hash(code_from_chunks([b"old"], DEFAULT_BITS))
        fmodel = mk_fmodel(name="a.txt", size=1, mtime=fst.mtime, hash=stale, hashed=3)
        manifest.files.add(fmodel)

        act = _scan_file(manifest, 0, PPP("."), tree.root, fst, started=7, hash=False)

        assert isinstance(act, Scanned)
        assert act.outcome == UPDATED
        assert act.file.hash is None
        assert act.file.hashed is None
        assert act.file.size == fst.size
        assert manifest.files.get(0, "a.txt") == act.file

    def test_unreadable_file_returns_error(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """hash_file answering Unreadable: that Unreadable comes back with
        errno EACCES, and no row is written."""
        fst = _fstat(tree, "a.txt")
        unreadable = Err.Unreadable("Permission denied", path=PPP("a.txt"), errno=13)
        monkeypatch.setattr("scout.lib.scan.hash_file", lambda *_a, **_k: unreadable)

        act = _scan_file(manifest, 0, PPP("b"), tree.root, fst, started=7)

        assert isinstance(act, Err.Unreadable)
        assert act.errno == errno.EACCES
        assert act.path == PPP("b/a.txt")
        assert manifest.files.get(0, "a.txt") is None


class TestScanDir:
    """_scan_dir on one Listing of the default tree."""

    def test_adds_dir_and_files_in_order(self, tree: Tree, manifest: Manifest) -> None:
        """The listing for b: dirs.get(b) exists after; two Scanned records
        ADDED, paths b/b1.txt then b/b2.txt; no Gone, no Unreadable."""
        fst_b1, fst_b2 = _fstat(tree, "b/b1.txt"), _fstat(tree, "b/b2.txt")
        listing = Listing(PPP("b"), (fst_b1, fst_b2), ())

        records = list(_scan_dir(manifest, tree.root, listing, started=7))

        assert manifest.dirs.get(PPP("b")) is not None
        assert [type(r) for r in records] == [Scanned, Scanned]
        assert [r.path for r in records] == [PPP("b/b1.txt"), PPP("b/b2.txt")]
        assert all(r.outcome == ADDED for r in records if isinstance(r, Scanned))

    def test_missing_name_is_marked_gone(self, tree: Tree, manifest: Manifest) -> None:
        """A stored row b/old.txt not in the listing: one Gone with path
        b/old.txt after the Scanned records; the row's gone is started."""
        d = manifest.dirs.add(PPP("b"))
        manifest.files.add(mk_fmodel(dir_id=d.id, name="old.txt"))
        fst_b1, fst_b2 = _fstat(tree, "b/b1.txt"), _fstat(tree, "b/b2.txt")
        listing = Listing(PPP("b"), (fst_b1, fst_b2), ())

        records = list(_scan_dir(manifest, tree.root, listing, started=7))

        assert len(records) == 3
        assert records[-1] == Gone(PPP("b/old.txt"))
        with sql.connect(manifest.db.path) as conn:
            q = "SELECT gone FROM file WHERE name = 'old.txt';"
            assert conn.execute(q).fetchone() == (7,)

    def test_listing_errors_are_yielded_last(
        self, tree: Tree, manifest: Manifest
    ) -> None:
        """A Listing for b/c with no files and one Unreadable: dirs.get(b/c)
        exists, the one record yielded is that Unreadable."""
        unreadable = Err.Unreadable("Permission denied", path=PPP("b/c"), errno=13)
        listing = Listing(PPP("b/c"), (), (unreadable,))

        records = list(_scan_dir(manifest, tree.root, listing, started=7))

        assert manifest.dirs.get(PPP("b/c")) is not None
        assert records == [unreadable]


class TestBatch:
    """_Batch commits the manifest only on size boundaries and on close."""

    def _count(self, manifest: Manifest) -> int:
        """File rows visible to a second connection, committed ones only."""
        with sql.connect(manifest.db.path) as conn:
            return conn.execute("SELECT count(*) FROM file;").fetchone()[0]

    def test_commits_on_size_boundary(self, manifest: Manifest) -> None:
        """size 2: after one add and tick a second sqlite3 connection sees
        no file rows; after the second add and tick it sees two."""
        batch = _Batch(manifest, 2)
        batch.open()
        manifest.files.add(mk_fmodel(name="one"))
        batch.tick()
        assert self._count(manifest) == 0
        manifest.files.add(mk_fmodel(name="two"))
        batch.tick()
        assert self._count(manifest) == 2
        batch.close()

    def test_close_commits_the_tail(self, manifest: Manifest) -> None:
        """size 5: one add and tick, then close; a second connection sees
        the row."""
        batch = _Batch(manifest, 5)
        batch.open()
        manifest.files.add(mk_fmodel(name="one"))
        batch.tick()
        assert self._count(manifest) == 0
        batch.close()
        assert self._count(manifest) == 1


class TestMarkDirGone:
    """_mark_dir_gone on a stored subtree with nothing on disk."""

    def test_marks_subtree_dirs_and_files(self, manifest: Manifest) -> None:
        """Stored b with b/x and b/c with b/c/y, plus root a: after
        _mark_dir_gone(b, 7) the Gone paths are exactly b/x, b/c/y, b/c, b;
        sqlite3 shows gone 7 on rows b, b/c, x, y and None on a."""
        b, c = manifest.dirs.add(PPP("b")), manifest.dirs.add(PPP("b/c"))
        manifest.files.add(mk_fmodel(dir_id=0, name="a"))
        manifest.files.add(mk_fmodel(dir_id=b.id, name="x"))
        manifest.files.add(mk_fmodel(dir_id=c.id, name="y"))

        gone = list(_mark_dir_gone(manifest, PPP("b"), started=7))

        expected = {PPP("b/x"), PPP("b/c/y"), PPP("b/c"), PPP("b")}
        assert {g.path for g in gone} == expected
        with sql.connect(manifest.db.path) as conn:
            dirs = dict(conn.execute("SELECT path, gone FROM dir;").fetchall())
            files = dict(conn.execute("SELECT name, gone FROM file;").fetchall())
        assert (dirs["b"], dirs["b/c"], dirs["."]) == (7, 7, None)
        assert (files["x"], files["y"], files["a"]) == (7, 7, None)


class TestScan:
    """scan on the default tree under the manifest fixture: wiring only."""

    def test_first_scan_counts_and_order(self, tree: Tree, manifest: Manifest) -> None:
        """Four Scanned in DFS path order, all ADDED; no Gone; no row for
        .scout.db; Summary added 4, others 0, finished >= started; the
        scan row has that finished and files_seen 4."""
        records = list(scan(manifest))
        summary = records[-1]
        scanned = [r for r in records if isinstance(r, Scanned)]

        assert isinstance(summary, Summary)
        assert [r.path for r in scanned] == sorted(tree.files)
        assert all(r.outcome == ADDED for r in scanned)
        assert not any(isinstance(r, Gone) for r in records)
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
        """Scan, remove b, scan again: Gone paths are exactly b/b1.txt,
        b/b2.txt, b/c/empty.txt, b/c, b; Summary gone 5, matched 1."""
        list(scan(manifest))
        shutil.rmtree(tree.root / "b")

        records = list(scan(manifest))

        summary = records[-1]
        assert isinstance(summary, Summary)
        expected = {
            PPP(p) for p in ("b/b1.txt", "b/b2.txt", "b/c/empty.txt", "b/c", "b")
        }
        assert {r.path for r in records if isinstance(r, Gone)} == expected
        assert (summary.gone, summary.matched) == (5, 1)

    def test_unreadable_dir_keeps_its_rows(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Scan, then os.scandir monkeypatched to fail on b: one Unreadable
        for b, no Gone, and the rows for b, b/c, and their files keep gone
        None."""
        list(scan(manifest))
        real = os.scandir

        def fake(path):
            if Path(path) == tree.root / "b":
                raise PermissionError(errno.EACCES, "Permission denied")
            return real(path)

        monkeypatch.setattr("scout.lib.fs.walk.os.scandir", fake)

        records = list(scan(manifest))

        unreadable = [r for r in records if isinstance(r, Err.Unreadable)]
        assert [u.path for u in unreadable] == [PPP("b")]
        assert not any(isinstance(r, Gone) for r in records)
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
