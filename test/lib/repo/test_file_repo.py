# test/lib/repo/test_file_repo.py
"""Pin FileRepo, the owner of the file table.
Author: Marcus
Created: 2026-09-09
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from dataclasses import replace
from pathlib import PurePosixPath as PPP

from factory import mk_file_model

from scout.lib.manifest import Manifest
from scout.lib.model.file import File
from scout.lib.model.hash import Hash
from scout.lib.repo.file_repo import FileRepo


def as_row(f: File) -> tuple:
    """The raw file row for f: hash as its code text, in column order."""
    code = None if f.hash is None else f.hash.code
    return (f.dir_id, f.name, code, f.size, f.mtime, f.hashed, f.gone)


class TestSchema:
    """FileRepo.SCHEMA creates file keyed on (dir_id, name) and can run again."""

    def test_idempotent(self, manifest: Manifest) -> None:
        """Running SCHEMA twice leaves the file table empty and present."""
        with sql.connect(manifest.db.path) as conn:
            conn.executescript(FileRepo.SCHEMA)
            assert conn.execute("SELECT COUNT(*) FROM file;").fetchone() == (0,)


class TestAdd:
    """add upserts a File on (dir_id, name) and returns it live."""

    def test_returns_live_file(self, manifest: Manifest) -> None:
        """The File returned equals the one given with gone None."""
        fm = mk_file_model(hash=Hash("A" * 24), hashed=7)
        assert manifest.files.add(fm) == fm
        with sql.connect(manifest.db.path) as conn:
            assert conn.execute("SELECT * FROM file;").fetchall() == [as_row(fm)]

    def test_same_key_updates_content(self, manifest: Manifest) -> None:
        """Adding the same (dir_id, name) twice leaves one row with new content."""
        fm, repo = mk_file_model(hash=Hash("A" * 24), hashed=7), manifest.files
        repo.add(fm)
        repo.add(fm2 := replace(fm, size=2, hashed=8))
        with sql.connect(manifest.db.path) as conn:
            rows = conn.execute("SELECT * FROM file;").fetchall()
            assert rows == [as_row(fm2)]

    def test_revives_gone_row(self, manifest: Manifest) -> None:
        """Adding over a row whose gone is set clears gone."""
        fm, repo = mk_file_model(), manifest.files
        repo.add(fm)
        with sql.connect(manifest.db.path) as conn:
            conn.execute("UPDATE file SET gone = 42")
        assert repo.add(fm).gone is None


class TestGet:
    """get returns the live File at (dir_id, name), or None."""

    def test_returns_added(self, manifest: Manifest) -> None:
        """get returns the File add returned, and Nonefor an unknown name."""
        fm, repo = mk_file_model(), manifest.files
        assert repo.add(fm) == repo.get(fm.dir_id, fm.name)

    def test_hides_gone(self, manifest: Manifest) -> None:
        """A file whose gone is set reads as None."""
        fm, repo = mk_file_model(), manifest.files
        repo.add(fm)
        with sql.connect(manifest.db.path) as conn:
            conn.execute("UPDATE file SET gone = 5")
        assert repo.get(fm.dir_id, fm.name) is None


class TestInDir:
    """in_dir lists live files of one directory by name."""

    def test_lists_only_that_dir(self, manifest: Manifest) -> None:
        """Files in other dirs and gone files are left out; order is by name."""
        repo, d = manifest.files, manifest.dirs.add(PPP("d"))
        b, a, _ = (repo.add(mk_file_model(name=n)) for n in ("b", "a", "z"))
        repo.add(mk_file_model(dir_id=d.id, name="c"))
        with sql.connect(manifest.db.path) as conn:
            conn.execute("UPDATE file SET gone = 5 WHERE name = 'z';")
        assert repo.in_dir(0) == [a, b]


class TestByHash:
    """by_hash lists live files sharing one hash."""

    def test_lists_duplicates_across_dirs(self, manifest: Manifest) -> None:
        """Rows with the hash in any dir, ordered by dir_id then name."""
        repo, d, h = manifest.files, manifest.dirs.add(PPP("d")), Hash("A" * 24)
        x = repo.add(mk_file_model(dir_id=0, name="x", hash=h, hashed=5))
        y = repo.add(mk_file_model(dir_id=d.id, name="y", hash=h, hashed=5))
        assert repo.by_hash(h) == [x, y]


class TestMarkGone:
    """mark_gone sets gone on every live file in the given dirs."""

    def test_covers_listed_dirs_only(self, manifest: Manifest) -> None:
        """Files in listed dirs get gone; other dirs and root are untouched."""
        repo, dirs = manifest.files, manifest.dirs
        d, e = dirs.add(PPP("d")), dirs.add(PPP("e"))
        for dir_id in (0, d.id, e.id):
            repo.add(mk_file_model(dir_id=dir_id))
        repo.mark_gone([d.id, e.id], 9)
        with sql.connect(manifest.db.path) as conn:
            q = "SELECT dir_id, gone FROM file ORDER BY dir_id;"
            assert conn.execute(q).fetchall() == [(0, None), (d.id, 9), (e.id, 9)]
