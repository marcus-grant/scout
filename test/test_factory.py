# test/test_factory.py
"""Pin what the test factories produce.

Author: Marcus
Created: 2026-09-04
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest

from scout.lib.models import FileRecord, FileStat

mk_file = factory.mk_file
mk_dir = factory.mk_dir
mk_tree = factory.mk_tree
mk_stat = factory.mk_stat
mk_file_record = factory.mk_file_record


class TestMkFile:
    """mk_file writes one file under root."""

    def test_writes_content_under_root(self, tmp_path: Path) -> None:
        """Creates parents, writes the bytes, returns the absolute path."""
        path = mk_file(tmp_path, rel := "a/b/hello.txt", content := b"Hello, World!")
        assert path == tmp_path / rel
        assert path.read_bytes() == content


class TestMkDir:
    """mk_dir creates one directory under root."""

    def test_creates_nested_dirs(self, tmp_path: Path) -> None:
        """Creates the directory with parents and returns its path."""
        result = mk_dir(tmp_path, path_str := "a/b/c/d/e/f")
        assert result == tmp_path / path_str
        for parent in result.parents:
            assert parent.exists(), f"parent {parent} directory should exist"
        assert result.is_dir(), f"{result} should be a directory"


class TestMkTree:
    """mk_tree builds a whole tree under root."""

    def test_returns_tree_describing_defaults(self, tmp_path: Path) -> None:
        """The returned Tree records root and the default files and dirs."""
        assert isinstance((tree := mk_tree(tmp_path)), factory.Tree)
        assert tree.root == tmp_path
        assert tree.files == factory.DEFAULT_FILES
        assert tree.dirs == factory.DEFAULT_DIRS

    @pytest.mark.parametrize(("rel", "d"), list(factory.DEFAULT_FILES.items()))
    def test_default_writes_expected_files(self, tmp_path: Path, rel: PPP, d) -> None:
        """Every DEFAULT_FILES entry exists with its content."""
        mk_tree(tmp_path)
        assert (tmp_path / rel).read_bytes() == d

    def test_default_creates_empty_dir(self, tmp_path: Path) -> None:
        """The default tree contains the empty directory d/."""
        mk_tree(tmp_path)
        assert (tmp_path / "d").is_dir()
        assert list((tmp_path / "d").iterdir()) == []

    def test_default_has_no_extra_entries(self, tmp_path: Path) -> None:
        """Nothing exists under root beyond the default files and dirs."""
        tree = mk_tree(tmp_path)
        file_paths = [p for p in tmp_path.rglob("*") if p.is_file()]
        found = {PPP(p.relative_to(tmp_path).as_posix()) for p in file_paths}
        assert found == set(tree.files)

    def test_override_replaces_defaults(self, tmp_path: Path) -> None:
        """Passing files and dirs replaces the default sets entirely."""
        tree = mk_tree(tmp_path, files={"x.txt": b"x"}, dirs=["y"])
        assert tree.files == {PPP("x.txt"): b"x"}
        assert tree.dirs == [PPP("y")]
        assert (tmp_path / "x.txt").read_bytes() == b"x"
        assert (tmp_path / "y").is_dir()
        assert not (tmp_path / "a.txt").exists()
        assert not (tmp_path / "d").exists()

    def test_empty_override_builds_nothing(self, tmp_path: Path) -> None:
        """Empty files and dirs give an empty root, not the defaults."""
        assert (tree := mk_tree(tmp_path, files={}, dirs=[])).files == {}
        assert tree.dirs == []
        assert list(tmp_path.iterdir()) == []


class TestMkStat:
    """mk_stat builds a FileStat from defaults and overrides."""

    def test_defaults(self) -> None:
        """No args yields FileStat("f", 1, 1)."""
        assert mk_stat() == FileStat("f", 1, 1)

    def test_overrides(self) -> None:
        """Positional overrides land on name, size, and mtime."""
        assert mk_stat("x", 42, 2**30) == FileStat("x", 42, 2**30)


class TestMkFileRecord:
    """mk_file_record builds a FileRecord from defaults and overrides."""

    def test_defaults(self) -> None:
        """No args yields FileRecord(0, FileStat("f", 1, 1)) with hash,
        hashed, gone None."""
        assert mk_file_record() == FileRecord(0, mk_stat())

    def test_scalar_overrides(self) -> None:
        """name, size, mtime build embedded stat; hash, hashed, gone -> the record"""
        expect = FileRecord(0, FileStat("x", 42, 2**30))
        assert mk_file_record(name="x", size=42, mtime=2**30) == expect

    def test_stat_overrides_scalars(self) -> None:
        """Given stat embedded as is; name, size, mtime passed beside it are ignored"""
        expect = FileRecord(0, FileStat("x", 42, 2**30))
        assert mk_file_record(stat=expect.stat, name="y", size=9, mtime=7) == expect


class TestMkManifest:
    """mk_manifest inits a fresh manifest under tmp_path."""

    def test_fresh_manifest_has_root_meta(self, tmp_path: Path) -> None:
        """A raw sqlite3 query on root / name returns root from meta."""
        factory.mk_manifest(tmp_path, "foobar", "foobarbaz")
        with sql.connect(tmp_path / "foobar") as conn:
            q = """SELECT property, value FROM meta
                WHERE property IN ('comment', 'root') ORDER BY property;"""
            expect = [("comment", "foobarbaz"), ("root", tmp_path.as_posix())]
            assert conn.execute(q).fetchall() == expect

    def test_fresh_manifest_has_no_detail_rows(self, tmp_path: Path) -> None:
        """Factory manifests carry no fs detail rows, on any host."""
        factory.mk_manifest(tmp_path)
        with sql.connect(tmp_path / ".scout.db") as conn:
            q = """SELECT count(*) FROM meta WHERE property IN
                ('fs_type', 'fs_uuid', 'fs_label', 'fs_model', 'hostname');"""
            assert conn.execute(q).fetchone()[0] == 0
