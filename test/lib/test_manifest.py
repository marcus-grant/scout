# test/lib/test_manifest.py
"""Pin Manifest, the composite over one .scout.db file.
Author: Marcus
Created: 2026-09-09
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

from assertion import assert_err_words
import pytest

import scout.lib.error as Err
from scout.lib.manifest import Manifest

TABLES = {"fs_meta", "scan", "dir", "file"}


class TestInit:
    """Manifest.init creates a manifest file with every table and its meta."""

    def test_creates_all_tables(self, tmp_path: Path) -> None:
        """After init, sqlite_master lists fs_meta, scan, dir, and file."""
        Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path)
        with sql.connect(db_path) as conn:
            q = """SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'"""
            assert {r[0] for r in conn.execute(q)} == TABLES

    def test_writes_meta(self, tmp_path: Path) -> None:
        """schema_version, hash_algo, root, and comment are readable raw."""
        Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path, comment="Disk 1")
        with sql.connect(db_path) as conn:
            q = "SELECT property, value FROM fs_meta ORDER BY property"
            assert conn.execute(q).fetchall() == [
                ("comment", "Disk 1"),
                ("hash_algo", "b3c32"),
                ("root", tmp_path.as_posix()),
                ("schema_version", "1"),
            ]

    def test_seeds_root_dir(self, tmp_path: Path) -> None:
        """The dir table holds (0, '.') after init."""
        Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path)
        with sql.connect(db_path) as conn:
            assert conn.execute("SELECT id, path FROM dir").fetchall() == [(0, ".")]

    def test_refuses_existing_path(self, tmp_path: Path) -> None:
        """init on a path that exists raises Err.ManifestExists with the path."""
        Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path)
        with pytest.raises(Err.ManifestExists) as exc:
            Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path)
        assert_err_words(exc, db_path, "exists")


class TestOpen:
    """Manifest.open returns a manifest whose repos share one db."""

    def test_reads_what_init_wrote(self, manifest: Manifest) -> None:
        """open on an init'd file exposes meta.root equal to the init root."""
        result = Manifest.open(manifest.db.path)
        assert result.fs_meta.root == manifest.fs_meta.root

    def test_repos_share_db(self, manifest: Manifest) -> None:
        """meta, scans, dirs, and files hold the same DBConnector."""
        man = Manifest.open(manifest.db.path)
        assert man.fs_meta.db is man.scans.db is man.dirs.db is man.files.db is man.db

    @pytest.mark.parametrize("version", (0, 9999))
    def test_rejects_wrong_schema_version(self, manifest: Manifest, version: int):
        """A manifest whose schema_version differs raises Err.BadSchemaVersion."""
        with sql.connect(db_path := manifest.db.path) as conn:
            q = "UPDATE fs_meta SET value = ? WHERE property = 'schema_version';"
            conn.execute(q, (version,))
        with pytest.raises(Err.BadSchemaVersion) as exc:
            Manifest.open(db_path)
        assert_err_words(exc, manifest.db.path, "schema_version", str(version))

    def test_rejects_non_manifest(self, tmp_path: Path) -> None:
        """A sqlite file without fs_meta raises Err.NotAManifest."""
        sql.connect(bad := tmp_path / "bad.db").execute("CREATE TABLE t (a)")
        with pytest.raises(Err.NotAManifest) as exc:
            Manifest.open(bad)
        assert_err_words(exc, bad, "fs_meta", "table")


class TestTransaction:
    """with manifest: groups repo calls; commit on exit, rollback on error."""

    def test_commits_on_clean_exit(self, manifest: Manifest) -> None:
        """A dir added inside the block is visible to a fresh connection after."""
        with manifest:
            manifest.dirs.add(PPP("a"))
            q = "SELECT path FROM dir WHERE path = 'a'"
            assert manifest.db.conn.execute(q).fetchone() == ("a",)

    def test_rolls_back_on_exception(self, manifest: Manifest) -> None:
        """A dir added before an exception in the block is gone after it."""
        with pytest.raises(RuntimeError):
            with manifest:
                manifest.dirs.add(PPP("a"))
                raise RuntimeError("boom")
        q = "SELECT path FROM dir WHERE path = 'a'"
        assert sql.connect(manifest.db.path).execute(q).fetchone() is None
