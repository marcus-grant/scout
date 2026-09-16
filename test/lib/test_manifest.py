# test/lib/test_manifest.py
"""Pin Manifest, the composite over one .scout.db file.
Author: Marcus
Created: 2026-09-09
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

import pytest
from assertion import assert_err_fields

import scout.lib.error as Err
from scout.lib.manifest import Manifest

TABLES = {"meta", "scan", "dir", "file"}


def _mk_detail(**overrides: str | None) -> dict[str, str | None]:
    """Return a full detail dict with canned values; overrides replace them."""
    return {
        "fs_type": "btrfs",
        "fs_uuid": "1234-5678",
        "fs_label": "mydisk",
        "fs_model": "samsung",
        "hostname": "boblocal",
    } | overrides


class TestInit:
    """Manifest.init creates a manifest file with every table and its meta."""

    def test_creates_all_tables(self, tmp_path: Path) -> None:
        """After init, sqlite_master lists meta, scan, dir, and file."""
        Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path)
        with sql.connect(db_path) as conn:
            q = """SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'"""
            assert {r[0] for r in conn.execute(q)} == TABLES

    def test_writes_meta(self, tmp_path: Path) -> None:
        """schema_version, hash_algo, root, and comment are readable raw."""
        db_path = tmp_path / ".scout.db"
        Manifest.init(db_path, tmp_path, comment="Disk 1", detail={})
        with sql.connect(db_path) as conn:
            q = "SELECT property, value FROM meta ORDER BY property"
            assert conn.execute(q).fetchall() == [
                ("comment", "Disk 1"),
                ("hash_algo", "b3c32"),
                ("root", tmp_path.as_posix()),
                ("schema_version", "1"),
            ]

    def test_writes_detail_rows_from_readers(self, tmp_path: Path) -> None:
        """Entries in detail land as meta rows through init."""
        db_path, detail = tmp_path / ".scout.db", _mk_detail()
        Manifest.init(db_path, tmp_path, detail=detail)
        with sql.connect(db_path) as conn:
            rows = dict(conn.execute("SELECT property, value FROM meta"))
        assert all(rows[k] == v for k, v in detail.items())

    def test_omits_none_entries_in_detail(self, tmp_path: Path) -> None:
        """A None detail entry writes no row at all; others still write."""
        detail = _mk_detail(fs_type=None, fs_uuid=None, fs_label=None, fs_model=None)
        Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path, detail=detail)
        with sql.connect(db_path) as conn:
            rows = dict(conn.execute("SELECT property, value FROM meta"))
        assert rows["hostname"] == "boblocal"
        assert all(k not in rows for k, v in detail.items() if v is None)

    def test_seeds_root_dir(self, tmp_path: Path) -> None:
        """The dir table holds (0, '.') after init."""
        Manifest.init((db_path := tmp_path / ".scout.db"), tmp_path)
        with sql.connect(db_path) as conn:
            assert conn.execute("SELECT id, path FROM dir").fetchall() == [(0, ".")]

    def test_refuses_existing_path(self, tmp_path: Path) -> None:
        """init on a path that exists raises Err.ManifestExists with the path."""
        db_posix = (db_path := tmp_path / ".scout.db").as_posix()
        Manifest.init(db_path, tmp_path)
        with pytest.raises(Err.ManifestExists) as exc:
            Manifest.init(db_path, tmp_path)
        assert_err_fields(exc, db_posix, "exists", path=PPP(db_posix))

    def test_rejects_missing_repo_parent(self, tmp_path: Path) -> None:
        """A repo path whose parent is absent raises Err.RepoParentMissing
        naming the path, before any file is created."""
        parent_posix = (db_path := tmp_path / "not-dir" / ".scout.db").parent.as_posix()
        with pytest.raises(Err.RepoParentMissing) as exc:
            Manifest.init(db_path, tmp_path)
        words = ("parent", "missing")
        assert_err_fields(exc, parent_posix, *words, path=PPP(parent_posix))
        assert not db_path.parent.exists()

    def test_rejects_file_target(self, tmp_path: Path) -> None:
        """A target that is a file raises Err.TargetNotDir naming it,
        before any file is created."""
        (bad := tmp_path / "foobar.bin").write_bytes(b"not-a-dir")
        with pytest.raises(Err.TargetNotDir) as exc:
            Manifest.init(tmp_path / ".scout.db", bad)
        bad_posix, words = bad.as_posix(), ("root", "not", "dir")
        assert_err_fields(exc, bad_posix, *words, path=PPP(bad_posix), role="root")
        assert not (tmp_path / ".scout.db").exists()


class TestOpen:
    """Manifest.open returns a manifest whose repos share one db."""

    def test_reads_what_init_wrote(self, manifest: Manifest) -> None:
        """open on an init'd file exposes meta.root equal to the init root."""
        result = Manifest.open(manifest.db.path)
        assert result.meta.root == manifest.meta.root

    def test_repos_share_db(self, manifest: Manifest) -> None:
        """meta, scans, dirs, and files hold the same DBConnector."""
        man = Manifest.open(manifest.db.path)
        assert man.meta.db is man.scans.db is man.dirs.db is man.files.db is man.db

    @pytest.mark.parametrize("version", (0, 9999))
    def test_rejects_wrong_schema_version(self, manifest: Manifest, version: int):
        """A manifest whose schema_version differs raises Err.BadSchemaVersion."""
        with sql.connect(db_path := manifest.db.path) as conn:
            q = "UPDATE meta SET value = ? WHERE property = 'schema_version';"
            conn.execute(q, (version,))
        with pytest.raises(Err.BadSchemaVersion) as exc:
            Manifest.open(db_path)
        words = ("schema_version", str(version), db_path.as_posix())
        assert_err_fields(exc, *words, path=PPP(db_path.as_posix()))

    def test_rejects_non_manifest(self, tmp_path: Path) -> None:
        """A sqlite file without meta raises Err.NotAManifest."""
        sql.connect(bad := tmp_path / "bad.db").execute("CREATE TABLE t (a)")
        with pytest.raises(Err.NotAManifest) as exc:
            Manifest.open(bad)
        bad_posix = bad.as_posix()
        assert_err_fields(exc, bad_posix, "meta", "table", path=PPP(bad_posix))


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
        with pytest.raises(RuntimeError), manifest:
            manifest.dirs.add(PPP("a"))
            raise RuntimeError("boom")
        q = "SELECT path FROM dir WHERE path = 'a'"
        assert sql.connect(manifest.db.path).execute(q).fetchone() is None
