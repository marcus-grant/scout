# test/lib/repo/test_db_connector.py
"""Pin DBConnector: opens and validates one manifest file, owns its connection.
Author: Marcus
Created: 2026-09-10
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path

import pytest
from assertion import assert_err_words

import scout.lib.error as Err
from scout.lib.manifest import Manifest
from scout.lib.repo.db_connector import DBConnector


class TestInit:
    """DBConnector(path) accepts a manifest and refuses everything else."""

    def test_opens_manifest(self, manifest: Manifest) -> None:
        """A fresh manifest's path yields a connector whose conn answers SQL."""
        db = DBConnector(manifest.db.path)
        assert isinstance(db.path, Path)
        assert db.conn.execute("SELECT 1").fetchone() == (1,)

    def test_rejects_missing_path(self, tmp_path: Path) -> None:
        """A path with no file raises Err.NoManifest with the path."""
        with pytest.raises(Err.NoManifest) as exc:
            DBConnector(missing := tmp_path / "missing.db")
        assert_err_words(exc, missing, "manifest", "file")  # type: ignore

    def test_rejects_non_sqlite_file(self, tmp_path: Path) -> None:
        """A file that is not SQLite raises Err.NotAManifest with the path."""
        (bad := tmp_path / "bad.db").write_bytes(b"foobar")
        with pytest.raises(Err.NotAManifest) as exc:
            DBConnector(bad)
        assert_err_words(exc, bad, "sqlite", "file")

    def test_rejects_sqlite_without_meta(self, tmp_path: Path) -> None:
        """A SQLite file lacking meta raises Err.NotAManifest with the path."""
        bad = tmp_path / "bad.db"
        sql.connect(bad).execute("CREATE TABLE foo (bar);").close()
        with pytest.raises(Err.NotAManifest) as exc:
            DBConnector(bad)
        assert_err_words(exc, bad, "meta", "table")


class TestTransaction:
    """begin, commit, and rollback group statements on the one connection."""

    _SELECT = "SELECT started FROM scan;"
    _INSERT = "INSERT INTO scan (started) VALUES (?);"
    _EXPECT = (1,)

    def _arrange(self, manifest: Manifest, value: tuple[int] = _EXPECT) -> DBConnector:
        """Begin a transaction on manifest's connector & insert one scan row."""
        (db := manifest.db).begin()
        db.conn.execute(self._INSERT, value)
        return db

    def test_commit_keeps_writes(self, manifest: Manifest) -> None:
        """A write between begin and commit is visible to a second connection."""
        (db := self._arrange(manifest)).commit()
        assert sql.connect(db.path).execute(self._SELECT).fetchone() == self._EXPECT

    def test_rollback_drops_writes(self, manifest: Manifest) -> None:
        """A write between begin and rollback is gone afterwards."""
        (db := self._arrange(manifest)).rollback()
        assert sql.connect(db.path).execute(self._SELECT).fetchone() is None

    def test_autocommit_outside_transaction(self, manifest: Manifest) -> None:
        """A write with no begin is durable on its own."""
        (db := manifest.db).conn.execute(self._INSERT, self._EXPECT)
        assert sql.connect(db.path).execute(self._SELECT).fetchone() == self._EXPECT

    def test_nested_begin_raises(self, manifest: Manifest) -> None:
        """begin while a transaction is open raises Err.NestedTransaction."""
        db = self._arrange(manifest)
        with pytest.raises(Err.NestedTransaction) as exc:
            db.begin()
        assert_err_words(exc, db.path, "transaction", "already")
