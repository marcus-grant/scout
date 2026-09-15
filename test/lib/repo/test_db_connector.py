# test/lib/repo/test_db_connector.py
"""Pin DBConnector: opens and validates one manifest file, owns its connection.
Author: Marcus
Created: 2026-09-10
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

import pytest
from assertion import assert_err_fields

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
        missing_ppp = PPP(missing := tmp_path / "missing.db")
        with pytest.raises(Err.NoManifest) as exc:
            DBConnector(missing)
        assert_err_fields(exc, "manifest", "file", missing.as_posix(), path=missing_ppp)

    def test_rejects_non_sqlite_file(self, tmp_path: Path) -> None:
        """A file that is not SQLite raises Err.NotAManifest with the path."""
        (bad := tmp_path / "bad.db").write_bytes(b"foobar")
        bad_posix = bad.as_posix()
        with pytest.raises(Err.NotAManifest) as exc:
            DBConnector(bad)
        assert_err_fields(exc, bad_posix, "sqlite", "file", path=PPP(bad_posix))

    def test_rejects_sqlite_without_meta(self, tmp_path: Path) -> None:
        """A SQLite file lacking meta raises Err.NotAManifest with the path."""
        bad_posix = (bad := tmp_path / "bad.db").as_posix()
        sql.connect(bad).execute("CREATE TABLE foo (bar);").close()
        with pytest.raises(Err.NotAManifest) as exc:
            DBConnector(bad)
        assert_err_fields(exc, bad_posix, "meta", "table", path=PPP(bad_posix))


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
        db_path = (db := self._arrange(manifest)).path.as_posix()
        with pytest.raises(Err.NestedTransaction) as exc:
            db.begin()
        assert_err_fields(exc, db_path, "transaction", "already", path=PPP(db_path))
