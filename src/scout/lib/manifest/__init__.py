# src/scout/lib/manifest/__init__.py
"""Manifest: the composite over one .scout.db, sharing one DBConnector.
Author: Marcus
Created: 2026-09-09
Revised: [2026-09-29]
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from collections.abc import Mapping
from pathlib import Path
from pathlib import PurePosixPath as PPP
from typing import Self

import scout.lib.error as Err
from scout.lib.fs import meta as fs_meta
from scout.lib.manifest.query import Claimed
from scout.lib.manifest.repo.db_connector import DBConnector
from scout.lib.manifest.repo.dir_repo import DirRepo
from scout.lib.manifest.repo.file_repo import FileRepo
from scout.lib.manifest.repo.meta_repo import MetaRepo
from scout.lib.manifest.repo.scan_repo import ScanRepo
from scout.lib.manifest.service import GoneSubtree


class Manifest:
    """One .scout.db: meta, scans, dirs, and files over a shared connector."""

    DEFAULT_NAME = ".scout.db"
    SCHEMA_VERSION = 1
    HASH_ALGO = "b3c32"
    REPOS = (MetaRepo, ScanRepo, DirRepo, FileRepo)  # In order of which must init first

    def __init__(self, db: DBConnector) -> None:
        """Build the four repos over db & services over db"""
        self.db = db
        self._write_count = 0
        self._commit_every: int | None = None
        self.meta = MetaRepo(self.db)
        self.scans = ScanRepo(self.db)
        self.dirs = DirRepo(self.db)
        self.files = FileRepo(self.db)
        self.gone_subtree = GoneSubtree(self.dirs, self.files)
        self.claimed = Claimed(self.db, self.dirs)

    def __enter__(self) -> Self:
        """Begin one transaction across every repo;
        commit or roll back on exit."""
        self.db.begin()
        return self

    def __exit__(self, *exc: object) -> None:
        """Commit on a clean exit, rollback when an exception is passing through."""
        if exc[0] is None:
            self.db.commit()
        else:
            self.db.rollback()
        self._commit_every = None
        self._write_count = 0

    def commit_every(self, write_count: int) -> Self:
        """Set the threshold for the next with block:
        commit and begin again after every `_write_count` calls to wrote();
        return self for the block."""
        self._write_count = 0
        self._commit_every = write_count
        return self

    def wrote(self) -> None:
        """Count one row written; at the threshold, commit & begin again."""
        if self._commit_every is None:
            return
        self._write_count += 1
        if self._write_count >= self._commit_every:
            self._write_count = 0
            self.db.commit()
            self.db.begin()

    @staticmethod
    def _create_tables(path: Path) -> None:
        """Create path and run every SCHEMA in REPOS; seeds root for DBConnector."""
        with sql.connect(path) as conn:
            for r in Manifest.REPOS:
                conn.executescript(r.SCHEMA)

    @staticmethod
    def _validate_init(path: Path, root: Path) -> None:
        """Raise Err.RepoParentMissing, Err.ManifestExists, or Err.TargetNotDir.
        if class init method path and root args are invalid"""
        if not path.parent.is_dir():
            msg = f"parent directory missing: {path.parent}"
            raise Err.RepoParentMissing(msg, path=PPP(path.parent.as_posix()))
        if path.exists():
            msg = f"file already exists: {path}"
            raise Err.ManifestExists(msg, path=PPP(path.as_posix()))
        if not root.is_dir():
            msg = f'target "{(role := "root")}" is not a directory: {root}'
            raise Err.TargetNotDir(msg, path=PPP(root.as_posix()), role=role)

    def _write_meta(
        self, root: Path, comment: str | None, detail: Mapping[str, str | None] = {}
    ) -> None:
        """Write schema_version, hash_algo, root, and comment to meta."""
        self.meta.schema_version = self.SCHEMA_VERSION
        self.meta.hash_algo = self.HASH_ALGO
        self.meta.root = PPP(root.as_posix())
        if comment is not None:
            self.meta.comment = comment
        self.meta.write_fs_detail(detail)

    @classmethod
    def init(
        cls,
        path: Path,
        root: Path,
        comment: str | None = None,
        detail: Mapping[str, str | None] | None = None,
    ) -> "Manifest":
        """Create the file at path, run every SCHEMA, write meta, and open it."""
        root = root.resolve()  # Root needs resolution first
        cls._validate_init(path, root)
        Manifest._create_tables(path)
        man = cls(DBConnector(path))
        detail = detail if detail is not None else fs_meta.read_all(root)
        man._write_meta(root, comment, detail)
        return man

    @classmethod
    def open(cls, path: Path) -> "Manifest":
        """Open an existing manifest, refusing a wrong schema_version."""
        path = path.resolve()
        if path.is_dir():
            path = path / cls.DEFAULT_NAME
        man = cls(DBConnector(path))
        if (version := man.meta.schema_version) != cls.SCHEMA_VERSION:
            msg = """schema_version {} at {}, this build reads {}"""
            params = (version, path, cls.SCHEMA_VERSION)
            raise Err.BadSchemaVersion(msg.format(*params), path=PPP(path.as_posix()))
        return man
