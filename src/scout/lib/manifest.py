# src/scout/lib/manifest.py
"""Manifest: the composite over one .scout.db, sharing one DBConnector.
Author: Marcus
Created: 2026-09-09
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.repo.db_connector import DBConnector
from scout.lib.repo.dir_repo import DirRepo
from scout.lib.repo.file_repo import FileRepo
from scout.lib.repo.meta_repo import MetaRepo
from scout.lib.repo.scan_repo import ScanRepo


class Manifest:
    """One .scout.db: meta, scans, dirs, and files over a shared connector."""

    SCHEMA_VERSION = 1
    HASH_ALGO = "b3c32"
    REPOS = (MetaRepo, ScanRepo, DirRepo, FileRepo)  # In order of which must init first

    def __init__(self, db: DBConnector) -> None:
        """Build the four repos over db."""
        self.db = db
        self.fs_meta = MetaRepo(db)
        self.scans = ScanRepo(db)
        self.dirs = DirRepo(db)
        self.files = FileRepo(db)

    @staticmethod
    def _create_tables(path: Path) -> None:
        """Create path and run every SCHEMA in REPOS; seeds root for DBConnector."""
        with sql.connect(path) as conn:
            for r in Manifest.REPOS:
                conn.executescript(r.SCHEMA)

    def _write_meta(self, root: Path, comment: str | None) -> None:
        """Write schema_version, hash_algo, root, and comment to fs_meta."""
        self.fs_meta.schema_version = self.SCHEMA_VERSION
        self.fs_meta.hash_algo = self.HASH_ALGO
        self.fs_meta.root = PPP(root.as_posix())
        if comment is not None:
            self.fs_meta.comment = comment

    @classmethod
    def init(cls, path: Path, root: Path, comment: str | None = None) -> "Manifest":
        """Create the file at path, run every SCHEMA, write meta, and open it."""
        if path.exists():
            msg = f"file already exists: {path}"
            raise Err.ManifestExists(msg, path=PPP(path.as_posix()))
        Manifest._create_tables(path)
        man = cls(DBConnector(path))
        man._write_meta(root, comment)
        return man

    @classmethod
    def open(cls, path: Path) -> "Manifest":
        """Open an existing manifest, refusing a wrong schema_version."""
        man = cls(DBConnector(path))
        if (version := man.fs_meta.schema_version) != cls.SCHEMA_VERSION:
            msg = f"schema_version {version}, this build reads {cls.SCHEMA_VERSION}"
            raise Err.BadSchemaVersion(msg, path=PPP(path.as_posix()))
        return man
