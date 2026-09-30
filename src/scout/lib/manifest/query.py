# src/scout/lib/manifest/query.py
"""Queries: reads that join across tables, composed by Manifest; never write.
Author: Marcus
Created: 2026-09-30
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass
from pathlib import PurePosixPath as PPP

from scout.lib.manifest.repo.db_connector import DBConnector
from scout.lib.manifest.repo.dir_repo import DirRepo


@dataclass(frozen=True)
class ClaimedCounts:
    """How many dirs and files the manifest claims under one path."""

    dirs: int
    files: int


class Claimed:
    """What the manifest still claims exists: rows with gone IS NULL."""

    _COUNT_FILES = """SELECT COUNT(*) FROM file JOIN dir ON file.dir_id = dir.id
        WHERE file.gone IS NULL AND dir.gone IS NULL AND ({})"""

    def __init__(self, db: DBConnector, dirs: DirRepo) -> None:
        """Bind the connector for the join and the dir repo for its predicates."""
        self.db = db
        self.dirs = dirs

    def counts(self, path: PPP) -> ClaimedCounts:
        """Count claimed dirs strictly under path, and claimed files in path
        and in those dirs."""
        count_d = self.dirs.count(DirRepo.where_under(str(path)))
        where, params = DirRepo.where_at_or_under(str(path))
        query = self._COUNT_FILES.format(where)
        count_f = self.db.conn.execute(query, params).fetchone()[0]
        return ClaimedCounts(dirs=count_d, files=count_f)
