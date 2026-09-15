# src/scout/lib/repo/meta_repo.py
"""MetaRepo: typed getters and setters over the meta table.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.repo.db_connector import DBConnector


class MetaRepo:
    """Owns meta; nothing else reads or writes it by hand."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS meta (
    property TEXT PRIMARY KEY,
    value TEXT
    );"""

    _SELECT = "SELECT value FROM meta WHERE property = ?;"
    _UPSERT = (
        "INSERT INTO meta (property, value) VALUES (?, ?) "
        "ON CONFLICT(property) DO UPDATE SET value = excluded.value;"
    )

    def __init__(self, db: DBConnector) -> None:
        """Bind to the manifest db shares."""
        self.db = db

    def _get(self, key: str) -> str | None:
        """Return the stored value for key, or None when there is no row."""
        row = self.db.conn.execute(self._SELECT, (key,)).fetchone()
        return None if row is None else row[0]

    def _set(self, key: str, value: str) -> None:
        """Upsert key to value."""
        self.db.conn.execute(self._UPSERT, (key, value))

    def _require(self, key: str) -> str:
        """Return the stored value for key or raise Err.NotAManifest."""
        if (value := self._get(key)) is None:
            path = PPP(self.db.path.as_posix())
            msg = f"meta table has no {key} property in {path}"
            raise Err.NotAManifest(msg, path=path)
        return value

    @property
    def root(self) -> PPP:
        """Root the manifest describes; required."""
        return PPP(self._require("root"))

    @root.setter
    def root(self, value: PPP) -> None:
        """Store root as its posix string."""
        self._set("root", value.as_posix())

    @property
    def schema_version(self) -> int:
        """Schema version this manifest was written at; required."""
        return int(self._require("schema_version"))

    @schema_version.setter
    def schema_version(self, value: int) -> None:
        """Store schema_version as decimal text."""
        self._set("schema_version", str(value))

    @property
    def hash_algo(self) -> str:
        """Name of the hash file.hash holds; required."""
        return self._require("hash_algo")

    @hash_algo.setter
    def hash_algo(self, value: str) -> None:
        """Store hash_algo."""
        self._set("hash_algo", value)

    @property
    def comment(self) -> str | None:
        """Human description of the store; optional."""
        return self._get("comment")

    @comment.setter
    def comment(self, value: str) -> None:
        """Store comment."""
        self._set("comment", value)

    @property
    def fs_type(self) -> str | None:
        """Filesystem type as read at init; optional."""
        return self._get("fs_type")

    @fs_type.setter
    def fs_type(self, value: str) -> None:
        """Store fs_type."""
        self._set("fs_type", value)

    @property
    def fs_uuid(self) -> str | None:
        """Filesystem UUID as read at init; optional."""
        return self._get("fs_uuid")

    @fs_uuid.setter
    def fs_uuid(self, value: str) -> None:
        """Store fs_uuid."""
        self._set("fs_uuid", value)

    @property
    def fs_label(self) -> str | None:
        """Filesystem label as read at init; optional."""
        return self._get("fs_label")

    @fs_label.setter
    def fs_label(self, value: str) -> None:
        """Store fs_label."""
        self._set("fs_label", value)

    @property
    def fs_model(self) -> str | None:
        """Device model as read at init; optional."""
        return self._get("fs_model")

    @fs_model.setter
    def fs_model(self, value: str) -> None:
        """Store fs_model."""
        self._set("fs_model", value)

    @property
    def hostname(self) -> str | None:
        """Host that ran init; optional."""
        return self._get("hostname")

    @hostname.setter
    def hostname(self, value: str) -> None:
        """Store hostname."""
        self._set("hostname", value)
