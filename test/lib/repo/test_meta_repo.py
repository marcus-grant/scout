# test/lib/repo/test_meta_repo.py
"""Pin MetaRepo, the typed owner of fs_meta.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest

import scout.lib.error as Err
from scout.lib.manifest import Manifest

OPTIONAL = ["comment", "fs_type", "fs_uuid", "fs_label", "fs_model", "hostname"]
REQUIRED = [("root", PPP("/mnt/x")), ("schema_version", 42), ("hash_algo", "b3c32")]


class TestMetaRepo:
    """MetaRepo reads and writes fs_meta through typed properties."""

    def test_root_reads_what_init_wrote(self, tmp_path: Path) -> None:
        """root returns the PPP the fresh manifest was created with."""
        assert factory.mk_manifest(tmp_path).fs_meta.root == PPP(tmp_path.as_posix())

    @pytest.mark.parametrize(("key", "value"), REQUIRED)
    def test_required_round_trip(
        self, manifest: Manifest, key: str, value: PPP | int | str
    ) -> None:
        """A required key set through its property reads back equal."""
        setattr(repo := manifest.fs_meta, key, value)
        assert getattr(repo, key) == value

    @pytest.mark.parametrize("key", OPTIONAL)
    def test_optional_absent_is_none(self, manifest: Manifest, key: str) -> None:
        """An optional key never set reads as None."""
        assert getattr(manifest.fs_meta, key) is None

    @pytest.mark.parametrize("key", OPTIONAL)
    def test_optional_round_trip(self, manifest: Manifest, key: str) -> None:
        """An optional key set as str reads back unchanged."""
        setattr(repo := manifest.fs_meta, key, expect := "foobar")
        assert getattr(repo, key) == expect

    def test_required_absent_raises(self, manifest: Manifest) -> None:
        """A required key with no row raises Err.NotAManifest."""
        with sql.connect(manifest.db.path) as conn:
            conn.execute("DELETE FROM fs_meta")
        with pytest.raises(Err.NotAManifest):
            _ = manifest.fs_meta.root

    def test_set_overwrites(self, manifest: Manifest) -> None:
        """Setting a key twice leaves one row holding the last value."""
        (repo := manifest.fs_meta).comment = "test"
        repo.comment = (expect := "foobar")
        assert repo.comment == expect
        with sql.connect(manifest.db.path) as conn:
            q = "SELECT count(*) FROM fs_meta WHERE property = 'comment'"
            assert conn.execute(q).fetchone() == (1,)
