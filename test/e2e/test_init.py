# test/e2e/test_init.py
"""Pin scout init end to end: CLI in, meta rows out, through sqlite3 only.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import Path

import factory
import pytest
from click.testing import CliRunner

from scout.cli import main


@pytest.mark.e2e
class TestInit:
    """scout init creates a manifest whose meta describes the target."""

    def _query_props_to_dict(self, db_path: Path) -> dict:
        """Queries sqlite DB on db_path for {property: value} of
        meta table's (property, value) columns."""
        with sql.connect(db_path) as conn:
            q = "SELECT property, value FROM meta"
            return {p: v for p, v in conn.execute(q).fetchall()}

    def test_defaults_write_meta(self, tmp_path: Path) -> None:
        """init on a factory tree creates target/.scout.db;
        meta holds schema_version 1, hash_algo b3c32, resolved root,
        the comment, and the five detail properties present or absent-null."""
        # Arrange mock file tree and command run
        tree, comment = factory.mk_tree(tmp_path), "test comment: f00bar"

        # Act on the command
        argv = ["init", str(tree.root), "--comment", comment]
        result = CliRunner().invoke(main, argv)

        # Assert, starting by pulling out 'props' from raw table
        props = self._query_props_to_dict(db_path := tmp_path / ".scout.db")
        assert result.exit_code == 0
        assert db_path.is_file()
        assert props["root"] == tmp_path.resolve().as_posix()
        assert props["schema_version"] == "1"
        assert props["hash_algo"] == "b3c32"
        assert props["comment"] == comment

    def test_stubbed_readers_land_in_meta(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With fs_detail readers monkeypatched to known values, those
        exact values appear as meta rows after init."""
        # Arrange the faked FS and FS detail / host mocks
        canned = {
            "fs_type": "ext4",
            "fs_uuid": "0f00-ba44",
            "fs_label": "testdisk",
            "fs_model": "TestVendor SSD",
            "hostname": "testhost",
        }
        for name, value in canned.items():
            mod_str = f"scout.lib.fs.meta.{name}"
            monkeypatch.setattr(mod_str, lambda *_, value=value: value)
        tree, db_path = factory.mk_tree(tmp_path), tmp_path / ".scout.db"

        # Act on the command
        result = CliRunner().invoke(main, ["init", str(tree.root)])

        # Assert, starting by pulling out 'props' from raw table
        props = self._query_props_to_dict(db_path)
        print(result.output, result.exception)
        assert result.exit_code == 0
        assert db_path.is_file()
        for k, v in canned.items():
            msg = f"property keyed '{k}' doesn't match arranged mock"
            assert props[k] == v, msg
