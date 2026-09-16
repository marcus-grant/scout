# test/cli/subcmd/test_run_init.py
"""Pin the init subcommand: arguments in, manifest and events out.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from pathlib import Path

import pytest
from click.testing import CliRunner

import scout.cli.event as events
from scout.cli import main
from scout.cli.subcmd.init import run_init
from scout.lib.fs import meta as fs_meta


class TestRunInit:
    """run_init creates the manifest and emits one InitDone."""

    def test_creates_manifest_and_emits_done(self, tmp_path: Path) -> None:
        """The db file exists after; the event carries repo, root, missing."""
        emitted: list[events.Event] = []
        run_init(tmp_path, None, None, emitted.append, detail={})
        assert (tmp_path / ".scout.db").is_file()
        assert emitted == [
            events.InitDone(tmp_path / ".scout.db", tmp_path.resolve(), ())
        ]

    def test_defaults_repo_beside_target(self, tmp_path: Path) -> None:
        """No repo argument means target/.scout.db."""
        emitted: list[events.Event] = []
        run_init(tmp_path, None, None, emitted.append, detail={})
        assert isinstance(init_event := emitted[0], events.InitDone)
        assert init_event.repo == tmp_path / ".scout.db"

    def test_missing_names_unread_details(self, tmp_path: Path) -> None:
        """Keys preset and falsy land in missing; absent keys are unmentioned."""
        emitted: list[events.Event] = []
        detail = {"fs_type": "ext4", "hostname": "boblocal", "fs_uuid": None}
        run_init(tmp_path, None, None, emitted.append, detail=detail)
        assert isinstance(init_event := emitted[0], events.InitDone)
        assert init_event.missing == ("fs_uuid",)

    def test_relative_target_resolves(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A relative target lands absolute in the manifest and the event."""
        monkeypatch.chdir(tmp_path)
        emitted: list[events.Event] = []
        run_init(Path("."), None, None, emitted.append, detail={})
        assert isinstance(init_event := emitted[0], events.InitDone)
        assert init_event.root == tmp_path.resolve()
        assert init_event.repo == tmp_path.resolve() / ".scout.db"

    def test_readers_receive_resolved_target(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """detail None resolution hands read_all the resolved target."""
        seen: list[Path] = []
        monkeypatch.setattr(fs_meta, "read_all", lambda t: (seen.append(t), {})[1])
        monkeypatch.chdir(tmp_path)
        run_init(Path("."), None, None, lambda _: None)
        assert seen == [tmp_path.resolve()]

    def test_detail_none_reads_system(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """detail None resolves through fs.meta.read_all, not an empty dict."""
        canned: dict[str, str | None] = {"fs_type": "btrfs", "fs_uuid": None}
        monkeypatch.setattr(fs_meta, "read_all", lambda *_: canned)
        emitted: list[events.Event] = []
        run_init(tmp_path, None, None, emitted.append)
        assert isinstance(done_event := emitted[0], events.InitDone)
        assert done_event.missing == ("fs_uuid",)


class TestInitCommand:
    """The init command wires arguments through run_init and echoes."""

    def test_invokes_and_echoes(self, tmp_path: Path) -> None:
        """Exit 0; the Initialized line reaches output; the db exists."""
        result = CliRunner().invoke(main, ["init", str(tmp_path)])
        assert result.exit_code == 0
        assert "Initialized" in result.output
        assert (tmp_path / ".scout.db").is_file()
