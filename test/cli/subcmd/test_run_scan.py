# test/cli/subcmd/test_run_scan.py
"""Pin run_scan: manifest updates and the event stream, through a
collecting emit.
Author: Marcus
Created: 2026-09-21
License: AGPL-3.0-or-later
"""

import sys
from pathlib import Path
from pathlib import PurePosixPath as PPP
from unittest.mock import Mock

import factory
import pytest
from click.testing import CliRunner, Result

import scout.cli.event as events
import scout.lib.error as Err
from scout.cli import main
from scout.cli.subcmd.scan import _bytes_progress, _translate_event, run_scan
from scout.lib.event import AccessLost, Event, FileScanned, ReadFailed, RecordGone
from scout.lib.manifest import Manifest
from scout.lib.models import RecordChange
from scout.lib.scan import ScanOptions
from scout.lib.scan.hashing import HashingPolicy
from scout.lib.scan.tally import Summary

# The tree fixture (test/conftest.py) is factory.mk_tree(tmp_path), which is
#   files: a.txt "alpha", b/b1.txt "bravo", b/b2.txt "alpha", b/c/empty.txt ""
#   dirs:  d (empty); implied by the files: ., b, b/c
# The manifest fixture is factory.mk_manifest(tmp_path): .scout.db in that root.
Tree = factory.Tree

# Alias for the HashingPolicy enum to shorten references
OFF = HashingPolicy.OFF
NEEDED = HashingPolicy.NEEDED
ALL = HashingPolicy.ALL
# Alias for type of _translate_events arg for events it translates
_EVT = FileScanned | RecordGone | ReadFailed | AccessLost | Summary


class TestTranslateEvent:
    """_translate_event on one constructed lib event per case."""

    def act(self, e: _EVT) -> events.CliEvent:
        """Test ACT phase aliasing/shortening the function being tested."""
        return _translate_event(e)

    def test_file_scanned_is_scan_file(self) -> None:
        """FileScanned becomes ScanFile with its path, record and change."""
        record = factory.mk_file_record()
        fields = (PPP("a.txt"), record, RecordChange.ADDED)
        assert self.act(FileScanned(*fields)) == events.ScanFile(*fields)

    def test_record_gone_is_scan_gone(self) -> None:
        """RecordGone becomes ScanGone with its path."""
        assert self.act(RecordGone(p := PPP("a.txt"))) == events.ScanGone(p)

    def test_read_failed_is_scan_error(self) -> None:
        """ReadFailed becomes ScanError with its path and error."""
        err = Err.Unreadable("x", p := PPP("a.txt"), errno=70)
        assert self.act(ReadFailed(p, err)) == events.ScanError(p, err)

    def test_access_lost_is_scan_access_lost(self) -> None:
        """AccessLost becomes ScanAccessLost with its path."""
        assert self.act(AccessLost(p := PPP("a.txt"))) == events.ScanAccessLost(p)

    def test_summary_is_scan_finished(self) -> None:
        """Summary becomes ScanFinished with every count and both timestamps."""
        args = (1, 2, 3, 4, 5, 6, 7)
        assert self.act(Summary(*args)) == events.ScanFinished(*args)

    def test_unknown_event_raises(self) -> None:
        """An Event with no case raises TypeError."""
        with pytest.raises(TypeError, match=r"scout\.lib\.event\.Event"):
            self.act(Event())  # type: ignore


class TestRunScan:
    """run_scan on the default tree under the manifest fixture."""

    def test_emits_one_event_per_record(self, tree: Tree, manifest: Manifest) -> None:
        """Four ScanFile in DFS order then one ScanFinished with added 4;
        no ScanGone, no ScanError."""
        seen: list[events.CliEvent] = []

        run_scan(manifest.db.path, ScanOptions(), lambda _: seen.append)

        expect = [events.ScanFile] * 4 + [events.ScanFinished]
        assert [type(evt) for evt in seen] == expect

    def test_detail_reaches_meta(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """What fs_meta.read_all returns for the root is written to meta."""
        _ = tree  # Shutup LSPs, need side-effect of tree without referencing it
        patch_str = "scout.cli.subcmd.scan.fs_meta.read_all"
        monkeypatch.setattr(patch_str, lambda _: {"hostname": "h"})

        run_scan(manifest.db.path, ScanOptions(), lambda _: lambda _: None)
        assert Manifest.open(manifest.db.path).meta.hostname == "h"

    def test_options_reach_the_scan(self, tree: Tree, manifest: Manifest) -> None:
        """The opt given to run_scan is the one scan runs with: OFF leaves hashes null."""
        seen: list[events.CliEvent] = []
        run_scan(manifest.db.path, ScanOptions(OFF), lambda _: seen.append)

        file_scans = [e for e in seen if isinstance(e, events.ScanFile)]
        assert len(file_scans) == 4
        assert all(e.file.hash is None for e in file_scans)

    def test_relative_path_excludes_own_manifest(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With cwd at the root and path ".", no ScanFile names .scout.db."""
        monkeypatch.chdir(tree.root)
        seen: list[events.CliEvent] = []

        run_scan(Path("."), ScanOptions(), lambda _: seen.append)
        paths = [e.path for e in seen if isinstance(e, events.ScanFile)]
        assert PPP(".scout.db") not in paths


class TestScanCommand:
    """The scan command through CliRunner on an initialized default tree."""

    def _invoke(self, tree: Tree, argv: list[str]) -> Result:
        """Runs the init command on the passed tmp_path Tree.
        Then runs and returns CliRunner.invoke(main, argv)"""
        result = CliRunner().invoke(main, ["init", str(tree.root)])
        assert result.exit_code == 0, result.output
        return CliRunner().invoke(main, ["scan", str(tree.root), *argv])

    def _options(self, monkeypatch: pytest.MonkeyPatch, *argv: str) -> ScanOptions:
        """Invoke scan with argv and run_scan patched; return the options it got."""
        run = Mock()
        monkeypatch.setattr("scout.cli.subcmd.scan.run_scan", run)
        result = CliRunner().invoke(main, ["scan", *argv])
        assert result.exit_code == 0, result.output
        return run.call_args.args[1]

    def test_default_prints_summary_only(self, tree: Tree) -> None:
        """init then scan: stdout is exactly the one ScanFinished line."""
        result = self._invoke(tree, [])

        assert result.exit_code == 0, result.output
        assert result.stdout == "added 4 updated 0 matched 0 gone 0 errors 0\n"

    def test_verbose_prints_file_lines(self, tree: Tree) -> None:
        """scan -v: stdout has the four added lines before the summary."""
        result = self._invoke(tree, ["-v"])

        lines = result.stdout.splitlines()
        assert result.exit_code == 0, result.output
        assert lines[:4] == [f"added {p}" for p in sorted(tree.files)]
        assert lines[4].startswith(f"added {len(tree.files)}")

    def test_rehash_with_no_hash_is_usage_error(self) -> None:
        """scan --rehash --no-hash exits 2 naming the conflict."""
        argv = ["scan", "--rehash", "--no-hash"]
        result = CliRunner().invoke(main, argv)
        assert result.exit_code == 2, f"expected exit 2, got {result}"
        assert "--rehash cannot be used with --no-hash" in result.stderr

    def test_progress_writes_file_counts(self, tree: Tree) -> None:
        """scan --progress: output contains '1 files' through '4 files'."""
        result = self._invoke(tree, ["--progress"])

        assert result.exit_code == 0, result.output
        counts = [s for s in result.stderr.splitlines() if s.endswith("files")]
        assert counts == [f"{n} files" for n in (1, 2, 3, 4)]

    def test_no_hash_is_policy_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """--no-hash passes ScanOptions with hash_policy OFF."""
        assert self._options(monkeypatch, "--no-hash").hash_policy == OFF

    def test_rehash_is_policy_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """--rehash passes ScanOptions with hash_policy ALL."""
        assert self._options(monkeypatch, "--rehash").hash_policy == ALL


class TestBytesProgress:
    """_bytes_progress writes a bytes line, or a spinner on a terminal."""

    def test_writes_bytes_line_when_not_a_terminal(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """_bytes_progress(5) writes '5 bytes' and a newline to stderr."""
        _bytes_progress(5)

        assert capsys.readouterr().err == "5 bytes\n"

    def test_spins_on_a_terminal(
        self, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With stderr a terminal, two calls write a carriage return and one
        spinner character each, no newline, and the characters differ."""
        monkeypatch.setattr(sys.stderr, "isatty", lambda: True)

        _bytes_progress(5)
        _bytes_progress(6)

        err = capsys.readouterr().err
        assert err[0] == "\r" and err[2] == "\r" and len(err) == 4
        assert err[1] != err[3]
