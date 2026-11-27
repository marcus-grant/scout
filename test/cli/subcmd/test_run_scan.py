# test/cli/subcmd/test_run_scan.py
"""Pin run_scan: manifest updates and the event stream, through a
collecting emit.
Author: Marcus
Created: 2026-09-21
License: AGPL-3.0-or-later
"""

from pathlib import Path
from pathlib import PurePosixPath as PPP
from types import MethodType
from unittest.mock import Mock

import factory
import pytest
from click.testing import CliRunner

import scout.lib.error as Err
from scout.cli import event as cli_event
from scout.cli import main
from scout.cli.emit import make_emit
from scout.cli.progress import OutputSink, ProgressLines
from scout.cli.subcmd.scan import _translate_event, run_scan
from scout.lib import event as lib_event
from scout.lib.manifest import Manifest
from scout.lib.models import RecordChange
from scout.lib.scan import ScanOptions
from scout.lib.scan.hashing import HashingPolicy
from scout.lib.scan.tally import ScanTally, Summary

# The tree fixture (test/conftest.py) is factory.mk_tree(tmp_path), which is
#   files: a.txt "alpha", b/b1.txt "bravo", b/b2.txt "alpha", b/c/empty.txt ""
#   dirs:  d (empty); implied by the files: ., b, b/c
# The manifest fixture is factory.mk_manifest(tmp_path): .scout.db in that root.
Tree = factory.Tree

# Alias for the HashingPolicy enum to shorten references
OFF = HashingPolicy.OFF
NEEDED = HashingPolicy.NEEDED
ALL = HashingPolicy.ALL

CliEvent = cli_event.CliEvent


class TestTranslateEvent:
    """_translate_event on one constructed lib event per case."""

    def act(self, e: lib_event.ScanEvent | Summary) -> CliEvent:
        """Test ACT phase aliasing/shortening the function being tested."""
        return _translate_event(e)

    def test_file_scanned_is_scan_file(self) -> None:
        """FileScanned becomes ScanFile with its path, record and change."""
        record = factory.mk_file_record()
        fields = (PPP("a.txt"), record, RecordChange.ADDED)
        assert self.act(lib_event.FileScanned(*fields)) == cli_event.ScanFile(*fields)

    def test_record_gone_is_scan_gone(self) -> None:
        """RecordGone becomes ScanGone with its path."""
        assert self.act(lib_event.RecordGone(p := PPP("a.py"))) == cli_event.ScanGone(p)

    def test_read_failed_is_scan_error(self) -> None:
        """ReadFailed becomes ScanError with its path and error."""
        err = Err.Unreadable("x", p := PPP("a.txt"), errno=70)
        assert self.act(lib_event.ReadFailed(p, err)) == cli_event.ScanError(p, err)

    def test_access_lost_is_scan_access_lost(self) -> None:
        """AccessLost becomes ScanAccessLost with its path."""
        evt = lib_event.AccessLost(p := PPP("a.txt"))
        assert self.act(evt) == cli_event.ScanAccessLost(p)

    def test_record_excluded_is_scan_excluded(self) -> None:
        """RecordExcluded becomes ScanExcluded with its path."""
        evt = lib_event.RecordExcluded(p := PPP("a.txt"))
        assert self.act(evt) == cli_event.ScanExcluded(p)

    def test_summary_is_scan_finished(self) -> None:
        """Summary becomes ScanFinished with every count and both timestamps."""
        args = (1, 2, 3, 4, 5, 6, 7)
        assert self.act(Summary(*args)) == cli_event.ScanFinished(*args)

    def test_unknown_event_raises(self) -> None:
        """An Event with no case raises TypeError."""
        with pytest.raises(TypeError, match=r"scout\.lib\.event\.Event"):
            self.act(lib_event.Event())  # type: ignore


class TestRunScan:
    """run_scan on the default tree under the manifest fixture."""

    def test_emits_one_event_per_record(self, tree: Tree, manifest: Manifest) -> None:
        """Four ScanFile in DFS order then one ScanFinished with added 4;
        no ScanGone, no ScanError."""
        seen: list[CliEvent] = []
        run_scan(manifest.db.path, ScanOptions(), ScanTally(), seen.append)

        expect = [cli_event.ScanFile] * 4 + [cli_event.ScanFinished]
        assert [type(evt) for evt in seen] == expect

    def test_counts_into_the_given_tally(self, tree: Tree, manifest: Manifest) -> None:
        """The tally passed to run_scan is the one the scan counts into."""
        run_scan(manifest.db.path, ScanOptions(), tally := ScanTally(), lambda _: None)
        assert tally.files_seen() == 4

    def test_detail_reaches_meta(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """What fs_meta.read_all returns for the root is written to meta."""
        _ = tree  # Shutup LSPs, need side-effect of tree without referencing it
        patch_str = "scout.cli.subcmd.scan.fs_meta.read_all"
        monkeypatch.setattr(patch_str, lambda _: {"hostname": "h"})

        run_scan(manifest.db.path, ScanOptions(), ScanTally(), lambda _: None)
        assert Manifest.open(manifest.db.path).meta.hostname == "h"

    def test_options_reach_the_scan(self, tree: Tree, manifest: Manifest) -> None:
        """The opt given to run_scan is the one scan runs with: OFF leaves hashes null."""
        seen: list[CliEvent] = []
        run_scan(manifest.db.path, ScanOptions(OFF), ScanTally(), seen.append)

        file_scans = [e for e in seen if isinstance(e, cli_event.ScanFile)]
        assert len(file_scans) == 4
        assert all(e.file.hash is None for e in file_scans)


class TestScanCommand:
    """The scan command through CliRunner on an initialized default tree."""

    def _options(self, monkeypatch: pytest.MonkeyPatch, *argv: str) -> ScanOptions:
        """Invoke scan with argv and run_scan patched; return the options it got."""
        run = Mock()
        monkeypatch.setattr("scout.cli.subcmd.scan.run_scan", run)
        result = CliRunner().invoke(main, ["scan", *argv])
        assert result.exit_code == 0, result.output
        return run.call_args.args[1]

    def _verbose_arg(self, monkeypatch: pytest.MonkeyPatch, *argv: str) -> object:
        """Invoke scan with argv, run_scan patched and make_emit spied on;
        return the verbose argument make_emit got."""
        monkeypatch.setattr("scout.cli.subcmd.scan.run_scan", Mock())
        spy = Mock(wraps=make_emit)
        monkeypatch.setattr("scout.cli.subcmd.scan.make_emit", spy)
        result = CliRunner().invoke(main, ["scan", *argv])
        assert result.exit_code == 0, result.output
        return spy.call_args.args[0]

    def test_verbose_reaches_make_emit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """-v passes True to make_emit as verbose."""
        assert self._verbose_arg(monkeypatch, "-v") is True

    def test_default_is_not_verbose(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without -v, make_emit is given False as verbose."""
        assert self._verbose_arg(monkeypatch) is False

    def test_progress_picks_progress_lines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--progress hands the scan the hashed callback of a ProgressLines."""
        opt = self._options(monkeypatch, "--progress")
        assert isinstance(opt.on_progress, MethodType)
        assert isinstance(opt.on_progress.__self__, ProgressLines)

    def test_short_progress_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """-p is the short form of --progress."""
        assert isinstance(p := self._options(monkeypatch, "-p").on_progress, MethodType)
        assert isinstance(p.__self__, ProgressLines)

    def test_default_picks_plain_sink(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without --progress the hashed callback belongs to a plain OutputSink."""
        opt = self._options(monkeypatch)
        assert isinstance(opt.on_progress, MethodType)
        assert type(opt.on_progress.__self__) is OutputSink

    def test_rehash_with_no_hash_is_usage_error(self) -> None:
        """scan --rehash --no-hash exits 2 naming the conflict."""
        argv = ["scan", "--rehash", "--no-hash"]
        result = CliRunner().invoke(main, argv)
        assert result.exit_code == 2, f"expected exit 2, got {result}"
        assert "--rehash cannot be used with --no-hash" in result.stderr

    def test_no_hash_is_policy_off(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """--no-hash passes ScanOptions with hash_policy OFF."""
        assert self._options(monkeypatch, "--no-hash").hash_policy == OFF

    def test_rehash_is_policy_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """--rehash passes ScanOptions with hash_policy ALL."""
        assert self._options(monkeypatch, "--rehash").hash_policy == ALL

    def test_excludes_reach_options(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Each --exclude PATH lands in ScanOptions.exclude as a PPP."""
        argv = ("--exclude", "a", "--exclude", "b/c")
        expect = frozenset({PPP("a"), PPP("b/c")})
        assert self._options(monkeypatch, *argv).exclude == expect

    def test_leading_slash_anchors_at_root(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--exclude /video is the root-relative video."""
        argv, expect = ("--exclude", "/video"), frozenset({PPP("video")})
        assert self._options(monkeypatch, *argv).exclude == expect

    def test_dotdot_exclude_is_usage_error(self) -> None:
        """--exclude ../x exits 2 naming the path."""
        result = CliRunner().invoke(main, ["scan", "--exclude", "../x"])
        assert result.exit_code == 2
        assert "../x" in result.stderr

    def test_repo_reaches_run_scan(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """-r PATH passes PATH to run_scan as the manifest to open."""
        monkeypatch.setattr("scout.cli.subcmd.scan.run_scan", run := Mock())
        result = CliRunner().invoke(main, ["scan", "-r", "x"])

        assert result.exit_code == 0, result.output
        assert run.call_args.args[0] == Path("x")

    def test_repo_defaults_to_cwd(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Without -r, run_scan is given Path(".") as the manifest to open."""
        monkeypatch.setattr("scout.cli.subcmd.scan.run_scan", run := Mock())
        result = CliRunner().invoke(main, ["scan"])

        assert result.exit_code == 0, result.output
        assert run.call_args.args[0] == Path(".")

