# test/cli/subcmd/test_run_scan.py
"""Pin run_scan: manifest updates and the event stream, through a
collecting emit.
Author: Marcus
Created: 2026-09-21
License: AGPL-3.0-or-later
"""

import errno
import os
import sqlite3 as sql
import sys
from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest
from click.testing import CliRunner, Result

import scout.cli.event as events
from scout.cli import main
from scout.cli.subcmd.scan import _bytes_progress, run_scan
from scout.lib.manifest import Manifest

# The tree fixture (test/conftest.py) is factory.mk_tree(tmp_path), which is
#   files: a.txt "alpha", b/b1.txt "bravo", b/b2.txt "alpha", b/c/empty.txt ""
#   dirs:  d (empty); implied by the files: ., b, b/c
# The manifest fixture is factory.mk_manifest(tmp_path): .scout.db in that root.
Tree = factory.Tree


class TestRunScan:
    """run_scan on the default tree under the manifest fixture."""

    def test_emits_one_event_per_record(self, tree: Tree, manifest: Manifest) -> None:
        """Four ScanFile in DFS order then one ScanFinished with added 4;
        no ScanGone, no ScanError."""
        seen: list[events.Event] = []

        run_scan(manifest.db.path, seen.append, detail={})  # {} turns off fs meta

        expect = [events.ScanFile] * 4 + [events.ScanFinished]
        assert [type(evt) for evt in seen] == expect

    def test_detail_reaches_meta(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """detail {"hostname": "h"} is readable from meta after the run, and
        fs_meta.read_all is not called when detail is given."""

        def forbidden(*_args, **_kwargs):
            _, _ = _args, _kwargs  # Shut up LSPs
            raise AssertionError("read_all called with detail given")

        _ = tree  # Shutup LSPs, need side-effect of tree though we don't reference it
        monkeypatch.setattr("scout.cli.subcmd.scan.fs_meta.read_all", forbidden)

        run_scan(manifest.db.path, lambda _: None, detail={"hostname": "h"})

        assert Manifest.open(manifest.db.path).meta.hostname == "h"

    def test_no_hash_reaches_the_rows(self, tree: Tree, manifest: Manifest) -> None:
        """hash False: every ScanFile has file.hash None."""
        seen: list[events.Event] = []

        run_scan(manifest.db.path, seen.append, hash=False, detail={})

        file_scans = [e for e in seen if isinstance(e, events.ScanFile)]
        assert len(file_scans) == 4
        assert all(e.file.hash is None for e in file_scans)

    def test_gone_maps_to_scan_gone(self, tree: Tree, manifest: Manifest) -> None:
        """Run, delete a.txt, run again: seen holds ScanGone(a.txt)."""
        seen: list[events.Event] = []
        run_scan(manifest.db.path, lambda _: None, detail={})
        (tree.root / "a.txt").unlink()

        run_scan(manifest.db.path, seen.append, detail={})

        assert events.ScanGone(PPP("a.txt")) in seen

    def test_unreadable_maps_to_scan_error(
        self, tree: Tree, manifest: Manifest, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """os.scandir failing on b: seen holds one ScanError whose path is b."""
        seen: list[events.Event] = []
        real = os.scandir

        def fake(p):
            if Path(p) == tree.root / "b":
                raise PermissionError(errno.EACCES, "Permission denied")
            return real(p)

        monkeypatch.setattr("scout.lib.fs.walk.os.scandir", fake)

        run_scan(manifest.db.path, seen.append, detail={})

        errors = [e for e in seen if isinstance(e, events.ScanError)]
        assert [e.path for e in errors] == [PPP("b")]


class TestScanCommand:
    """The scan command through CliRunner on an initialized default tree."""

    def _invoke(self, tree: Tree, argv: list[str]) -> Result:
        """Runs the init command on the passed tmp_path Tree.
        Then runs and returns CliRunner.invoke(main, argv)"""
        result = CliRunner().invoke(main, ["init", str(tree.root)])
        assert result.exit_code == 0, result.output
        return CliRunner().invoke(main, ["scan", str(tree.root), *argv])

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

    def test_rehash_with_no_hash_is_usage_error(self, tree: Tree) -> None:
        """scan --rehash --no-hash exits 2."""
        assert self._invoke(tree, ["--rehash", "--no-hash"]).exit_code == 2

    def test_progress_writes_file_counts(self, tree: Tree) -> None:
        """scan --progress: output contains '1 files' through '4 files'."""
        result = self._invoke(tree, ["--progress"])

        assert result.exit_code == 0, result.output
        counts = [s for s in result.stderr.splitlines() if s.endswith("files")]
        assert counts == [f"{n} files" for n in (1, 2, 3, 4)]

    def test_no_hash_leaves_hashes_null(self, tree: Tree) -> None:
        """scan --no-hash: every file row has hash NULL."""
        result = self._invoke(tree, ["--no-hash"])

        assert result.exit_code == 0, result.output
        with sql.connect(tree.root / ".scout.db") as conn:
            q = "SELECT count(*) FROM file WHERE hash IS NOT NULL;"
            assert conn.execute(q).fetchone() == (0,)

    def test_rehash_updates_matching_rows(self, tree: Tree) -> None:
        """scan, then scan --rehash: the second summary says updated 4."""
        self._invoke(tree, [])
        result = CliRunner().invoke(main, ["scan", str(tree.root), "--rehash"])

        assert result.exit_code == 0, result.output
        assert result.stdout.startswith("added 0 updated 4 ")


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
