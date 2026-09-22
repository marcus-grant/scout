# test/e2e/test_scan_cli.py
"""Pin scout scan end to end: CLI in, file, dir and scan rows out, through
sqlite3 only.
Author: Marcus
Created: 2026-09-17
Revised: [2026-09-22]
License: AGPL-3.0-or-later
"""

import shutil
import sqlite3 as sql
import time
from pathlib import Path
from pathlib import PurePosixPath as PPP
from typing import NamedTuple

import factory
import pytest
from click.testing import CliRunner

from scout.cli import main

# The tree fixture builds factory's default tree under tmp_path:
# a.txt, b/b1.txt, b/b2.txt, b/c/empty.txt, and the empty dir d.
# scout init puts .scout.db at tree.root.
Tree = factory.Tree

FILE_ROWS = (
    "SELECT d.path, f.name, f.hash, f.hashed, f.gone, f.size "
    "FROM file AS f JOIN dir AS d ON f.dir_id = d.id;"
)
DIR_ROWS = "SELECT path, gone FROM dir;"
SCAN_ROWS = "SELECT started, finished, files_seen FROM scan ORDER BY started;"


class FileRow(NamedTuple):
    """One file row as the e2e suite reads it."""

    hash: str | None
    hashed: int | None
    gone: int | None
    size: int


class ScanRow(NamedTuple):
    """One scan row as the e2e suite reads it."""

    started: int
    finished: int
    files_seen: int


class Run(NamedTuple):
    """One CLI invocation: exit code, stdout and stderr as line tuples,
    and the ns clock before and after it."""

    exit_code: int
    out: tuple[str, ...]
    err: tuple[str, ...]
    before: int
    after: int


def _files(db: Path) -> dict[PPP, FileRow]:
    """Map each stored file path to its FileRow."""
    with sql.connect(db) as conn:
        rows = conn.execute(FILE_ROWS).fetchall()
    return {PPP(p) / n: FileRow(h, hd, g, s) for p, n, h, hd, g, s in rows}


def _dirs(db: Path) -> dict[str, int | None]:
    """Map each stored dir path to its gone value."""
    with sql.connect(db) as conn:
        rows = conn.execute(DIR_ROWS).fetchall()
    return {p: g for p, g in rows}


def _scans(db: Path) -> list[ScanRow]:
    """Every scan row as a ScanRow, oldest first."""
    with sql.connect(db) as conn:
        rows = conn.execute(SCAN_ROWS).fetchall()
    return [ScanRow(s, f, fs) for s, f, fs in rows]


def _init(root: Path) -> None:
    """Run scout init on root and assert it exited 0."""
    result = CliRunner().invoke(main, ["init", str(root)])
    assert result.exit_code == 0, f"scout init failed: {result.output}"


def _scan(root: Path, *flags: str) -> Run:
    """Run scout scan on root with flags, assert it exited 0, and return
    the Run; out and err are the streams split on newline with the
    trailing empty element dropped."""
    before = time.time_ns()
    result = CliRunner().invoke(main, ["scan", str(root), *flags])
    after = time.time_ns()
    assert result.exit_code == 0, f"scout scan failed: {result.output}"
    return Run(
        result.exit_code,
        tuple(result.stdout.splitlines()),
        tuple(result.stderr.splitlines()),
        before,
        after,
    )


@pytest.mark.e2e
class TestScan:
    """scout init then scout scan records a factory tree and its changes."""

    def test_first_scan_records_tree(self, tree: Tree) -> None:
        """Scan with -v: stdout is one added line per factory file then the
        summary line, stderr is empty; one file row per factory file with
        hash and hashed set, one dir row per directory including the empty
        one, no row for .scout.db, one finished scan row whose files_seen
        is the file count, and every timestamp inside the run window."""
        db = tree.root / ".scout.db"
        _init(tree.root)

        run = _scan(tree.root, "-v")

        assert set(run.out[:-1]) == {f"added {p}" for p in tree.files}
        assert run.out[-1] == "added 4 updated 0 matched 0 gone 0 errors 0"
        assert run.err == ()

        files, dirs, scans = _files(db), _dirs(db), _scans(db)
        expected_dirs = {str(p) for f in tree.files for p in f.parents}
        expected_dirs |= {str(d) for d in tree.dirs}
        assert set(files) == set(tree.files)
        assert set(dirs) == expected_dirs
        assert all(g is None for g in dirs.values())
        assert len(scans) == 1
        scan = scans[0]
        assert run.before < scan.started <= scan.finished < run.after
        assert scan.files_seen == len(tree.files)
        for row in files.values():
            assert row.hash is not None
            assert row.hashed == scan.started
            assert row.gone is None

    def test_no_hash_rescan_clears_changed_hash(self, tree: Tree) -> None:
        """Lengthen one file and scan with --no-hash: stdout is the summary
        line alone with updated 1 and matched 3; that row has null hash and
        null hashed and the new size; every other row keeps its hash."""
        db = tree.root / ".scout.db"
        _init(tree.root)
        _scan(tree.root)
        first = _files(db)
        changed = PPP("b/b1.txt")
        content = tree.files[changed] + b"-more"
        factory.mk_file(tree.root, str(changed), content)

        run = _scan(tree.root, "--no-hash")

        assert run.out == ("added 0 updated 1 matched 3 gone 0 errors 0",)
        assert run.err == ()
        second = _files(db)
        assert second[changed].hash is None
        assert second[changed].hashed is None
        assert second[changed].size == len(content)
        for path in set(tree.files) - {changed}:
            assert second[path].hash == first[path].hash

    def test_rescan_records_changes(self, tree: Tree) -> None:
        """Delete the b subtree, lengthen a.txt, add new.txt, scan again:
        stdout is one gone line per swept file then per swept dir, files
        before dirs, then the summary line with added 1 updated 1 matched 0
        gone 5; the three swept file rows and both swept dir rows have gone
        set to the second started, a.txt has a new hash and hashed equal to
        the second started, new.txt has hashed equal to the second started,
        and d is untouched."""
        db = tree.root / ".scout.db"
        _init(tree.root)
        _scan(tree.root)
        first = _files(db)
        swept_files = {p for p in tree.files if p.parts[0] == "b"}
        swept_dirs = {"b", "b/c"}
        changed, added = PPP("a.txt"), PPP("new.txt")
        shutil.rmtree(tree.root / "b")
        factory.mk_file(tree.root, str(changed), tree.files[changed] + b"-more")
        factory.mk_file(tree.root, str(added), b"new")

        run = _scan(tree.root)

        assert len(run.out) == 6
        assert set(run.out[:3]) == {f"gone {p}" for p in swept_files}
        assert set(run.out[3:5]) == {f"gone {d}" for d in swept_dirs}
        assert run.out[5] == "added 1 updated 1 matched 0 gone 5 errors 0"
        assert run.err == ()
        second, dirs = _files(db), _dirs(db)
        started = _scans(db)[1].started
        for path in swept_files:
            assert second[path].gone == started
        for path in swept_dirs:
            assert dirs[path] == started
        assert dirs["d"] is None
        assert second[changed].hash != first[changed].hash
        assert second[changed].hashed == started
        assert second[added].hashed == started
        assert second[added].gone is None

    def test_unchanged_rescan_then_rehash(self, tree: Tree) -> None:
        """Scan twice with nothing touched: the second stdout is the summary
        line alone with matched 4, stderr is empty, every file row is
        identical to the first. Scan a third time with --rehash --progress:
        stdout is the summary line alone with updated 4, stderr is not
        empty, every hash is unchanged, every hashed is the third started."""
        db = tree.root / ".scout.db"
        _init(tree.root)
        _scan(tree.root)
        first = _files(db)
        matched = "added 0 updated 0 matched 4 gone 0 errors 0"

        second_run = _scan(tree.root)

        assert second_run.out == (matched,)
        assert second_run.err == ()
        assert _files(db) == first

        third_run = _scan(tree.root, "--rehash", "--progress")

        assert third_run.out == ("added 0 updated 4 matched 0 gone 0 errors 0",)
        assert third_run.err != ()
        third = _files(db)
        started = _scans(db)[2].started
        for path in tree.files:
            assert third[path].hash == first[path].hash
            assert third[path].hashed == started

    def test_scan_without_manifest_fails(self, tree: Tree) -> None:
        """Scan a tree never initialized: nonzero exit, empty stdout, one
        line on stderr."""
        result = CliRunner().invoke(main, ["scan", str(tree.root)])

        assert result.exit_code != 0
        assert result.stdout == ""
        assert len(result.stderr.splitlines()) == 1
