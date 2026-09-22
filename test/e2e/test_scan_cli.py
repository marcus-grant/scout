# test/e2e/test_scan.py
"""Pin scout scan end to end: CLI in, file, dir and scan rows out, through
sqlite3 only.
Author: Marcus
Created: 2026-09-17
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
import time
from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest
from click.testing import CliRunner

from scout.cli import main

FILE_ROWS = (
    "SELECT d.path, f.name, f.hash, f.hashed, f.gone, f.size "
    "FROM file AS f JOIN dir AS d ON f.dir_id = d.id;"
)
DIR_ROWS = "SELECT path, gone FROM dir;"
SCAN_ROWS = "SELECT started, finished, files_seen FROM scan ORDER BY started;"


def _files(db: Path) -> dict[PPP, tuple]:
    """Map each stored file path to (hash, hashed, gone, size)."""
    with sql.connect(db) as conn:
        rows = conn.execute(FILE_ROWS).fetchall()
    return {PPP(d) / n: (h, hd, g, s) for d, n, h, hd, g, s in rows}


def _dirs(db: Path) -> dict[str, int | None]:
    """Map each stored dir path to its gone value."""
    with sql.connect(db) as conn:
        return dict(conn.execute(DIR_ROWS).fetchall())


def _scans(db: Path) -> list[tuple]:
    """Every scan row as (started, finished, files_seen), oldest first."""
    with sql.connect(db) as conn:
        return conn.execute(SCAN_ROWS).fetchall()


def _scan(root: Path, *flags: str) -> tuple[int, int, int]:
    """Run scout scan on root; return (exit_code, ns before, ns after)."""
    before = time.time_ns()
    result = CliRunner().invoke(main, ["scan", str(root), *flags])
    after = time.time_ns()
    assert result.exit_code == 0, f"scan failed: {result.output}"
    return result.exit_code, before, after


@pytest.mark.e2e
class TestScan:
    """scout init then scout scan records a factory tree and its changes."""

    def test_first_scan_records_tree(self, tmp_path: Path) -> None:
        """One file row per factory file with hash and hashed set, one dir
        row per directory including the empty one, no row for .scout.db,
        one finished scan row whose files_seen is the file count, and every
        timestamp inside the run window."""
        tree = factory.mk_tree(tmp_path)
        db = tmp_path / ".scout.db"
        CliRunner().invoke(main, ["init", str(tmp_path)])

        _, before, after = _scan(tmp_path)

        files, dirs, scans = _files(db), _dirs(db), _scans(db)
        expected_dirs = {str(p) for f in tree.files for p in f.parents}
        expected_dirs |= {str(d) for d in tree.dirs}
        assert set(files) == set(tree.files)
        assert set(dirs) == expected_dirs
        assert all(g is None for g in dirs.values())
        assert len(scans) == 1
        started, finished, seen = scans[0]
        assert before < started <= finished < after
        assert seen == len(tree.files)
        for hash_, hashed, gone, _ in files.values():
            assert hash_ is not None
            assert hashed == started
            assert gone is None

    def test_rescan_records_changes(self, tmp_path: Path) -> None:
        """Delete one file, lengthen one, add one, scan again: the deleted
        row has gone set to the second started, the lengthened row has a
        new hash and hashed equal to the second started, the added row has
        hashed equal to the second started, every other row is untouched."""
        tree = factory.mk_tree(tmp_path)
        db = tmp_path / ".scout.db"
        CliRunner().invoke(main, ["init", str(tmp_path)])
        _scan(tmp_path)
        first = _files(db)
        deleted, changed, added = PPP("a.txt"), PPP("b/b1.txt"), PPP("b/new.txt")
        (tmp_path / deleted).unlink()
        factory.mk_file(tmp_path, str(changed), tree.files[changed] + b"-more")
        factory.mk_file(tmp_path, str(added), b"new")

        _scan(tmp_path)

        second = _files(db)
        started = _scans(db)[1][0]
        assert second[deleted][2] == started
        assert second[changed][0] != first[changed][0]
        assert second[changed][1] == started
        assert second[added][1] == started
        for path in set(tree.files) - {deleted, changed}:
            assert second[path] == first[path]

    def test_no_hash_rescan_clears_changed_hash(self, tmp_path: Path) -> None:
        """Lengthen one file and scan with --no-hash: that row has null hash
        and null hashed and the new size; every other row keeps its hash."""
        tree = factory.mk_tree(tmp_path)
        db = tmp_path / ".scout.db"
        CliRunner().invoke(main, ["init", str(tmp_path)])
        _scan(tmp_path)
        first = _files(db)
        changed = PPP("b/b1.txt")
        content = tree.files[changed] + b"-more"
        factory.mk_file(tmp_path, str(changed), content)

        _scan(tmp_path, "--no-hash")

        second = _files(db)
        assert second[changed][0] is None
        assert second[changed][1] is None
        assert second[changed][3] == len(content)
        for path in set(tree.files) - {changed}:
            assert second[path][0] == first[path][0]
