# test/lib/manifest/test_service.py
"""Tests for the services Manifest composes.
Author: Marcus
Created: 2026-09-29
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import PurePosixPath as PPP

from factory import mk_file_record

from scout.lib.manifest import Manifest


class TestGoneSubtree:
    """gone_subtree.mark on a stored subtree with nothing on disk."""

    def test_marks_subtree_dirs_and_files(self, manifest: Manifest) -> None:
        """Returns files before their dir, deepest dirs last; siblings untouched."""
        b, c = manifest.dirs.upsert(PPP("b")), manifest.dirs.upsert(PPP("b/c"))
        manifest.files.upsert(mk_file_record(dir_id=0, name="a"))
        manifest.files.upsert(mk_file_record(dir_id=b.id, name="x"))
        manifest.files.upsert(mk_file_record(dir_id=c.id, name="y"))

        gone = manifest.gone_subtree.mark(PPP("b"), started=7)

        assert set(gone) == {PPP("b"), PPP("b/c"), PPP("b/x"), PPP("b/c/y")}
        with sql.connect(manifest.db.path) as conn:
            dirs = dict(conn.execute("SELECT path, gone FROM dir;").fetchall())
            files = dict(conn.execute("SELECT name, gone FROM file;").fetchall())
        assert (dirs["b"], dirs["b/c"], dirs["."]) == (7, 7, None)
        assert (files["x"], files["y"], files["a"]) == (7, 7, None)

    def test_unstored_path_marks_nothing(self, manifest: Manifest) -> None:
        """A path with no dir row returns empty and writes nothing."""
        b = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_file_record(dir_id=b.id, name="x"))

        gone = manifest.gone_subtree.mark(PPP("c"), started=7)

        assert gone == []
        with sql.connect(manifest.db.path) as c:
            gone_d = c.execute("SELECT gone FROM dir WHERE path='b';").fetchone()
            gone_f = c.execute("SELECT gone FROM file WHERE name='x';").fetchone()
        assert (gone_d, gone_f) == ((None,), (None,))
