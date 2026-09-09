# test/lib/repo/test_dir_repo.py
"""Pin DirRepo, the owner of the dir table.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import sqlite3 as sql
from pathlib import PurePosixPath as PPP

import pytest

import scout.lib.error as Err
from scout.lib.manifest import Manifest
from scout.lib.model.dir import Dir
from scout.lib.repo.dir_repo import DirRepo


class TestSchema:
    """DirRepo.SCHEMA creates dir with its root row and can run again."""

    def test_idempotent(self, manifest: Manifest) -> None:
        """Running SCHEMA twice leaves exactly one row, (0, '.', NULL)."""
        repo = manifest.dirs
        with sql.connect(manifest.db.path) as conn:
            conn.executescript(DirRepo.SCHEMA)
            assert conn.execute("SELECT count(*) FROM dir;").fetchone() == (1,)
        assert repo.get(PPP(".")) == Dir(0, PPP("."))


class TestAdd:
    """add upserts a path with its ancestors and returns the leaf Dir."""

    def test_returns_dir_with_id(self, manifest: Manifest) -> None:
        """The Dir returned has the given path, gone None, and an int id."""
        repo, path = manifest.dirs, PPP("a/b")
        assert (dir := repo.add(path)).path == path
        assert dir.gone is None
        assert isinstance(dir, Dir)

    def test_same_path_returns_same_id(self, manifest: Manifest) -> None:
        """Adding a path twice yields one row and the same id."""
        repo, path = manifest.dirs, PPP("a/b")
        a, b = repo.add(path), repo.add(path)
        assert a.id == b.id
        with repo.db.connect() as conn:
            q = "SELECT count(*) FROM dir WHERE path = 'a/b';"
            assert conn.execute(q).fetchone() == (1,)

    def test_creates_ancestors(self, manifest: Manifest) -> None:
        """Adding a/b/c stores a and a/b as live dirs too, in one call."""
        manifest.dirs.add(PPP("a/b/c"))
        q = "SELECT id, path, gone FROM dir WHERE id != 0 ORDER BY id;"
        with sql.connect(manifest.db.path) as conn:
            rows = conn.execute(q).fetchall()
        assert rows == [(1, "a", None), (2, "a/b", None), (3, "a/b/c", None)]

    @pytest.mark.parametrize("bad", [PPP("/a"), PPP("../a"), PPP("a/../../b")])
    def test_rejects_paths_outside_root(self, manifest: Manifest, bad: PPP) -> None:
        """Absolute paths and paths escaping root raise Err.NotUnderRoot."""
        with pytest.raises(Err.NotUnderRoot, match="not under root") as exc:
            manifest.dirs.add(bad)
        assert exc.value.path == bad


class TestGet:
    """get returns the live Dir at a path, or None."""

    def test_returns_added(self, manifest: Manifest) -> None:
        """get returns the Dir add returned, and None for an unknown path."""
        repo, missing = manifest.dirs, PPP("a/b/c")
        result = repo.add(added := PPP("a/b"))
        assert repo.get(added) == result
        assert repo.get(missing) is None

    def test_hides_gone(self, manifest: Manifest) -> None:
        """A dir whose gone is set reads as None."""
        repo = manifest.dirs
        repo.add(PPP("a"))
        with sql.connect(manifest.db.path) as conn:
            conn.execute("UPDATE dir SET gone = 7 WHERE path = 'a';")
        assert repo.get(PPP("a")) is None


class TestDescendants:
    """descendants returns live dirs strictly under a path, by path."""

    def test_strict_prefix_matches(self, manifest: Manifest) -> None:
        """descendants of a excludes a itself and the sibling ab."""
        repo = manifest.dirs
        _, ab, abc, _ = (repo.add(PPP(p)) for p in ("a", "a/b", "a/b/c", "ab"))
        assert repo.descendants(PPP("a")) == [ab, abc]

    def test_respects_byte_boundaries(self, manifest: Manifest) -> None:
        """Edge names around the a/ range are excluded; wildcards and UTF-8 kept."""
        repo = manifest.dirs
        edges = ("a.", "a0", "a\U0001f600", "A/x")
        under = ("a/50%_x", "a/\u00e9", "a/\U0001f600")
        for name in edges:
            repo.add(PPP(name))
        expect = [repo.add(PPP(name)) for name in under]
        assert repo.descendants(PPP("a")) == expect

    def test_of_root_is_everything(self, manifest: Manifest) -> None:
        """descendants of PPP('.') returns every live dir; '' spells root too."""
        repo = manifest.dirs
        expect = [repo.add(PPP(p)) for p in ("a", "b", "b/c")]
        assert repo.descendants(PPP(".")) == expect
        assert repo.descendants(PPP("")) == expect

    def test_hides_gone(self, manifest: Manifest) -> None:
        """A dir whose gone is set is left out of the list."""
        repo = manifest.dirs
        for n in ("a/x", "a/y/z", "b"):
            repo.add(PPP(n))
        with sql.connect(manifest.db.path) as conn:
            conn.execute("UPDATE dir SET gone = 7 WHERE path IN ('a/y', 'a/y/z');")
        assert repo.descendants(PPP("a")) == [repo.get(PPP("a/x"))]
        expect = [repo.get(PPP("a")), repo.get(PPP("a/x")), repo.get(PPP("b"))]
        assert repo.descendants(PPP(".")) == expect


class TestMarkGone:
    """mark_gone sets gone on a path and its subtree, and add revives it."""

    def test_covers_subtree(self, manifest: Manifest) -> None:
        """gone is set on the dir and its descendants, not on siblings."""
        repo, started = manifest.dirs, 42
        parent, child, sibling = PPP("a"), PPP("a/b"), PPP("ab")
        for p in (parent, child, sibling):
            repo.add(p)
        repo.mark_gone(parent, started)
        q = "SELECT path, gone FROM dir WHERE id != 0 ORDER BY path;"
        with sql.connect(manifest.db.path) as conn:
            rows = conn.execute(q).fetchall()
        expect = [(str(parent), started), (str(child), started), (str(sibling), None)]
        assert rows == expect

    def test_revived_by_add(self, manifest: Manifest) -> None:
        """Re-adding a path under a gone dir clears gone on the whole chain."""
        repo = manifest.dirs
        repo.add(PPP("a/b"))
        repo.mark_gone(PPP("a"), 7)
        repo.add(PPP("a/b"))
        q = "SELECT path, gone FROM dir WHERE id != 0 ORDER BY path;"
        with sql.connect(manifest.db.path) as conn:
            rows = conn.execute(q).fetchall()
        assert rows == [("a", None), ("a/b", None)]
