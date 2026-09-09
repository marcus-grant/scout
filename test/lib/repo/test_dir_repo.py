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
from scout.lib.model.dir import Dir
from scout.lib.repo.db_connector import DBConnector
from scout.lib.repo.dir_repo import DirRepo
from scout.lib.repo.scan_repo import ScanRepo


def mk_dir_repo(db: DBConnector) -> DirRepo:
    """Create the scan and dir tables on db and return a DirRepo over it."""
    with sql.connect(db.path) as conn:
        conn.executescript(ScanRepo.SCHEMA + DirRepo.SCHEMA)
    return DirRepo(db)


class TestSchema:
    """DirRepo.SCHEMA creates dir with its root row and can run again."""

    def test_idempotent(self, db: DBConnector) -> None:
        """Running SCHEMA twice leaves exactly one row, (0, '.', NULL)."""
        repo = mk_dir_repo(db)
        with sql.connect(db.path) as conn:
            conn.executescript(DirRepo.SCHEMA)
            assert conn.execute("SELECT count(*) FROM dir;").fetchone() == (1,)
        assert repo.get(PPP(".")) == Dir(0, PPP("."))


class TestAdd:
    """add upserts a path with its ancestors and returns the leaf Dir."""

    def test_returns_dir_with_id(self, db: DBConnector) -> None:
        """The Dir returned has the given path, gone None, and an int id."""
        repo, path = mk_dir_repo(db), PPP("a/b")
        assert (dir := repo.add(path)).path == path
        assert dir.gone is None
        assert isinstance(dir, Dir)

    def test_same_path_returns_same_id(self, db: DBConnector) -> None:
        """Adding a path twice yields one row and the same id."""
        repo, path = mk_dir_repo(db), PPP("a/b")
        a, b = repo.add(path), repo.add(path)
        assert a.id == b.id
        with repo.db.connect() as conn:
            q = "SELECT count(*) FROM dir WHERE path = 'a/b';"
            assert conn.execute(q).fetchone() == (1,)

    def test_creates_ancestors(self, db: DBConnector) -> None:
        """Adding a/b/c stores a and a/b as live dirs too, in one call."""
        mk_dir_repo(db).add(PPP("a/b/c"))
        q = "SELECT id, path, gone FROM dir WHERE id != 0 ORDER BY id;"
        with sql.connect(db.path) as conn:
            rows = conn.execute(q).fetchall()
        assert rows == [(1, "a", None), (2, "a/b", None), (3, "a/b/c", None)]

    @pytest.mark.parametrize("bad", [PPP("/a"), PPP("../a"), PPP("a/../../b")])
    def test_rejects_paths_outside_root(self, db: DBConnector, bad: PPP) -> None:
        """Absolute paths and paths escaping root raise Err.NotUnderRoot."""
        with pytest.raises(Err.NotUnderRoot, match="not under root") as exc:
            mk_dir_repo(db).add(bad)
        assert exc.value.path == bad


class TestGet:
    """get returns the live Dir at a path, or None."""

    def test_returns_added(self, db: DBConnector) -> None:
        """get returns the Dir add returned, and None for an unknown path."""
        repo, missing = mk_dir_repo(db), PPP("a/b/c")
        result = repo.add(added := PPP("a/b"))
        assert repo.get(added) == result
        assert repo.get(missing) is None

    def test_hides_gone(self, db: DBConnector) -> None:
        """A dir whose gone is set reads as None."""
        repo = mk_dir_repo(db)
        repo.add(PPP("a"))
        with sql.connect(db.path) as conn:
            conn.execute("UPDATE dir SET gone = 7 WHERE path = 'a';")
        assert repo.get(PPP("a")) is None


class TestDescendants:
    """descendants returns live dirs strictly under a path, by path."""

    def test_strict_prefix_matches(self, db: DBConnector) -> None:
        """descendants of a excludes a itself and the sibling ab."""
        repo = mk_dir_repo(db)
        _, ab, abc, _ = (repo.add(PPP(p)) for p in ("a", "a/b", "a/b/c", "ab"))
        assert repo.descendants(PPP("a")) == [ab, abc]

    def test_respects_byte_boundaries(self, db: DBConnector) -> None:
        """Edge names around the a/ range are excluded; wildcards and UTF-8 kept."""
        repo = mk_dir_repo(db)
        edges = ("a.", "a0", "a\U0001f600", "A/x")
        under = ("a/50%_x", "a/\u00e9", "a/\U0001f600")
        for name in edges:
            repo.add(PPP(name))
        expect = [repo.add(PPP(name)) for name in under]
        assert repo.descendants(PPP("a")) == expect

    def test_of_root_is_everything(self, db: DBConnector) -> None:
        """descendants of PPP('.') returns every live dir; '' spells root too."""
        repo = mk_dir_repo(db)
        expect = [repo.add(PPP(p)) for p in ("a", "b", "b/c")]
        assert repo.descendants(PPP(".")) == expect
        assert repo.descendants(PPP("")) == expect

    def test_hides_gone(self, db: DBConnector) -> None:
        """A dir whose gone is set is left out of the list."""
        repo = mk_dir_repo(db)
        for n in ("a/x", "a/y/z", "b"):
            repo.add(PPP(n))
        with db.connect() as conn:
            conn.execute("UPDATE dir SET gone = 7 WHERE path IN ('a/y', 'a/y/z');")
        assert repo.descendants(PPP("a")) == [repo.get(PPP("a/x"))]
        expect = [repo.get(PPP("a")), repo.get(PPP("a/x")), repo.get(PPP("b"))]
        assert repo.descendants(PPP(".")) == expect


class TestMarkGone:
    """mark_gone sets gone on a path and its subtree, and add revives it."""

    def test_covers_subtree(self, db: DBConnector) -> None:
        """gone is set on the dir and its descendants, not on siblings."""
        repo, started = mk_dir_repo(db), 42
        parent, child, sibling = PPP("a"), PPP("a/b"), PPP("ab")
        for p in (parent, child, sibling):
            repo.add(p)
        repo.mark_gone(parent, started)
        q = "SELECT path, gone FROM dir WHERE id != 0 ORDER BY path;"
        with sql.connect(db.path) as conn:
            rows = conn.execute(q).fetchall()
        expect = [(str(parent), started), (str(child), started), (str(sibling), None)]
        assert rows == expect

    def test_revived_by_add(self, db: DBConnector) -> None:
        """Re-adding a path under a gone dir clears gone on the whole chain."""
        repo = mk_dir_repo(db)
        repo.add(PPP("a/b"))
        repo.mark_gone(PPP("a"), 7)
        repo.add(PPP("a/b"))
        q = "SELECT path, gone FROM dir WHERE id != 0 ORDER BY path;"
        with sql.connect(db.path) as conn:
            rows = conn.execute(q).fetchall()
        assert rows == [("a", None), ("a/b", None)]
