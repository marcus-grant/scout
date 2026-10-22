# test/lib/fs/test_walk.py
"""Pin lib.fs.walk: DFS listings of a factory tree, one per directory.
Author: Marcus
Created: 2026-09-17
License: AGPL-3.0-or-later
"""

import errno
import os
from contextlib import nullcontext
from pathlib import Path
from pathlib import PurePosixPath as PPP
from types import SimpleNamespace
from unittest.mock import Mock

import factory
import pytest

import scout.lib.error as Err
from scout.lib.fs.walk import (
    FileStat,
    _read_dir,
    _read_entry,
    walk,
)

# tree: Tree is a fixture that is the default case of mk_tree
# Its shape from tmp_path as root is:
# ./a.txt, ./b/b1.txt, ./b/b2.txt, ./b/c/empty.txt, ./d/(empty dir)
Tree = factory.Tree

# Expected DFS order PurePosixPaths (PPP) of default tree fixture directories
_DIR_PATHS_DFS = [PPP(n) for n in (".", "b", "b/c", "d")]


def _fail_stat(monkeypatch: pytest.MonkeyPatch, root: Path, rel: str, err: int) -> None:
    """Patch os.scandir as walk.py sees it,
    so the entry at root-relative rel raises OSError(err) on stat;
    every other path and entry is real."""
    real = os.scandir
    entry = PPP(rel)

    def _stat(**_):
        raise OSError(err, os.strerror(err))

    def fake(path):
        if Path(path) != root / entry.parent:
            return real(path)
        with real(path) as it:
            entries = [
                SimpleNamespace(
                    name=e.name, is_dir=e.is_dir, is_file=e.is_file, stat=_stat
                )
                if e.name == entry.name
                else e
                for e in it
            ]
        return nullcontext(entries)

    monkeypatch.setattr("scout.lib.fs.walk.os.scandir", fake)


def _fail_scandir(
    monkeypatch: pytest.MonkeyPatch, root: Path, rel: str, err: int
) -> None:
    """Patch os.scandir as walk.py sees it so listing the directory at
    root-relative rel raises OSError(err); every other path is real."""
    real = os.scandir

    def fake(path):
        if Path(path) == root / rel:
            raise OSError(err, os.strerror(err))
        return real(path)

    monkeypatch.setattr("scout.lib.fs.walk.os.scandir", fake)


class TestReadEntry:
    """_read_entry on mocked os.DirEntry values, no disk."""

    def _entry(self, name: str, kind: str = "file", err: int | None = None) -> Mock:
        """A mocked DirEntry of kind "dir", "file" or "link"; stat fails with err if set."""
        (m := Mock(spec=os.DirEntry)).name = name
        m.is_dir.return_value, m.is_file.return_value = kind == "dir", kind == "file"
        m.stat.return_value = Mock(st_size=5, st_mtime_ns=9)
        m.stat.side_effect = None if err is None else OSError(err, os.strerror(err))
        return m

    def test_dir_entry_is_its_name(self) -> None:
        """A directory entry reads as its name."""
        assert _read_entry(self._entry("c", "dir"), PPP("b/c")) == "c"

    def test_file_entry_is_its_stat(self) -> None:
        """A file entry reads as FileStat(name, size, mtime_ns)."""
        ent = self._entry(n := "a.txt", "file")
        assert _read_entry(ent, PPP(n)) == FileStat(n, 5, 9)

    def test_symlink_entry_is_dropped(self) -> None:
        """An entry that is neither dir nor file reads as None."""
        assert _read_entry(self._entry("link", "link"), PPP("link")) is None

    def test_failed_stat_is_unreadable(self) -> None:
        """A stat failing with EIO reads as Unreadable at the entry's path."""
        read = _read_entry(self._entry("b1.txt", err=errno.EIO), PPP("b"))
        assert isinstance(read, Err.Unreadable)
        assert (read.path, read.errno) == (PPP("b/b1.txt"), errno.EIO)

    def test_vanished_entry_is_dropped(self) -> None:
        """A stat failing with ENOENT reads as None."""
        assert _read_entry(self._entry("b1.txt", err=errno.ENOENT), PPP("b")) is None


class TestReadDir:
    """_read_dir reads one directory into one WalkedDir."""

    def test_reads_entries_in_name_order(self, tree: Tree) -> None:
        """Read b gives files' stats & subdir in name order; . gives subdirs (b, d)."""
        b1_path, b2_path = tree.root / "b/b1.txt", tree.root / "b/b2.txt"
        b1_stat = FileStat("b1.txt", 5, b1_path.stat().st_mtime_ns)
        b2_stat = FileStat("b2.txt", 5, b2_path.stat().st_mtime_ns)

        b_walked = _read_dir(tree.root, PPP("b"))
        assert b_walked.files == (b1_stat, b2_stat)
        assert b_walked.subdirs == ("c",)
        assert _read_dir(tree.root, PPP(".")).subdirs == ("b", "d")

    def test_excluded_entries_are_named_not_read(self, tree: Tree) -> None:
        """Excluding .scout.db and b/c: both named in excluded, neither is read."""
        factory.mk_file(tree.root, ".scout.db", b"manifest")
        exclude = frozenset({PPP(".scout.db"), PPP("b/c")})

        walked_root = _read_dir(tree.root, PPP("."), exclude=exclude)
        walked_b = _read_dir(tree.root, PPP("b"), exclude=exclude)

        assert walked_root.excluded == (".scout.db",)
        assert walked_b.excluded == ("c",)
        assert all(f.name != ".scout.db" for f in walked_root.files)
        assert walked_b.subdirs == ()

    def test_omits_symlinks(self, tree: Tree) -> None:
        """Reading '.' with a symlink to file & symlink to directory, gives neither:
        link.txt is not in files, linkc not in subdirs."""
        (tree.root / "link.txt").symlink_to(tree.root / "b/b2.txt")
        (tree.root / "linkc").symlink_to(tree.root / "b/c", target_is_directory=True)

        walked = _read_dir(tree.root, PPP("."))

        assert all(f.name != "link.txt" for f in walked.files)
        assert "linkc" not in walked.subdirs

    def test_unlistable_dir_is_flagged(
        self, tree: Tree, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Reading b when listing it fails with EACCES gives a WalkedDir
        flagged unlistable, with empty subdirs and files and one error
        carrying path b and errno.EACCES."""
        _fail_scandir(monkeypatch, tree.root, "b", errno.EACCES)

        walked = _read_dir(tree.root, PPP("b"))

        assert walked.unlistable
        assert (walked.subdirs, walked.files) == ((), ())
        assert [(e.path, e.errno) for e in walked.errors] == [(PPP("b"), errno.EACCES)]

    def test_entry_error_lands_in_errors(
        self, tree: Tree, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """b1.txt's stat failing with EIO lands in errors; b2.txt is still read."""
        _fail_stat(monkeypatch, tree.root, "b/b1.txt", errno.EIO)
        walked = _read_dir(tree.root, PPP("b"))

        assert [f.name for f in walked.files] == ["b2.txt"]
        errors = [(e.path, e.errno) for e in walked.errors]
        assert errors == [(PPP("b/b1.txt"), errno.EIO)]
        assert not walked.unlistable


class TestWalk:
    """walk yields one WalkedDir per directory in sorted DFS path order."""

    def test_default_tree_in_dfs_order(self, tree: Tree) -> None:
        """On the default tree the listing paths are ., b, b/c, d in that
        order, each a PPP, and nothing else."""
        assert [d.path for d in walk(tree.root)] == _DIR_PATHS_DFS

    def test_excluded_dir_not_walked_into(self, tree: Tree) -> None:
        """Excluding b/c: the yielded paths are ".", "b", "d",
        so nothing at or under b/c is walked."""
        exclude = frozenset({PPP("b/c")})
        walked = list(walk(tree.root, exclude=exclude))
        assert [w.path for w in walked] == [PPP("."), PPP("b"), PPP("d")]

    def test_unlistable_dir_not_walked_into(
        self, tree: Tree, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Listing b fails: the yielded paths are ".", "b", "d", so nothing
        under b is walked."""
        _fail_scandir(monkeypatch, tree.root, "b", errno.EACCES)
        walked = list(walk(tree.root))
        assert [w.path for w in walked] == [PPP("."), PPP("b"), PPP("d")]
