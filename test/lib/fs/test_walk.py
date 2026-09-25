# test/lib/fs/test_walk.py
"""Pin lib.fs.walk: DFS listings of a factory tree, one per directory.
Author: Marcus
Created: 2026-09-17
License: AGPL-3.0-or-later
"""

import errno
import os
from collections.abc import Iterable
from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest

from scout.lib.fs.walk import FileStat, WalkedDir, walk

# tree: Tree is a fixture that is the default case of mk_tree
# Its shape from tmp_path as root is:
# ./a.txt, ./b/b1.txt, ./b/b2.txt, ./b/c/empty.txt, ./d/(empty dir)
Tree = factory.Tree

# Expected DFS order PurePosixPaths (PPP) of default tree fixture directories
_DIR_PATHS_DFS = [PPP(n) for n in (".", "b", "b/c", "d")]


def _walked_dir_at(listings: Iterable[WalkedDir], path: str | PPP) -> WalkedDir:
    """Iterate walk()'s returned WalkedDir iterator and return matching path or None."""
    _path = PPP(path) if isinstance(path, str) else path
    lst = next((lst for lst in listings if lst.path == _path), None)
    assert lst is not None, f"No listing found with path {path}"
    return lst


class TestWalk:
    """walk yields one WalkedDir per directory in sorted DFS path order."""

    def test_default_tree_in_dfs_order(self, tree: Tree) -> None:
        """On the default tree the listing paths are ., b, b/c, d in that
        order, each a PPP, and nothing else."""
        assert [d.path for d in walk(tree.root)] == _DIR_PATHS_DFS

    def test_listing_holds_file_stats(self, tree: Tree) -> None:
        """The listing for b holds FileStat(b1.txt, 5, mtime) and
        FileStat(b2.txt, 5, mtime) in name order, where each mtime equals
        the file's st_mtime_ns, and no errors."""
        b1_path, b2_path = tree.root / "b/b1.txt", tree.root / "b/b2.txt"
        b1_stat = FileStat("b1.txt", 5, b1_path.stat().st_mtime_ns)
        b2_stat = FileStat("b2.txt", 5, b2_path.stat().st_mtime_ns)

        b_lst = _walked_dir_at(walk(tree.root), "b")
        assert b_lst.files == (b1_stat, b2_stat)

    def test_walked_dir_holds_subdir_names(self, tree: Tree) -> None:
        """Each WalkedDir's subdirs are its child directory names in name
        order: ("b", "d") for ".", ("c",) for "b", () for "b/c" and "d"."""
        results_map = {walked.path: walked.subdirs for walked in walk(tree.root)}

        assert results_map[PPP(".")] == ("b", "d")
        assert results_map[PPP("b")] == ("c",)
        assert results_map[PPP("b/c")] == ()
        assert results_map[PPP("d")] == ()

    def test_exclude_skips_those_files_and_dirs(self, tree: Tree) -> None:
        """Excluding ./.scout.db and b/c: the root's WalkedDir has no
        .scout.db in files, b's WalkedDir has no subdirs, and the yielded
        paths are ".", "b", "d", so nothing at or under b/c is walked."""
        factory.mk_file(tree.root, ".scout.db", b"manifest")
        exclude = frozenset({PPP(".scout.db"), PPP("b/c")})

        walked = list(walk(tree.root, exclude=exclude))

        root_walk, b_walk = _walked_dir_at(walked, "."), _walked_dir_at(walked, "b")
        assert all(f.name != ".scout.db" for f in root_walk.files)
        assert b_walk.subdirs == ()
        assert [w.path for w in walked] == [PPP("."), PPP("b"), PPP("d")]

    def test_symlinks_are_not_listed_or_followed(self, tree: Tree) -> None:
        """A symlink to a file is not in any listing; a symlink to a
        directory yields no listing of its own and is not descended."""
        (tree.root / "link.txt").symlink_to(tree.root / "b/b2.txt")
        (tree.root / "linkc").symlink_to(tree.root / "b/c", target_is_directory=True)

        listings_list = list(walk(tree.root))
        all_files = [f for lst in listings_list for f in lst.files]

        assert all(f.name != "link.txt" for f in all_files)
        assert [lst.path for lst in listings_list] == _DIR_PATHS_DFS

    def test_unlistable_dir_is_reported_and_not_descended(
        self, tree: Tree, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A directory that can't be listed yields WalkedDir flagged unlistable,
        whose one error carries errno.EACCES, and nothing under it is walked:
        the yielded paths are ".", "b", "d".
        Patches os.scandir to raise PermissionError on b,
        so test run as root isn't bypassing a mode check."""
        real = os.scandir

        def fake(path):
            if Path(path) == tree.root / "b":
                raise PermissionError(errno.EACCES, "Permission denied")
            return real(path)

        monkeypatch.setattr("scout.lib.fs.walk.os.scandir", fake)
        yielded = list(walk(tree.root))

        bad = _walked_dir_at(yielded, "b")
        assert bad.unlistable
        assert bad.errors[0].errno == errno.EACCES
        assert [walked.path for walked in yielded] == [PPP("."), PPP("b"), PPP("d")]
