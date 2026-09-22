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

from scout.lib.fs.walk import FileStat, Listing, walk

"""
tree: Tree is a fixture that is the default case of mk_tree
Its shape from tmp_path as root is:
./a.txt, ./b/b1.txt, ./b/b2.txt, ./b/c/empty.txt, ./d/(empty dir)
"""
Tree = factory.Tree

# Expected DFS order PurePosixPaths (PPP) of default tree fixture directories
_DIR_PATHS_DFS = [PPP(n) for n in (".", "b", "b/c", "d")]


def _listing_by_path(listings: Iterable[Listing], path: str | PPP) -> Listing:
    """Iterate walk()'s returned Listing iterator and return matching path or None."""
    _path = PPP(path) if isinstance(path, str) else path
    lst = next((lst for lst in listings if lst.path == _path), None)
    assert lst is not None, f"No listing found with path {path}"
    return lst


class TestWalk:
    """walk yields one Listing per directory in sorted DFS path order."""

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

        b_lst = _listing_by_path(walk(tree.root), "b")
        assert b_lst.files == (b1_stat, b2_stat)

    def test_exclude_skips_that_file(self, tree: Tree) -> None:
        """A file at an excluded root-relative path is absent from its
        directory's listing; every other file is present."""
        factory.mk_file(tree.root, EXCLUDE := ".scout.db", b"dont scan")

        listings = walk(tree.root, exclude=frozenset({PPP(EXCLUDE)}))

        assert all(f.name != EXCLUDE for f in _listing_by_path(listings, ".").files)

    def test_symlinks_are_not_listed_or_followed(self, tree: Tree) -> None:
        """A symlink to a file is not in any listing; a symlink to a
        directory yields no listing of its own and is not descended."""
        (tree.root / "link.txt").symlink_to(tree.root / "b/b2.txt")
        (tree.root / "linkc").symlink_to(tree.root / "b/c", target_is_directory=True)

        listings_list = list(walk(tree.root))
        all_files = [f for lst in listings_list for f in lst.files]

        assert all(f.name != "link.txt" for f in all_files)
        assert [lst.path for lst in listings_list] == _DIR_PATHS_DFS

    def test_unreadable_dir_yields_error_and_no_files(
        self, tree: Tree, monkeypatch
    ) -> None:
        """A directory with mode 000 yields a Listing with its path,
        no files, and one Err.Unreadable carrying that path and errno.EACCES.
        Instead of setting a file to mode 000, patch os.scandir to
        raise PermissionError on a test path.
        That way root running this test doesn't bypass the mode check."""
        real = os.scandir

        def fake(path):
            if Path(path) == tree.root / "b" / "c":
                raise PermissionError(errno.EACCES, "Permission denied")
            return real(path)

        monkeypatch.setattr("scout.lib.fs.walk.os.scandir", fake)
        listings_list = list(walk(tree.root))
        bad = _listing_by_path(listings_list, "b/c")
        assert bad.files == ()
        assert (bad.errors[0].path, bad.errors[0].errno) == (PPP("b/c"), errno.EACCES)
        assert [lst.path for lst in listings_list] == _DIR_PATHS_DFS
