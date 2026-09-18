# test/lib/fs/test_hash.py
"""Pin lib.fs.hash: a real file hashes to b3c32's answer; unreadable reports.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import errno
from pathlib import PurePosixPath as PPP

import factory
import pytest
from b3c32 import code_from_chunks

import scout.lib.error as Err
from scout.lib.fs.hash import hash_file
from scout.lib.model.hash import DEFAULT_BITS, Hash

Tree = factory.Tree


class TestHashFile:
    """hash_file returns the Hash of a file's bytes or an Unreadable."""

    def test_matches_b3c32_on_file_bytes(self, tree: Tree) -> None:
        """a.txt hashes to Hash(code_from_chunks([b"alpha"], DEFAULT_BITS)),
        and b/c/empty.txt to the hash of no bytes."""
        file_alpha = Hash(code_from_chunks([b"alpha"], DEFAULT_BITS))
        file_empty = Hash(code_from_chunks([], DEFAULT_BITS))

        assert hash_file(tree.root / "a.txt") == file_alpha
        assert hash_file(tree.root / "b" / "c" / "empty.txt") == file_empty

    def test_unreadable_returns_error(
        self, tree: Tree, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With b3c32.code_from_path monkeypatched to raise PermissionError,
        hash_file returns Unreadable with errno EACCES and path a.txt."""

        def raiser(*a, **k):
            _, _ = a, k  # Shut up LSP
            raise PermissionError(errno.EACCES, "Permission denied")

        monkeypatch.setattr("scout.lib.fs.hash.code_from_path", raiser)

        result = hash_file(tree.root / "a.txt")

        assert isinstance(result, Err.Unreadable)
        assert result.errno == errno.EACCES
        assert result.path == PPP("a.txt")

    def test_progress_callback_sees_bytes(self, tree: Tree) -> None:
        """With interval_ms 0 and a list-appending callback on a.txt, the
        callback is called at least once and its last value is 5."""
        seen: list[int] = []

        result = hash_file(tree.root / "a.txt", on_progress=seen.append)

        assert isinstance(result, Hash)
        assert seen and seen[-1] == 5
