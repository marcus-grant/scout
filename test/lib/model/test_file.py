# test/lib/model/test_file.py
"""Pin the File model in scout.lib.model.file.
Author: Marcus
Created: 2026-09-09
License: AGPL-3.0-or-later
"""

from dataclasses import FrozenInstanceError

import pytest

import scout.lib.error as Err
from scout.lib.model.file import File
from scout.lib.model.hash import Hash


class TestFile:
    """File mirrors one file row keyed by (dir_id, name)."""

    def test_defaults_to_unhashed_and_live(self) -> None:
        """File(dir_id, name, size, mtime) has hash, hashed, and gone None."""
        f = File(1, "foo", 42, 2**30)
        assert all(getattr(f, attr) is None for attr in ("hash", "hashed", "gone"))

    def test_equal_by_fields(self) -> None:
        """Two File built from the same args are equal."""
        args = (1, "foo", 42, 2**30)
        kw = {"hash": Hash("A" * 24), "hashed": (2**30) + 1}
        assert File(*args, **kw) == File(*args, **kw)

    def test_frozen(self) -> None:
        """Assigning a field raises FrozenInstanceError."""
        with pytest.raises(FrozenInstanceError):
            File(1, "foo", 42, 2**30).hash = Hash("A" * 24)  # type: ignore

    @pytest.mark.parametrize("kw", [{"hash": Hash("A" * 24)}, {"hashed": 7}])
    def test_rejects_hash_without_hashed(self, kw: dict) -> None:
        """hash and hashed must both be set or both be None."""
        with pytest.raises(Err.UnpairedHash):
            File(1, "foo", 42, 2**30, **kw)
