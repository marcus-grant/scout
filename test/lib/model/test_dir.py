# test/lib/model/test_dir.py
"""Pin the Dir model in scout.lib.model.dir.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

from dataclasses import FrozenInstanceError
from pathlib import PurePosixPath as PPP

import pytest

from scout.lib.model.dir import Dir


class TestDir:
    """Dir mirrors one dir row: id, path relative to root, gone."""

    def test_gone_defaults_to_none(self) -> None:
        """Dir(id, path) is live: gone is None."""
        assert Dir(7, PPP("a/b")).gone is None

    def test_equal_by_fields(self) -> None:
        """Two Dir with the same id, path, and gone are equal."""
        args = (7, PPP("a/b"), 42)
        assert Dir(*args) == Dir(*args)

    def test_frozen(self) -> None:
        """Assigning a field raises FrozenInstanceError."""
        dir = Dir(7, PPP("a/b"))
        with pytest.raises(FrozenInstanceError):
            dir.gone = 42  # type: ignore
