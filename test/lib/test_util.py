# test/lib/test_util.py
"""Pin the pure helpers in scout.lib.util.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from collections.abc import Callable
from pathlib import Path
from pathlib import PurePosixPath as PPP

import pytest
from assertion import assert_err_fields

import scout.lib.error as Err
from scout.lib.util import to_rel


class TestToRel:
    """to_rel maps a path to root-relative PPP or raises NotUnderRoot."""

    def test_absolute_under_root_relativizes(self, tmp_path: Path) -> None:
        """An absolute path under root returns its relative PPP."""
        given, expect = tmp_path / "subdir/file.txt", PPP("subdir/file.txt")
        assert to_rel(given, tmp_path) == expect

    def test_str_input_matches_path_input(self, tmp_path: Path) -> None:
        """The same location as str and as Path yield the same PPP."""
        given_str = str(given_path := tmp_path / "subdir/file.txt")
        assert to_rel(given_str, tmp_path) == to_rel(given_path, tmp_path)

    @pytest.mark.parametrize(
        "make_bad",
        [
            lambda root: root / "..",
            lambda root: root / ".." / "elsewhere",
            lambda _: Path("/definitely/elsewhere"),
        ],
    )
    def test_rejects_escape(
        self, tmp_path: Path, make_bad: Callable[[Path], Path]
    ) -> None:
        """A path resolving outside root raises Err.NotUnderRoot."""
        bad_posix = (bad := make_bad(tmp_path)).as_posix()
        with pytest.raises(Err.NotUnderRoot) as exc:
            to_rel(bad, tmp_path)
        assert_err_fields(exc, bad_posix, path=PPP(bad_posix))

    def test_root_itself_is_dot(self, tmp_path: Path) -> None:
        """root maps to PPP('.'), the root dir row's path."""
        assert to_rel(tmp_path, tmp_path) == PPP(".")
