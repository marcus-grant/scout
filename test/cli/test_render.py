# test/cli/test_render.py
"""Pin porcelain rendering of verb events to fixed text.
Author: Marcus
Created: 2026-09-16
License: AGPL-3.0-or-later
"""

from pathlib import Path

import pytest

from scout.cli.event import InitDone
from scout.cli.render import porcelain


class TestPorcelainInitDone:
    """porcelain turns InitDone into the fixed result lines."""

    _INIT_DONE = "Initialized scout manifest {repo} for {root}"

    def _expected_success_out(self, repo: Path, root: Path) -> tuple[str, ...]:
        """Expected out lines for a successful init on repo and root."""
        return (self._INIT_DONE.format(repo=repo, root=root),)

    def _expected_missing_err(self, missing: tuple[str, ...]) -> tuple[str, ...]:
        """Expected err lines naming each missing detail property."""
        return tuple(f"could not read {m}" for m in missing)

    def test_success_wording(self) -> None:
        """The exact success line, fully literal, pinned once."""
        event = InitDone(Path("/data/.scout.db"), Path("/data"), missing=())
        assert porcelain(event).out == (
            "Initialized scout manifest /data/.scout.db for /data",
        )

    def test_missing_wording(self) -> None:
        """The exact could-not-read line, fully literal, pinned once."""
        event = InitDone(Path("/data/.scout.db"), Path("/data"), ("fs_uuid",))
        assert porcelain(event).err == ("could not read fs_uuid",)

    def test_success_has_no_err_lines(self) -> None:
        """Nothing missing means an empty err stream."""
        event = InitDone(Path("/data/.scout.db"), Path("/data"), missing=())
        assert porcelain(event).err == ()

    def test_missing_details_each_get_a_line(self) -> None:
        """One err line per missing entry, out unchanged, via templates."""
        repo = (root := Path("/data")) / ".scout.db"
        missing = ("fs_uuid", "hostname")
        out = porcelain(InitDone(repo, root, missing))
        assert out.out == self._expected_success_out(repo, root)
        assert out.err == self._expected_missing_err(missing)

    def test_unknown_event_raises(self) -> None:
        """An event type porcelain does not know raises, not silence."""
        with pytest.raises(TypeError):
            porcelain(object())  # type: ignore[arg-type]
