# test/cli/test_render.py
"""Pin porcelain rendering of verb events to fixed text.
Author: Marcus
Created: 2026-09-16
License: AGPL-3.0-or-later
"""

from pathlib import Path
from pathlib import PurePosixPath as PPP

import factory
import pytest

import scout.cli.event as events
import scout.lib.error as Err
from scout.cli.render import Output, porcelain
from scout.lib.models import RecordChange


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
        evt = events.InitDone(Path("/data/.scout.db"), Path("/data"), missing=())
        assert porcelain(evt).out == (
            "Initialized scout manifest /data/.scout.db for /data",
        )

    def test_missing_wording(self) -> None:
        """The exact could-not-read line, fully literal, pinned once."""
        evt = events.InitDone(Path("/data/.scout.db"), Path("/data"), ("fs_uuid",))
        assert porcelain(evt).err == ("could not read fs_uuid",)

    def test_success_has_no_err_lines(self) -> None:
        """Nothing missing means an empty err stream."""
        evt = events.InitDone(Path("/data/.scout.db"), Path("/data"), missing=())
        assert porcelain(evt).err == ()

    def test_missing_details_each_get_a_line(self) -> None:
        """One err line per missing entry, out unchanged, via templates."""
        repo = (root := Path("/data")) / ".scout.db"
        missing = ("fs_uuid", "hostname")
        out = porcelain(events.InitDone(repo, root, missing))
        assert out.out == self._expected_success_out(repo, root)
        assert out.err == self._expected_missing_err(missing)

    def test_unknown_event_raises(self) -> None:
        """An event type porcelain does not know raises, not silence."""
        with pytest.raises(TypeError):
            porcelain(object())  # type: ignore[arg-type]


class TestPorcelainScan:
    """Exact porcelain lines for the five scan events, pinned once each."""

    def test_file_line(self) -> None:
        """ScanFile(b/x.txt, any FileRecord, ADDED) renders out ("added b/x.txt",)."""
        args = (PPP("b/x.txt"), factory.mk_file_record(), RecordChange.ADDED)
        evt = events.ScanFile(*args)

        assert porcelain(evt).out == ("added b/x.txt",)
        assert porcelain(evt).err == ()

    def test_gone_line(self) -> None:
        """ScanGone(b/x.txt) renders out ("gone b/x.txt",)."""
        assert porcelain(events.ScanGone(PPP("b/x.txt"))).out == ("gone b/x.txt",)

    def test_error_line(self) -> None:
        """ScanError(b/c, Unreadable("Permission denied")) renders err
        ("b/c: Permission denied",) and empty out."""
        evt = events.ScanError(PPP("b/c"), Err.Unreadable("Permission denied"))

        assert porcelain(evt) == Output(err=("b/c: Permission denied",))

    def test_finished_line(self) -> None:
        """ScanFinished with counts 4 1 2 3 0 renders out
        ("added 4 updated 1 matched 2 gone 3 errors 0",)."""
        evt = events.ScanFinished(1, 2, added=4, updated=1, matched=2, errors=0, gone=3)
        msg = "added 4 updated 1 matched 2 gone 3 errors 0"

        assert porcelain(evt) == Output(out=(msg,))
