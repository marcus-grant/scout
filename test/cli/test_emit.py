# test/cli/test_emit.py
"""Pin make_emit: which events reach the sink, and when status is called.
Author: Marcus
Created: 2026-11-25
License: AGPL-3.0-or-later
"""

import io
from pathlib import PurePosixPath as PPP

import factory

from scout.cli import event
from scout.cli.emit import make_emit
from scout.cli.progress import OutputSink
from scout.cli.render import Output
from scout.lib.models import RecordChange

mk_frec = factory.mk_file_record


class RecordingSink(OutputSink):
    """A sink that records its write and status calls in order."""

    def __init__(self) -> None:
        """Start with no calls recorded."""
        super().__init__(io.StringIO(), io.StringIO())
        self.calls: list[tuple[str] | tuple[str, Output]] = []

    def write(self, output: Output) -> None:
        """Record the write."""
        self.calls.append(("write", output))

    def status(self) -> None:
        """Record the status call."""
        self.calls.append(("status",))


def tag(evt: event.CliEvent) -> Output:
    """Render an event as its type name on out."""
    return Output(out=(type(evt).__name__,))


class TestMakeEmit:
    """make_emit over a recording sink and a tagging render."""

    def test_scan_file_dropped_unless_verbose(self) -> None:
        """Without verbose a ScanFile is not written, but status is called."""
        sink = RecordingSink()
        emit = make_emit(False, tag, sink)
        emit(event.ScanFile(PPP("a"), mk_frec(), RecordChange.ADDED))

        assert sink.calls == [("status",)]

    def test_scan_file_written_when_verbose(self) -> None:
        """With verbose a ScanFile is written, then status is called."""
        sink = RecordingSink()
        emit = make_emit(True, tag, sink)
        emit(event.ScanFile(PPP("a"), mk_frec(), RecordChange.ADDED))

        assert sink.calls == [("write", Output(out=("ScanFile",))), ("status",)]

    def test_other_events_always_written(self) -> None:
        """A ScanGone and a ScanFinished are written without verbose, each
        followed by a status call."""
        sink = RecordingSink()
        emit = make_emit(False, tag, sink)
        emit(event.ScanGone(PPP("b")))
        emit(event.ScanFinished(1, 2, 0, 0, 0, 0, 0))

        assert sink.calls == [
            ("write", Output(out=("ScanGone",))),
            ("status",),
            ("write", Output(out=("ScanFinished",))),
            ("status",),
        ]
