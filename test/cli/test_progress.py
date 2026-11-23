# test/cli/test_progress.py
"""Pin OutputSink: rendered output reaches its streams, flushed.
Author: Marcus
Created: 2026-11-20
License: AGPL-3.0-or-later
"""

import io

from scout.cli.progress import OutputSink
from scout.cli.render import Output


class FlushRecordingIO(io.StringIO):
    """A StringIO that records its contents at each flush."""

    def __init__(self) -> None:
        """Start empty with no flushes recorded."""
        super().__init__()
        self.flushed: list[str] = []

    def flush(self) -> None:
        """Record the contents so far, then flush."""
        self.flushed.append(self.getvalue())
        super().flush()


class TestOutputSink:
    """OutputSink over StringIO streams."""

    def test_write_sends_each_line_to_its_stream(self) -> None:
        """Out lines reach out and err lines reach err, one per line."""
        out, err = io.StringIO(), io.StringIO()
        OutputSink(out, err).write(Output(out=("a", "b"), err=("c",)))

        assert out.getvalue() == "a\nb\n"
        assert err.getvalue() == "c\n"

    def test_write_flushes_both_streams(self) -> None:
        """Each stream is flushed after its lines are written."""
        out, err = FlushRecordingIO(), FlushRecordingIO()
        OutputSink(out, err).write(Output(out=("a",), err=("c",)))

        assert out.flushed[-1] == "a\n"
        assert err.flushed[-1] == "c\n"

    def test_enter_returns_self(self) -> None:
        """Entering the sink as a context manager binds the sink itself."""
        sink = OutputSink(io.StringIO(), io.StringIO())
        with sink as entered:
            assert entered is sink
