# test/cli/test_progress.py
"""Pin OutputSink: rendered output reaches its streams, flushed.
Author: Marcus
Created: 2026-11-20
License: AGPL-3.0-or-later
"""

import io

import pytest

from scout.cli.progress import OutputSink, _human_bytes
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


class TestHumanBytes:
    """_human_bytes over fixed byte counts."""

    @pytest.mark.parametrize(
        ("n", "expected"),
        [
            pytest.param(0, "0B", id="zero"),
            pytest.param(1023, "1023B", id="max-bytes"),
            pytest.param(1024, "1.0KiB", id="one-kib"),
            pytest.param(1536, "1.5KiB", id="one-and-half-kib"),
            pytest.param(1024**2 - 1, "1.0MiB", id="rounds-up-to-mib"),
            pytest.param(5 * 1024**4, "5.0TiB", id="five-tib"),
            pytest.param(2048 * 1024**5, "2048.0PiB", id="pib-is-largest"),
        ],
    )
    def test_human_bytes(self, n: int, expected: str) -> None:
        """A byte count renders in its largest IEC unit, one decimal above B."""
        assert _human_bytes(n) == expected
