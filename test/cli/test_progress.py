# test/cli/test_progress.py
"""Pin OutputSink: rendered output reaches its streams, flushed.
Author: Marcus
Created: 2026-11-20
License: AGPL-3.0-or-later
"""

import io
from pathlib import PurePosixPath as PPP

import factory
import pytest

from scout.cli.progress import (
    ERASE,
    LOG_INTERVAL_NS,
    STATUS_INTERVAL_NS,
    OutputSink,
    ProgressLines,
    StatusLine,
    _human_bytes,
    _progress_line,
    _terminal_width,
    open_sink,
)
from scout.cli.render import Output
from scout.lib import event
from scout.lib.models import Hash, RecordChange
from scout.lib.scan.tally import ScanTally

mk_frec = factory.mk_file_record

L0 = "progress 0.0s files 0 - hashed 0B -"


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


class FakeClock:
    """A clock whose current time the test sets."""

    def __init__(self) -> None:
        """Start at zero."""
        self.now = 0

    def __call__(self) -> int:
        """Return the current time."""
        return self.now


class TtyIO(io.StringIO):
    """A StringIO that says it is a terminal."""

    def isatty(self) -> bool:
        """Return True."""
        return True


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


class TestProgressLines:
    """ProgressLines over a fake clock, a real tally and StringIO streams."""

    def test_first_status_writes_a_line(self) -> None:
        """The first status call writes one line to err."""
        err = io.StringIO()
        sink = ProgressLines(ScanTally(), io.StringIO(), err, FakeClock())
        sink.status()

        assert err.getvalue() == "progress 0.0s files 0 - hashed 0B -\n"

    def test_status_within_interval_writes_nothing(self) -> None:
        """A status call less than one interval after the last line adds none."""
        err, clock = io.StringIO(), FakeClock()
        sink = ProgressLines(ScanTally(), io.StringIO(), err, clock)
        sink.status()
        clock.now = LOG_INTERVAL_NS - 1
        sink.status()

        assert len(err.getvalue().splitlines()) == 1

    def test_status_after_interval_writes_again(self) -> None:
        """A status call one full interval after the last line writes another."""
        err, clock = io.StringIO(), FakeClock()
        sink = ProgressLines(ScanTally(), io.StringIO(), err, clock)
        sink.status()
        clock.now = LOG_INTERVAL_NS
        sink.status()

        lines = err.getvalue().splitlines()
        assert len(lines) == 2
        assert lines[1].startswith("progress 10.0s")

    def test_calls_inside_interval_do_not_delay_the_next_line(self) -> None:
        """The next line is due an interval after the last line, not the last call."""
        err, clock = io.StringIO(), FakeClock()
        sink = ProgressLines(ScanTally(), io.StringIO(), err, clock)
        sink.status()
        clock.now = LOG_INTERVAL_NS - 1
        sink.status()
        clock.now = LOG_INTERVAL_NS
        sink.status()

        assert len(err.getvalue().splitlines()) == 2

    def test_interval_is_a_parameter(self) -> None:
        """The interval given at construction decides when the next line writes."""
        err, clock = io.StringIO(), FakeClock()
        sink = ProgressLines(
            ScanTally(), io.StringIO(), err, clock, interval_ns=10**9
        )
        sink.status()
        clock.now = 10**9
        sink.status()

        assert len(err.getvalue().splitlines()) == 2

    def test_exit_writes_a_final_line(self) -> None:
        """Leaving the sink writes one line with the tally's counts and rates."""
        tally, err, clock = ScanTally(), io.StringIO(), FakeClock()
        hashed = mk_frec(size=1024, hash=Hash("A" * 24), hashed=1)
        tally.see(event.FileScanned(PPP("a"), hashed, RecordChange.ADDED))
        sink = ProgressLines(tally, io.StringIO(), err, clock)
        clock.now = 2 * 10**9
        with sink:
            pass

        expected = "progress 2.0s files 1 0.5/s hashed 1.0KiB 512B/s\n"
        assert err.getvalue() == expected

    def test_progress_line_is_flushed(self) -> None:
        """The line from the first status call is flushed to err."""
        err = FlushRecordingIO()
        ProgressLines(ScanTally(), io.StringIO(), err, FakeClock()).status()

        assert err.flushed[-1:] == ["progress 0.0s files 0 - hashed 0B -\n"]


class TestStatusLine:
    """StatusLine over a fake clock, a fixed width and StringIO streams."""

    def test_first_status_draws(self) -> None:
        """The first status call draws the line after an erase, with no newline."""
        err = io.StringIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 80)
        sink.status()

        assert err.getvalue() == ERASE + L0

    def test_status_within_interval_does_not_redraw(self) -> None:
        """A status call less than one interval after the last draw adds none."""
        err, clock = io.StringIO(), FakeClock()
        sink = StatusLine(ScanTally(), io.StringIO(), err, clock, lambda: 80)
        sink.status()
        clock.now = STATUS_INTERVAL_NS - 1
        sink.status()

        assert err.getvalue() == ERASE + L0

    def test_calls_inside_interval_do_not_delay_redraw(self) -> None:
        """The next draw is due an interval after the last draw, not the last call."""
        err, clock = io.StringIO(), FakeClock()
        sink = StatusLine(ScanTally(), io.StringIO(), err, clock, lambda: 80)
        sink.status()
        clock.now = STATUS_INTERVAL_NS - 1
        sink.status()
        clock.now = STATUS_INTERVAL_NS
        sink.status()

        assert err.getvalue().count(ERASE) == 2

    def test_write_erases_writes_redraws(self) -> None:
        """Write erases the line, writes the output, then draws the line again."""
        out, err = io.StringIO(), io.StringIO()
        sink = StatusLine(ScanTally(), out, err, FakeClock(), lambda: 80)
        sink.status()
        sink.write(Output(out=("a",), err=("e",)))

        assert out.getvalue() == "a\n"
        assert err.getvalue() == ERASE + L0 + ERASE + "e\n" + ERASE + L0

    def test_exit_erases_without_redraw(self) -> None:
        """Leaving the sink erases the line and draws nothing after it."""
        err = io.StringIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 80)
        sink.status()
        with sink:
            pass

        assert err.getvalue() == ERASE + L0 + ERASE

    def test_line_cut_to_width(self) -> None:
        """The drawn text is cut to one column less than the width."""
        err = io.StringIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 20)
        sink.status()

        assert err.getvalue() == ERASE + L0[:19]

    def test_draw_is_flushed(self) -> None:
        """The first draw is flushed to err."""
        err = FlushRecordingIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 80)
        sink.status()

        assert err.flushed[-1:] == [ERASE + L0]

    def test_hashed_draws_spinner_and_bytes(self) -> None:
        """A hashed call draws the line at once with a spinner and the bytes done."""
        err = io.StringIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 80)
        sink.status()
        sink.hashed(1024)

        assert err.getvalue().endswith(ERASE + L0 + " - 1.0KiB")

    def test_spinner_advances_per_hashed(self) -> None:
        """Each hashed call steps the spinner one character."""
        err = io.StringIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 80)
        sink.status()
        sink.hashed(1024)
        sink.hashed(2048)

        expected = ERASE + L0 + " - 1.0KiB" + ERASE + L0 + " \\ 2.0KiB"
        assert err.getvalue().endswith(expected)

    def test_status_leaves_slow_mode_at_once(self) -> None:
        """A status call after hashed redraws without the suffix, unthrottled."""
        err = io.StringIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 80)
        sink.status()
        sink.hashed(1024)
        sink.status()

        expected = ERASE + L0 + " - 1.0KiB" + ERASE + L0
        assert err.getvalue().endswith(expected)

    def test_write_keeps_slow_suffix(self) -> None:
        """The redraw after a write keeps the spinner and bytes while slow."""
        err = io.StringIO()
        sink = StatusLine(ScanTally(), io.StringIO(), err, FakeClock(), lambda: 80)
        sink.status()
        sink.hashed(1024)
        sink.write(Output(err=("e",)))

        expected = ERASE + "e\n" + ERASE + L0 + " - 1.0KiB"
        assert err.getvalue().endswith(expected)


class TestTerminalWidth:
    """_terminal_width on a stream that is not a terminal."""

    def test_terminal_width_falls_back_to_80(self) -> None:
        """A stream with no terminal gives 80 columns."""
        assert _terminal_width(io.StringIO()) == 80


class TestOpenSink:
    """open_sink chooses the run's sink from the progress flag."""

    def test_plain_without_progress(self) -> None:
        """Without progress the sink is a plain OutputSink that writes to out."""
        out = io.StringIO()
        sink = open_sink(False, ScanTally(), out, io.StringIO())

        assert type(sink) is OutputSink
        sink.write(Output(out=("a",)))
        assert out.getvalue() == "a\n"

    def test_progress_lines_with_progress(self) -> None:
        """With progress and err not a terminal the sink is ProgressLines and
        status writes to err."""
        err = io.StringIO()
        sink = open_sink(True, ScanTally(), io.StringIO(), err)

        assert isinstance(sink, ProgressLines)
        sink.status()
        assert len(err.getvalue().splitlines()) == 1

    def test_status_line_on_a_terminal(self) -> None:
        """With progress and err a terminal the sink is a StatusLine."""
        sink = open_sink(True, ScanTally(), io.StringIO(), TtyIO())
        assert isinstance(sink, StatusLine)


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


class TestProgressLine:
    """_progress_line over fixed elapsed times and counts."""

    @pytest.mark.parametrize(
        ("elapsed_ns", "files", "hashed", "expected"),
        [
            pytest.param(0, 0, 0, "progress 0.0s files 0 - hashed 0B -", id="start"),
            pytest.param(
                1_500_000_000,
                3,
                1024,
                "progress 1.5s files 3 - hashed 1.0KiB -",
                id="warming-up",
            ),
            pytest.param(
                2_000_000_000,
                4,
                2048,
                "progress 2.0s files 4 2.0/s hashed 2.0KiB 1.0KiB/s",
                id="warmup-ends",
            ),
            pytest.param(
                10_000_000_000,
                318,
                175 * 10 * 1024**2,
                "progress 10.0s files 318 31.8/s hashed 1.7GiB 175.0MiB/s",
                id="typical",
            ),
        ],
    )
    def test_progress_line(
        self, elapsed_ns: int, files: int, hashed: int, expected: str
    ) -> None:
        """Rates show as - until the warmup, then as averages per second."""
        assert _progress_line(elapsed_ns, files, hashed) == expected
