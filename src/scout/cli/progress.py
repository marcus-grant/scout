# src/scout/cli/progress.py
"""Sinks that take rendered output to the terminal streams, with or without
progress.
Author: Marcus
Created: 2026-11-20
License: AGPL-3.0-or-later
"""

import itertools
import os
import time
from collections.abc import Callable
from typing import Self, TextIO

from scout.cli.render import Output
from scout.lib.scan.tally import ScanTally

RATE_WARMUP_NS = 2 * 10**9
LOG_INTERVAL_NS = 10 * 10**9
STATUS_INTERVAL_NS = 500 * 10**6
ERASE = "\r\x1b[K"
SPINNER = "-\\|/"


class OutputSink:
    """Writes rendered output to its streams; no progress shown."""

    def __init__(self, out: TextIO, err: TextIO) -> None:
        """Hold the streams for out and err lines."""
        self._out = out
        self._err = err

    def __enter__(self) -> Self:
        """Return self."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Nothing to clean up."""

    def write(self, output: Output) -> None:
        """Write output's out lines to out and its err lines to err, then flush both."""
        for line in output.out:
            self._out.write(line + "\n")
        for line in output.err:
            self._err.write(line + "\n")
        self._out.flush()
        self._err.flush()

    def status(self) -> None:
        """No progress: nothing."""

    def hashed(self, done: int) -> None:
        """No progress: nothing."""


class ProgressLines(OutputSink):
    """The --progress sink when stderr is not a terminal: plain lines, no CR."""

    def __init__(
        self,
        tally: ScanTally,
        out: TextIO,
        err: TextIO,
        clock: Callable[[], int] = time.monotonic_ns,
        interval_ns: int = LOG_INTERVAL_NS,
    ) -> None:
        """Hold the tally, streams, clock and interval; start timing now."""
        super().__init__(out, err)
        self._tally = tally
        self._clock = clock
        self._interval_ns = interval_ns
        self._start = clock()
        self._last: int | None = None

    def __exit__(self, *exc: object) -> None:
        """Write one final progress line."""
        self._emit(self._clock())

    def status(self) -> None:
        """Write a progress line at the first call, then once per interval."""
        now = self._clock()
        if self._last is None or now - self._last >= self._interval_ns:
            self._emit(now)
            self._last = now

    def _emit(self, now: int) -> None:
        """Write one progress line for time now to err and flush err."""
        line = _progress_line(
            now - self._start, self._tally.files_seen(), self._tally.bytes_hashed()
        )
        self._err.write(line + "\n")
        self._err.flush()


def _terminal_width(stream: TextIO) -> int:
    """The columns of stream's terminal, or 80 when stream is not a terminal."""
    try:
        return os.get_terminal_size(stream.fileno()).columns
    except OSError:
        return 80


class StatusLine(OutputSink):
    """The --progress sink on a terminal: one stderr line, erased and redrawn."""

    def __init__(
        self,
        tally: ScanTally,
        out: TextIO,
        err: TextIO,
        clock: Callable[[], int] = time.monotonic_ns,
        width: Callable[[], int] | None = None,
        interval_ns: int = STATUS_INTERVAL_NS,
    ) -> None:
        """Hold the tally, streams, clock, width and interval; start timing now."""
        super().__init__(out, err)
        self._tally = tally
        self._clock = clock
        self._width = width if width is not None else lambda: _terminal_width(err)
        self._interval_ns = interval_ns
        self._start = clock()
        self._last: int | None = None
        self._spin = itertools.cycle(SPINNER)
        self._slow: tuple[str, int] | None = None

    def __exit__(self, *exc: object) -> None:
        """Erase the status line and leave nothing drawn."""
        self._erase()

    def status(self) -> None:
        """Draw the status line at the first call, then once per interval;
        leaving slow-file mode draws at once."""
        now = self._clock()
        if self._slow is not None:
            self._slow = None
            self._draw(now)
        elif self._last is None or now - self._last >= self._interval_ns:
            self._draw(now)

    def hashed(self, done: int) -> None:
        """Enter slow-file mode holding done, step the spinner, and draw at once."""
        self._slow = (next(self._spin), done)
        self._draw(self._clock())

    def write(self, output: Output) -> None:
        """Erase the status line, write output, then draw the status line again."""
        self._erase()
        super().write(output)
        self._draw(self._clock())

    def _erase(self) -> None:
        """Clear the status line on err and flush err."""
        self._err.write(ERASE)
        self._err.flush()

    def _draw(self, now: int) -> None:
        """Draw the status line for time now on err, cut to the width, and flush."""
        line = _progress_line(
            now - self._start, self._tally.files_seen(), self._tally.bytes_hashed()
        )
        if self._slow is not None:
            spin, done = self._slow
            line += f" {spin} {_human_bytes(done)}"
        self._err.write(ERASE + line[: max(self._width() - 1, 0)])
        self._err.flush()
        self._last = now


def open_sink(progress: bool, tally: ScanTally, out: TextIO, err: TextIO) -> OutputSink:
    """Return the run's sink: plain without progress; with progress, StatusLine
    when err is a terminal, else ProgressLines."""
    if progress and err.isatty():
        return StatusLine(tally, out, err)
    if progress:
        return ProgressLines(tally, out, err)
    return OutputSink(out, err)


def _human_bytes(n: int) -> str:
    """A byte count as B below 1024, else one decimal in KiB, MiB, GiB, TiB or PiB."""
    if n < 1024:
        return f"{n}B"
    units = ("KiB", "MiB", "GiB", "TiB", "PiB")
    value = n / 1024
    unit = 0
    while round(value, 1) >= 1024 and unit < len(units) - 1:
        value /= 1024
        unit += 1
    return f"{value:.1f}{units[unit]}"


def _progress_line(elapsed_ns: int, files: int, hashed: int) -> str:
    """One progress line: elapsed, files and hashed bytes with their average rates."""
    if elapsed_ns < RATE_WARMUP_NS:
        file_rate = byte_rate = "-"
    else:
        file_rate = f"{files * 10**9 / elapsed_ns:.1f}/s"
        byte_rate = f"{_human_bytes(hashed * 10**9 // elapsed_ns)}/s"
    return (
        f"progress {elapsed_ns / 10**9:.1f}s files {files} {file_rate} "
        f"hashed {_human_bytes(hashed)} {byte_rate}"
    )
