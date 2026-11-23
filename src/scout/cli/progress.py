# src/scout/cli/progress.py
"""Sinks that take rendered output to the terminal streams, with or without
progress.
Author: Marcus
Created: 2026-11-20
License: AGPL-3.0-or-later
"""

from typing import Self, TextIO

from scout.cli.render import Output


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
