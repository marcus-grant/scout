# src/scout/cli/emit.py
"""Route each CLI event through a renderer to the run's sink.
Author: Marcus
Created: 2026-11-25
License: AGPL-3.0-or-later
"""

from collections.abc import Callable

from scout.cli import event
from scout.cli.progress import OutputSink
from scout.cli.render import Output


def make_emit(
    verbose: bool,
    render: Callable[[event.CliEvent], Output],
    sink: OutputSink,
) -> Callable[[event.CliEvent], None]:
    """Return the emit that renders events to sink, hiding ScanFile unless verbose."""

    def emit_all(evt: event.CliEvent) -> None:
        """Write every event rendered, then call status."""
        sink.write(render(evt))
        sink.status()

    def emit_hiding_files(evt: event.CliEvent) -> None:
        """Write evt rendered unless it is a ScanFile, then call status."""
        if not isinstance(evt, event.ScanFile):
            sink.write(render(evt))
        sink.status()

    return emit_all if verbose else emit_hiding_files
