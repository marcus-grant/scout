# src/scout/cli/render.py
"""Renderers: each turns verb events into output; porcelain is the default.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass

import click

import scout.cli.event as events


@dataclass(frozen=True)
class Output:
    """Rendered lines split by destination stream."""

    out: tuple[str, ...] = ()
    err: tuple[str, ...] = ()


def _init_done(event: events.InitDone) -> Output:
    """One Initialized line; then one could-not-read line per missing entry."""
    success = f"Initialized scout manifest {event.repo} for {event.root}"
    missing = (f"could not read {m}" for m in event.missing)
    return Output(out=(success,), err=tuple(missing))


def _scan_file(event: events.ScanFile) -> Output:
    """One out line: the outcome word, a space, the path."""
    return Output(out=(f"{event.outcome.value} {event.path}",))


def _scan_gone(event: events.ScanGone) -> Output:
    """One out line: gone, a space, the path."""
    return Output(out=(f"gone {event.path}",))


def _scan_error(event: events.ScanError) -> Output:
    """One err line: the path, a colon, the error's message."""
    return Output(err=(f"{event.path}: {event.error}",))


def _scan_finished(event: events.ScanFinished) -> Output:
    """One out line with every count, in the order added updated matched
    gone errors, each as word space number, space separated."""
    parts = (
        f"added {event.added}",
        f"updated {event.updated}",
        f"matched {event.matched}",
        f"gone {event.gone}",
        f"errors {event.errors}",
    )
    return Output(out=(" ".join(parts),))


def porcelain(event: events.CliEvent) -> Output:
    """Return the porcelain lines for event; raise on unknown event types."""
    match event:
        case events.InitDone():
            return _init_done(event)
        case events.ScanFile():
            return _scan_file(event)
        case events.ScanGone():
            return _scan_gone(event)
        case events.ScanError():
            return _scan_error(event)
        case events.ScanFinished():
            return _scan_finished(event)
    raise TypeError(f"porcelain renderer has no handler for event: {type(event)}")


def echo_porcelain(event: events.CliEvent) -> None:
    """Render event with porcelain and echo out and err lines."""
    output = porcelain(event)
    for line in output.out:
        click.echo(line)
    for line in output.err:
        click.echo(line, err=True)
