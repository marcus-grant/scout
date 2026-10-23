# src/scout/cli/render/porcelain.py
"""The porcelain renderer: plain greppable lines per event; the reference renderer.
Author: Marcus
Created: 2026-09-15
Revised: [2026-10-23]
License: AGPL-3.0-or-later
"""

import click

from scout.cli import event
from scout.cli.render import Output


def _init_done(evt: event.InitDone) -> Output:
    """One Initialized line; then one could-not-read line per missing entry."""
    success = f"Initialized scout manifest {evt.repo} for {evt.root}"
    missing = (f"could not read {m}" for m in evt.missing)
    return Output(out=(success,), err=tuple(missing))


def _scan_file(evt: event.ScanFile) -> Output:
    """One out line: the outcome word, a space, the path."""
    return Output(out=(f"{evt.change.value} {evt.path}",))


def _scan_gone(evt: event.ScanGone) -> Output:
    """One out line: gone, a space, the path."""
    return Output(out=(f"gone {evt.path}",))


def _scan_excluded(evt: event.ScanExcluded) -> Output:
    """One out line: excluded, a space, the path."""
    return Output(out=(f"excluded {evt.path}",))


def _scan_error(evt: event.ScanError) -> Output:
    """One err line: the path, a colon, the error's message."""
    return Output(err=(f"{evt.path}: {evt.error}",))


def _scan_access_lost(evt: event.ScanAccessLost) -> Output:
    """One err line: the path, a colon, access lost."""
    return Output(err=(f"{evt.path}: access lost",))


def _scan_finished(evt: event.ScanFinished) -> Output:
    """One out line with every count, in the order added updated matched
    gone errors, each as word space number, space separated."""
    parts = (
        f"added {evt.added}",
        f"updated {evt.updated}",
        f"matched {evt.matched}",
        f"gone {evt.gone}",
        f"errors {evt.errors}",
    )
    return Output(out=(" ".join(parts),))


def render(evt: event.CliEvent) -> Output:
    """Return the porcelain lines for event; raise on unknown event types."""
    match evt:
        case event.InitDone():
            return _init_done(evt)
        case event.ScanFile():
            return _scan_file(evt)
        case event.ScanGone():
            return _scan_gone(evt)
        case event.ScanExcluded():
            return _scan_excluded(evt)
        case event.ScanError():
            return _scan_error(evt)
        case event.ScanAccessLost():
            return _scan_access_lost(evt)
        case event.ScanFinished():
            return _scan_finished(evt)
    raise TypeError(f"porcelain renderer has no handler for event: {type(evt)}")


def echo_porcelain(evt: event.CliEvent) -> None:
    """Render event with porcelain and echo out and err lines."""
    output = render(evt)
    for line in output.out:
        click.echo(line)
    for line in output.err:
        click.echo(line, err=True)
