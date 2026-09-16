# src/scout/cli/render.py
"""Renderers: each turns verb events into output; porcelain is the default.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass

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


def porcelain(event: events.Event) -> Output:
    """Return the porcelain lines for event; raise on unknown event types."""
    match event:
        case events.InitDone():
            return _init_done(event)
    raise TypeError(f"porcelain renderer has no handler for event: {type(event)}")
