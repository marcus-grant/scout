# src/scout/cli/event.py
"""Verb events: one frozen dataclass per fact a verb can emit.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Event:
    """Root of every verb event; renderers dispatch on concrete types."""


@dataclass(frozen=True)
class InitDone(Event):
    """Init succeeded: where the manifest is, what it roots, what was unread."""

    repo: Path
    root: Path
    missing: tuple[str, ...]
