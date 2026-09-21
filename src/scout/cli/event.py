# src/scout/cli/event.py
"""Verb events: one frozen dataclass per fact a verb can emit.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass
from pathlib import Path
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.model.file import File
from scout.lib.scan import Outcome


@dataclass(frozen=True)
class Event:
    """Root of every verb event; renderers dispatch on concrete types."""


@dataclass(frozen=True)
class InitDone(Event):
    """Init succeeded: where the manifest is, what it roots, what was unread."""

    repo: Path
    root: Path
    missing: tuple[str, ...]


@dataclass(frozen=True)
class ScanStarted(Event):
    """A scan began: which manifest, which root, when."""

    repo: Path
    root: Path
    started: int


@dataclass(frozen=True)
class ScanFile(Event):
    """One file processed: its root-relative path, its row, its outcome."""

    path: PPP
    file: File
    outcome: Outcome


@dataclass(frozen=True)
class ScanGone(Event):
    """One row marked gone this scan: its root-relative path."""

    path: PPP


@dataclass(frozen=True)
class ScanError(Event):
    """A file or directory the scan could not read."""

    path: PPP
    error: Err.ScanDomain


@dataclass(frozen=True)
class ScanFinished(Event):
    """A scan ended: its window and the count per outcome, errors, gone."""

    started: int
    finished: int
    added: int
    updated: int
    matched: int
    errors: int
    gone: int
