# src/scout/lib/event.py
"""Lib events: one frozen dataclass per fact a lib operation can yield.
Author: Marcus
Created: 2026-10-02
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass
from pathlib import PurePosixPath as PPP

import scout.lib.error as Err
from scout.lib.models import FileRecord, RecordChange


@dataclass(frozen=True)
class Event:
    """Root of every lib event; adapters map concrete types to their own."""


@dataclass(frozen=True)
class FileScanned(Event):
    """One file as the scan left it: its root-relative path, its record,
    and what the scan did to that record."""

    path: PPP
    record: FileRecord
    change: RecordChange


@dataclass(frozen=True)
class RecordGone(Event):
    """One record marked gone: its root-relative path."""

    path: PPP


@dataclass(frozen=True)
class ReadFailed(Event):
    """A path that could not be listed, stated or read, and why."""

    path: PPP
    error: Err.Unreadable
