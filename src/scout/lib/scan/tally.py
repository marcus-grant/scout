# src/scout/lib/scan/tally.py
"""Count what a scan reports, and freeze the counts into its Summary.
Author: Marcus
Created: 2026-10-15
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass

import scout.lib.event as events
from scout.lib.models import RecordChange


@dataclass(frozen=True)
class Summary:
    """What one scan did in total: its window and the count per change,
    plus the errors met and the rows marked gone."""

    started: int
    finished: int
    added: int
    updated: int
    matched: int
    errors: int
    gone: int


class ScanTally:
    """Live counts of one scan's events; one instance per run."""

    def __init__(self) -> None:
        """Start every count at zero."""
        self._counts = {enum_member: 0 for enum_member in RecordChange}
        self._errors = 0
        self._gone = 0

    def see(self, event: events.Event) -> None:
        """Count one event: FileScanned by its change, RecordGone, ReadFailed."""
        if isinstance(event, events.FileScanned):
            self._counts[event.change] += 1
        elif isinstance(event, events.RecordGone):
            self._gone += 1
        elif isinstance(event, events.ReadFailed):
            self._errors += 1

    def summary(self, started: int, finished: int) -> Summary:
        """Freeze the counts into a Summary for the given window."""
        return Summary(
            started,
            finished,
            self._counts[RecordChange.ADDED],
            self._counts[RecordChange.UPDATED],
            self._counts[RecordChange.MATCHED],
            self._errors,
            self._gone,
        )

    def files_seen(self) -> int:
        """How many files the scan saw: every FileScanned, whatever its change."""
        return sum(self._counts.values())
