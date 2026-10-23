# src/scout/cli/render/__init__.py
"""Renderers: each turns verb events into output; porcelain is the default.
Author: Marcus
Created: 2026-09-15
Revised: [2026-10-23]
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Output:
    """Rendered lines split by destination stream."""

    out: tuple[str, ...] = ()
    err: tuple[str, ...] = ()
