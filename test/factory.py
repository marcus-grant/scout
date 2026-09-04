# test/factory.py
"""Factories that build test inputs with defaults and override kwargs.

Author: Marcus
Created: 2026-09-04
License: AGPL-3.0-or-later
"""

from pathlib import Path


def mk_file(root: Path, rel: str, content: bytes = b"") -> Path:
    """Write content to root/rel, creating parents, and return the path."""
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path
