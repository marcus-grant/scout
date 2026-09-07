# test/conftest.py
"""Fixtures: zero-arg calls of the factories in factory.py.

Author: Marcus
Created: 2026-09-07
License: AGPL-3.0-or-later
"""

from pathlib import Path

import pytest
from factory import Tree, mk_tree


@pytest.fixture
def tree(tmp_path: Path) -> Tree:
    """The default factory tree built under tmp_path."""
    return mk_tree(tmp_path)
