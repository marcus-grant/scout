# test/conftest.py
"""Fixtures: zero-arg calls of the factories in factory.py.

Author: Marcus
Created: 2026-09-07
License: AGPL-3.0-or-later
"""

from pathlib import Path

import factory
import pytest

from scout.lib.manifest import Manifest


@pytest.fixture
def tree(tmp_path: Path) -> factory.Tree:
    """The default factory tree built under tmp_path."""
    return factory.mk_tree(tmp_path)


@pytest.fixture
def manifest(tmp_path: Path) -> Manifest:
    return factory.mk_manifest(tmp_path)
