# test/lib/test_import.py
"""Pin the library entry point: every module under lib imports cleanly.

Author: Marcus
Created: 2026-09-04
License: AGPL-3.0-or-later
"""

import importlib
import pkgutil

import lib


def lib_modules() -> list[str]:
    """Return the dotted names of every module under the lib package."""
    return [m.name for m in pkgutil.walk_packages(lib.__path__, prefix="lib.")]


def test_every_lib_module_imports() -> None:
    """Importing each module under lib raises nothing."""
    names = lib_modules()
    assert names
    for name in names:
        importlib.import_module(name)
