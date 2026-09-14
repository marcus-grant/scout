# test/assertion.py
"""Shared assertion helpers for the test suite.
Author: Marcus
Created: 2026-09-10
License: AGPL-3.0-or-later
"""

from pathlib import Path
from pathlib import PurePosixPath as PPP

import pytest


def assert_err_words(exc: pytest.ExceptionInfo, path: Path | PPP, *words: str) -> None:
    """The error carries path as PPP and its message mentions path and words."""
    assert exc.value.path == PPP(path.as_posix())
    text = str(exc.value).lower()
    for word in (path.as_posix().lower(), *words):
        assert word in text, f"{word!r} not in {text!r}"
