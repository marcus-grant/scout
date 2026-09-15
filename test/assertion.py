# test/assertion.py
"""Shared assertion helpers for the test suite.
Author: Marcus
Created: 2026-09-10
License: AGPL-3.0-or-later
"""

import pytest


def assert_err_fields(exc: pytest.ExceptionInfo, *words: str, **fields: object) -> None:
    """The error's message mentions words; each kwarg equals that member."""
    text = str(exc.value).lower()
    for word in words:
        assert word in text, f"{word!r} not in {text!r}"
    for name, expect in fields.items():
        got = getattr(exc.value, name)
        assert got == expect, f"{name}: {got!r} != {expect!r}"
