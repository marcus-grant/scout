# test/test_assertion.py
"""Pin the shared assertion helpers themselves.
Author: Marcus
Created: 2026-09-15
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP

import pytest
from assertion import assert_err_fields

import scout.lib.error as Err


class TestAssertErrFields:
    """assert_err_fields checks message words and any error members."""

    def test_words_must_appear_in_message(self) -> None:
        """A word absent from the lowered message fails the assert."""
        with pytest.raises(Err.NotAManifest, match="foobar") as exc:
            raise Err.NotAManifest("foobar")
        with pytest.raises(AssertionError, match="missing"):
            assert_err_fields(exc, "missing")

    def test_fields_must_match_members(self) -> None:
        """A kwarg is compared against the member of the same name."""
        with pytest.raises(Err.NotAManifest) as exc:
            raise Err.NotAManifest("foobar")
        with pytest.raises(AssertionError, match="path"):
            assert_err_fields(exc, path="wrong")

    def test_passes_on_match(self) -> None:
        """Matching words and members raise nothing."""
        with pytest.raises(Err.NotAManifest) as exc:
            raise Err.NotAManifest("foobar", path=PPP("x"))
        assert_err_fields(exc, "foobar", path=PPP("x"))
