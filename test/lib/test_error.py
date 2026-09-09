# test/lib/test_error.py
"""Pin the ScoutDomain error hierarchy in scout.lib.error.
Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

import inspect
from pathlib import PurePosixPath as PPP

import pytest

import scout.lib.error as Err

HIERARCHY: dict[type[Err.ScoutDomain], type[Err.ScoutDomain]] = {
    Err.ScoutUnknown: Err.ScoutDomain,
    Err.ManifestDomain: Err.ScoutDomain,
    Err.NotAManifest: Err.ManifestDomain,
    Err.BadSchemaVersion: Err.ManifestDomain,
    Err.ManifestExists: Err.ManifestDomain,
    Err.HashDomain: Err.ScoutDomain,
    Err.BadHash: Err.HashDomain,
    Err.UnpairedHash: Err.HashDomain,
    Err.PathDomain: Err.ScoutDomain,
    Err.NotUnderRoot: Err.PathDomain,
}


def defined_errors() -> set[type[Err.ScoutDomain]]:
    """Return every ScoutDomain subclass defined in scout.lib.error."""
    return {
        cls
        for _, cls in inspect.getmembers(Err, inspect.isclass)
        if issubclass(cls, Err.ScoutDomain)
        and cls is not Err.ScoutDomain
        and cls.__module__ == Err.__name__
    }


class TestScoutDomain:
    """ScoutDomain is the root every Scout error descends from."""

    def test_is_exception(self) -> None:
        """ScoutDomain raises and is caught as a plain Exception."""
        with pytest.raises(Exception, match="x"):
            raise Err.ScoutDomain("x")


class TestHierarchy:
    """Every error is caught by the parent HIERARCHY names for it."""

    @pytest.mark.parametrize(("child", "parent"), HIERARCHY.items())
    def test_child_caught_by_parent(
        self, child: type[Err.ScoutDomain], parent: type[Err.ScoutDomain]
    ) -> None:
        """Raising child is caught by except parent."""
        with pytest.raises(parent, match="x"):
            raise child("x")


class TestCoverage:
    """HIERARCHY names every error error.py defines, and nothing else."""

    def test_hierarchy_matches_module(self) -> None:
        """The set of defined errors equals the set HIERARCHY pins."""
        assert defined_errors() == set(HIERARCHY)


class TestManifestDomain:
    """ManifestDomain carries the optional path its children inherit."""

    @pytest.mark.parametrize(
        "cls", [c for c, p in HIERARCHY.items() if p is Err.ManifestDomain]
    )
    def test_carries_optional_path(self, cls: type[Err.ManifestDomain]) -> None:
        """path is None by default and stored when given, on every child."""
        assert cls("x").path is None
        assert cls("x", path=PPP("y")).path == PPP("y")


class TestScoutUnknown:
    """ScoutUnknown rewraps an exception Scout did not predict."""

    def test_wrap_keeps_cause(self) -> None:
        """wrap sets __cause__ to the wrapped exception."""
        err = Err.ScoutUnknown.wrap(cause := RuntimeError("boom"))
        assert err.__cause__ is cause

    def test_wrap_message_names_cause(self) -> None:
        """str() of a wrapped error is the cause's type name and message."""
        cause = RuntimeError("boom")
        assert str(Err.ScoutUnknown.wrap(cause)) == "RuntimeError: boom"


class TestHashDomain:
    """HashDomain carries the optional code its children inherit."""

    @pytest.mark.parametrize(
        "cls", [c for c, p in HIERARCHY.items() if p is Err.HashDomain]
    )
    def test_carries_optional_code(self, cls: type[Err.HashDomain]) -> None:
        """code is None by default and stored when given, on every child."""
        assert cls("x").code is None
        assert cls("x", code="y").code == "y"


class TestPathDomain:
    """PathDomain carries the optional path its children inherit."""

    @pytest.mark.parametrize(
        "cls", [c for c, p in HIERARCHY.items() if p is Err.PathDomain]
    )
    def test_carries_optional_path(self, cls: type[Err.PathDomain]) -> None:
        """path is None by default and stored when given, on every child."""
        assert cls("x").path is None
        assert cls("x", path=PPP("y")).path == PPP("y")
