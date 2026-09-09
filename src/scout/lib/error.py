# src/scout/lib/error.py
"""Typed error hierarchy for scout.lib, rooted at ScoutDomain.

Rules:
- Every class here is constructible as Cls("message"), like Exception.
- Names ending in Domain group errors and are meant to be subclassed;
  a domain may add members its children inherit, always optional.
- Concrete errors carry no Error suffix and read as `except Err.Name`.
- ScoutUnknown rewraps what Scout did not predict; build it with wrap.
- The lib raises; the CLI catches at the subcommand boundary.

Author: Marcus
Created: 2026-09-08
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP


class ScoutDomain(Exception):
    """Root of every error scout.lib raises; the CLI catches at its boundary."""


class ScoutUnknown(ScoutDomain):
    """An exception Scout did not predict, rewrapped so the boundary can report it."""

    @classmethod
    def wrap(cls, cause: BaseException) -> "ScoutUnknown":
        """Return an instance naming cause in its message with cause as __cause__."""
        err = cls(f"{type(cause).__name__}: {cause}")
        err.__cause__ = cause
        return err


class ManifestDomain(ScoutDomain):
    """Errors about one manifest file: finding, opening, or creating it."""

    path: PPP | None = None

    def __init__(self, msg: str, path: PPP | None = None) -> None:
        """Store the manifest path the error is about, when known."""
        self.path = path
        super().__init__(msg)


class NotAManifest(ManifestDomain):
    """The file exists but is not a Scout manifest."""


class BadSchemaVersion(ManifestDomain):
    """The manifest's schema_version is not the one this build reads."""


class ManifestExists(ManifestDomain):
    """A manifest is already present at the target path."""


class HashDomain(ScoutDomain):
    """Errors about a hash code: its alphabet or its width."""

    code: str | None

    def __init__(self, msg: str, code: str | None = None) -> None:
        self.code = code
        super().__init__(msg)


class BadHash(HashDomain):
    """A string that is not a certified-width b3c32 code."""


class UnpairedHash(HashDomain):
    """A hash without its hashed scan, or a hashed scan without a hash."""


class PathDomain(ScoutDomain):
    """Errors about a path handed to the lib: shape or placement."""

    path: PPP | None

    def __init__(self, msg: str, path: PPP | None = None) -> None:
        """Store the offending path, when known."""
        self.path = path
        super().__init__(msg)


class NotUnderRoot(PathDomain):
    """A path that is absolute or escapes root with .. cannot be stored."""
