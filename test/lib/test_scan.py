# test/lib/test_scan.py
"""Pin lib.scan: the per-file decision and the scan run.
Author: Marcus
Created: 2026-09-18
License: AGPL-3.0-or-later
"""

import factory

from scout.lib.fs.walk import FileStat
from scout.lib.scan import Outcome, decide

# Alias for factory:
# Creates default file with override kwargs:
# File(dir_id=0, name="f", size=1, mtime=1, hash=None, hashed=None, gone=None)
mk_fmodel = factory.mk_file_model


def _mk_fstat(**overrides) -> FileStat:
    """Makes a default FileStat with optional overrides.
    Defaults: name:str="f", size:int=1, mtime:int=1"""
    default = {"name": "f", "size": 1, "mtime": 1}
    return FileStat(**{**default, **overrides})


ADDED = Outcome.ADDED
MATCHED = Outcome.MATCHED
UPDATED = Outcome.UPDATED


class TestDecide:
    """decide maps a stored row and a fresh stat to one Outcome."""

    def test_no_row_is_added(self) -> None:
        """row None gives ADDED, with or without force."""
        assert decide(None, _mk_fstat()) == ADDED
        assert decide(None, _mk_fstat()) == ADDED

    def test_equal_stat_is_matched(self) -> None:
        """A row whose size and mtime equal the stat gives MATCHED."""
        assert decide(mk_fmodel(), _mk_fstat()) == MATCHED

    def test_size_or_mtime_change_is_updated(self) -> None:
        """A row differing from the stat in size only, or mtime only, gives UPDATED."""
        assert decide(mk_fmodel(size=2), _mk_fstat()) == UPDATED
        assert decide(mk_fmodel(mtime=2), _mk_fstat()) == UPDATED
        assert decide(mk_fmodel(size=2, mtime=2), _mk_fstat()) == UPDATED

    def test_force_updates_a_matching_row(self) -> None:
        """A row equal to the stat gives UPDATED when force is True."""
        assert decide(mk_fmodel(), _mk_fstat(), force=True) == UPDATED
