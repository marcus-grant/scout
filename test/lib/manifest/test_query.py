# test/lib/manifest/test_query.py
"""Tests for the queries Manifest composes.
Author: Marcus
Created: 2026-09-30
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP

from factory import mk_file_record

from scout.lib.manifest import Manifest
from scout.lib.manifest.query import ClaimedCounts


class TestClaimed:
    """claimed.counts on stored rows with nothing on disk."""

    def test_counts_claimed_subtree_of_path(self, manifest: Manifest) -> None:
        """Dirs at any depth under path, files in path and those dirs;
        gone and prefix-sharing siblings excluded."""
        b, c = manifest.dirs.upsert(PPP("b")), manifest.dirs.upsert(PPP("b/c"))
        d, bb = manifest.dirs.upsert(PPP("b/d")), manifest.dirs.upsert(PPP("bb"))
        e = manifest.dirs.upsert(PPP("b/c/e"))
        for dir_id in (0, b.id, c.id, d.id, e.id, bb.id):
            manifest.files.upsert(mk_file_record(dir_id=dir_id))
        manifest.gone_subtree.mark(PPP("b/d"), started=7)

        assert manifest.claimed.counts(PPP(".")) == ClaimedCounts(dirs=4, files=5)
        assert manifest.claimed.counts(PPP("b")) == ClaimedCounts(dirs=2, files=3)

    def test_gone_file_in_claimed_dir_not_counted(self, manifest: Manifest) -> None:
        """A file marked gone is excluded while its dir still counts."""
        b = manifest.dirs.upsert(PPP("b"))
        manifest.files.upsert(mk_file_record(dir_id=b.id, name="1"))
        manifest.files.upsert(mk_file_record(dir_id=b.id, name="2"))
        manifest.files.mark_gone_one(b.id, "2", 7)

        assert manifest.claimed.counts(PPP(".")) == ClaimedCounts(dirs=1, files=1)

    def test_unstored_path_counts_nothing(self, manifest: Manifest) -> None:
        """A path with no dir row gives ClaimedCounts(0, 0)."""
        assert manifest.claimed.counts(PPP("x")) == ClaimedCounts(dirs=0, files=0)
        assert manifest.claimed.counts(PPP(".")) == ClaimedCounts(dirs=0, files=0)
