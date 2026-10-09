# src/scout/lib/manifest/service.py
"""Services: one concern's writes across repos, composed by Manifest.
Author: Marcus
Created: 2026-09-29
License: AGPL-3.0-or-later
"""

from pathlib import PurePosixPath as PPP

from scout.lib.manifest.repo.dir_repo import DirRepo
from scout.lib.manifest.repo.file_repo import FileRepo


class GoneSubtree:
    """Mark a stored subtree gone across the dir and file tables."""

    def __init__(self, dirs: DirRepo, files: FileRepo) -> None:
        """Bind the two repos the marking writes through."""
        self.dirs = dirs
        self.files = files

    def mark(self, path: PPP, started: int) -> list[PPP]:
        """Mark the present dir at path, every present dir under it,
        and every present file in them gone with started;
        return their paths: every file first, then every dir, each in path order.
        Empty when path is not stored.
        Writes complete before returning; nothing is committed."""
        if (top := self.dirs.get(path)) is None:
            return []  # Empty subtrees should be empty lists

        # Prepare the loops and returned paths to go through all descendants
        subtree = [top, *self.dirs.descendants(path)]
        paths: list[PPP] = []
        for d in subtree:  # For each, collect file paths to append first
            paths.extend(d.path / f.stat.name for f in self.files.in_dir(d.id))
        for d in subtree:  # Then append the dir path itself
            paths.append(d.path)

        # Before returning, actually mark both table rows gone with started
        self.files.mark_gone([d.id for d in subtree], started)
        self.dirs.mark_gone(path, started)
        return paths
