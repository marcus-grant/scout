# test/factory.py
"""Factories that build test inputs with defaults and override kwargs.

Author: Marcus
Created: 2026-09-04
License: AGPL-3.0-or-later
"""

from dataclasses import dataclass, field
from pathlib import Path
from pathlib import PurePosixPath as PPP

from scout.lib.manifest import Manifest
from scout.lib.model.file import File
from scout.lib.model.hash import Hash
from scout.lib.repo.db_connector import DBConnector


def mk_file(root: Path, rel: str, content: bytes = b"") -> Path:
    """Write content to root/rel, creating parents, and return the path."""
    (path := root / rel).parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def mk_dir(root: Path, rel: str) -> Path:
    """Create root/rel with parents, if absent, and return the path."""
    (path := root / rel).mkdir(parents=True, exist_ok=True)
    return path


DEFAULT_FILES: dict[PPP, bytes] = {
    PPP("a.txt"): b"alpha",
    PPP("b/b1.txt"): b"bravo",
    PPP("b/b2.txt"): b"alpha",
    PPP("b/c/empty.txt"): b"",
}
DEFAULT_DIRS: list[PPP] = [PPP("d")]


@dataclass
class Tree:
    """A built test tree: its root and the files it contains."""

    root: Path
    files: dict[PPP, bytes] = field(default_factory=dict)
    dirs: list[PPP] = field(default_factory=list)


def _cast_ppp_dict(files: dict[str, bytes]) -> dict[PPP, bytes]:
    return {PPP(k): v for k, v in files.items()}


def mk_tree(
    root: Path,
    files: dict[str, bytes] | None = None,
    dirs: list[str] | None = None,
) -> Tree:
    """Build a tree under root; None selects the defaults, else replaces them."""
    valid_dirs = DEFAULT_DIRS if dirs is None else [PPP(d) for d in dirs]
    valid_files = DEFAULT_FILES if files is None else _cast_ppp_dict(files)
    tree = Tree(root=root, files=valid_files, dirs=valid_dirs)
    for path in tree.dirs:
        mk_dir(root, str(path))
    for path in tree.files:
        mk_file(root, str(path), tree.files[path])
    return tree


def mk_db(root: Path, name: str = ".scout.db") -> DBConnector:
    """Return a DBConnector on a fresh manifest at root / name, rooted at root"""
    return DBConnector(root / name)


def mk_file_model(
    dir_id: int = 0,
    name: str = "f",
    size: int = 1,
    mtime: int = 1,
    hash: Hash | None = None,
    hashed: int | None = None,
    gone: int | None = None,
) -> File:
    """Return a File with small defaults; kw overrides hash, hashed, or gone."""
    return File(
        dir_id,
        name,
        size,
        mtime,
        hash=hash,
        hashed=hashed,
        gone=gone,
    )


def mk_manifest(
    root: Path, name: str = ".scout.db", comment: str | None = None
) -> Manifest:
    """Return a Manifest freshly init'd at root / name, rooted at root."""
    return Manifest.init(root / name, root, comment=comment)
