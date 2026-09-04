# test/test_factory.py
"""Pin what the test factories produce.

Author: Marcus
Created: 2026-09-04
License: AGPL-3.0-or-later
"""

from pathlib import Path

from factory import mk_file


def test_mk_file_writes_content_under_root(tmp_path: Path) -> None:
    """mk_file creates parent directories, writes the bytes, and
    returns the absolute path."""
    rel = "a/b/hello.txt"
    content = b"Hello, World!"
    path = mk_file(tmp_path, rel, content)
    read_content = path.read_bytes()
    assert path == tmp_path / rel
    assert read_content == content
