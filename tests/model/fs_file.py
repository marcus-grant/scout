from datetime import datetime
import os
from pyfakefs.fake_filesystem_unittest import Patcher
import pytest
import random

from scoutlib.model.fs_file import FsFile


@pytest.fixture
def patcher():
    with Patcher() as patcher:
        yield patcher


def test_file_from_path(patcher):
    # Mocking
    dt_pre = datetime.now()  # Timestamp before file creation
    content = "Hello, World!\n"
    patcher.fs.create_file("hello.txt", content)
    dt_post = datetime.now()  # Timestamp after file creation
    f = FsFile.from_path("hello.txt")
    # Assetions
    assert f.name == "hello.txt"  # Name matches
    assert f.size == 14  # Size matches
    assert dt_pre <= f.mtime <= dt_post  # Timestamp within range
    assert f.hash_str is None  # Hash not calculated
    assert f.hash_bytes is None  # Hash not calculated
