# TODO: These below are all for the hash_file function
# TODO: The below random files should be created with a separate script that
# ....  generates them & gets their hashes via md5sum as
# ....  the gold standard reference (for hash tests)
# TODO: Test random large binary files
# TODO: Test random small binary files
# TODO: Test random large text files
# TODO: Test random small text files
# TODO: Test that non-printable character files are handled correctly
# TODO: Test that non-UTF-8 files are handled correctly
# TODO: Test that file permissions (e.g. 000, 777, 644, 600)
# ....  are handled correctly on non-user files (e.g. raise error)
# TODO: Test that symbolic links pointing to real files work when hashing
# TODO: Test that symbolic links pointing to dead links raise errors
# TODO: Test that UTF-8 text files with high codepoints are handled correctly
# import hashlib
import os
from posixpath import realpath
from pyfakefs.fake_filesystem_unittest import Patcher
import pytest
import random

from scoutlib.fs import hash_file


@pytest.fixture
def patcher():
    with Patcher() as patcher:
        yield patcher

def create_random_file(fpath, patcher, size=1024, seed=42, real_dir=None):
    patcher.fs.create_file(fpath)
    random.seed(seed)
    with open(fpath, 'wb') as f:
        f.write(bytearray(random.getrandbits(8) for _ in range(size)))
    if not real_dir:
        # Fill in here so that if a a directory path is given, /$real_dir/$fpath is created as a copy and overwritten




def test_hash_file_empty(patcher):
    # TODO: Check that symlinks leading to real files work,
    # ... but dead links raises errors
    # TODO: Find way to make the hash checking more reliable.
    # ... when trying the function in REPL it matches md5sum, but not here
    # Create mock empty file
    patcher.fs.create_file("empty.txt")

    # Define the corresponding known md5 hashes
    expected = "d41d8cd98f00b204e9800998ecf8427e"  # MD5 hash of an empty string

    # Assert all the trials
    assert expected == hash_file("empty.txt")


def test_hash_file_hello(patcher):
    # Create mock file hello.txt with contents "Hello, World!\n"
    patcher.fs.create_file("hello.txt", contents="Hello, World!\n")

    # Define the corresponding known md5 hashes
    expected = "bea8252ff4e80f41719ea13cdf007273"

    assert expected == hash_file("hello.txt")


def test_ls():
    pass
