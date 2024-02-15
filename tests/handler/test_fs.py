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

from scoutlib.handler import fs


@pytest.fixture
def patcher():
    with Patcher() as patcher:
        yield patcher


def patch_file_empty(patcher):
    patcher.fs.create_file("empty.txt")


def patch_file_hello(patcher):
    contents = "Hello, World!\n"
    patcher.fs.create_file("hello.txt", contents=contents)


# def create_random_file(fpath, patcher, size=1024, seed=42, real_dir=None):
#     patcher.fs.create_file(fpath)
#     random.seed(seed)
#     with open(fpath, 'wb') as f:
#         f.write(bytearray(random.getrandbits(8) for _ in range(size)))
#     if not real_dir:
#         # Fill in here so that if a a directory path is given, /$real_dir/$fpath is created as a copy and overwritten


def test_hash_file_empty(patcher):
    # TODO: Check that symlinks leading to real files work,
    # ... but dead links raises errors
    # TODO: Find way to make the hash checking more reliable.
    # ... when trying the function in REPL it matches md5sum, but not here
    # Create mock empty file
    patch_file_empty(patcher)
    assert fs.hash_md5("empty.txt") == "d41d8cd98f00b204e9800998ecf8427e"


def test_hash_file_hello(patcher):
    patch_file_hello(patcher)
    assert fs.hash_md5_str("hello.txt") == "bea8252ff4e80f41719ea13cdf007273"


def test_hash_file_str_matches(patcher):
    patch_file_hello(patcher)
    assert fs.hash_md5_str("hello.txt") == fs.hash_md5("hello.txt").hexdigest()


def test_hash_file_bytes_matches(patcher):
    patch_file_hello(patcher)
    assert fs.hash_md5_bytes("hello.txt") == fs.hash_md5("hello.txt").digest()


def test_ls():
    pass
