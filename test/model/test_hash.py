# TODO: These below are all for the hash_file function
# TODO: The below random files should be created with a separate script that
# ....  generates them & gets their hashes via md5sum as
# ....  the gold standard reference (for hash tests)
# TODO: Create a closure fixture function that take file name and returns contents
# ... obscuring potentially long contents with a name string
# TODO: Test random large binary files
# TODO: Test random small binary files
# TODO: Test random large text files
# TODO: Test random small text files
# TODO: Test that non-printable character fileNAMES are handled correctly
# TODO: Test that non-printable character files are handled correctly
# TODO: Test that non-UTF-8 files are handled correctly
# TODO: Test that file permissions (e.g. 000, 777, 644, 600)
# ....  are handled correctly on non-user files (e.g. raise error)
# TODO: Test that symbolic links pointing to real files work when hashing
# TODO: Test that symbolic links pointing to dead links raise errors
# TODO: Test that UTF-8 text files with high codepoints are handled correctly
# import hashlib
# import os
import pytest
from pyfakefs.fake_filesystem_unittest import Patcher
from typing import Union

from scoutlib.model.hash import HashMD5


### Fixtures & Helpers ###
@pytest.fixture
def patcher():
    with Patcher() as patcher:
        yield patcher


@pytest.fixture
def create_file(patcher):
    def _create_file(path: str, contents: Union[str, bytes]):
        patcher.fs.create_file(path, contents=contents)
        return path

    return _create_file


def check_hash_members(h: HashMD5, expect: Union[str, bytes]) -> bool:
    """
    Evaluate both members of a HashMD5 object as matching the expected value.
    The 'expect' can be either hex string or bytes.
    Whichever is provided gets converted to the other type for comparison.
    Returns True or False when both members match or not.
    """
    expected_hex, expected_bytes = None, None
    if isinstance(expect, str):
        expected_hex = expect
        expected_bytes = bytes.fromhex(expect)
    elif isinstance(expect, bytes):
        expected_hex = expect.hex()
        expected_bytes = expect
    else:
        msg = "Expect must be str or bytes "
        msg += "(inside tests.test_hash.check_hash_members)"
        raise ValueError(msg)
    return h.hex == expected_hex and h.bytes == expected_bytes


CONTENT_HELLO = "Hello, World!\n"
CONTENT_EMPTY = ""
CONTENT_EMOJI = "Some Emoji:\n🍯🐝🧸🪆🎲🀄🗿\n!?&*@!1"

KNOWN_HASH_HELLO = "bea8252ff4e80f41719ea13cdf007273"
KNOWN_HASH_EMPTY = "d41d8cd98f00b204e9800998ecf8427e"
KNOWN_HASH_EMOJI = "cfe1f89506e6f59d406154f22b7ab920"


### Actual tests ###
def test_from_path_file_not_exist(patcher):
    """
    Tests HashMD5.from_path() when the file does not exist.
    """
    with pytest.raises(ValueError):
        HashMD5.from_path("/this/path/does/not/exist/foobar69420.txt")


# @pytest.mark.parametrize("path", ["empty.txt", "hello.txt"])
# def test_from_path_special_chars_in_path(patcher, create_file, path, content, expect):
#     """
#     Tests HashMD5.from_path() when the file has special characters.
#     """
#     path = "hello.txt"
#     content = "Hello, World!\n"
#     _ = create_file(path, content)
#     h = HashMD5.from_path(path)
#     assert check_hash_members(h, KNOWN_HASH_HELLO)


@pytest.mark.parametrize(
    "path, content, expect",
    [
        ("empty.txt", "", KNOWN_HASH_EMPTY),
        ("hello.txt", CONTENT_HELLO, KNOWN_HASH_HELLO),
        # FIXME: CAn't get this working ("emoji.txt", CONTENT_EMOJI, KNOWN_HASH_EMOJI),
    ],
)
def test_from_path_content(create_file, path, content, expect):
    """
    Tests various parameterized file contents with HashMD5.from_path().
    A known hash is provided for each pair of contents.
    """
    _ = create_file(path, content)
    h = HashMD5.from_path(path)
    assert check_hash_members(h, expect)
