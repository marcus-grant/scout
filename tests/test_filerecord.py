# import os
# from pyfakefs.fake_filesystem_unittest import Patcher
# import pytest
# import random
#
# import scoutlib.filerecord as fr
#
#
# @pytest.fixture
# def patcher():
#     with Patcher() as patcher:
#         yield patcher
#
#
# def test_read(fpath, patcher):
#     # Create mock file hello.txt with contents "Hello, World!\n"
#     patcher.fs.create_file(fpath, contents="Hello, World!\n")
#
#     # Create the file object
#     frec = fr.FileRecord.from_file(fpath)
#
#     # Assert File Record has correct attributes
#     assert frec.name == "hello.txt"
#     assert frec.hash == b"b1946ac92492d2347c6235b4d2611184"
#     # assert frec is None
