# TODO: Add crc32
from attrs import define
import hashlib
import os
from typing import Optional


@define
class HashMD5:
    """
    Represents a hash of a file in the MD5 algorithm.
    Provides member & class methods to create and compare hashes.
    """

    bytes: Optional[bytes]
    hex: Optional[str]

    @classmethod
    def from_path(cls, path: str, chunk_size: int = 4096) -> "HashMD5":
        """
        Creates a HashMD5 object from a file path.
        It hashes the file using the MD5 algorithm.
        Factory to create object with both kinds of hash representations.
        """
        if not os.path.isfile(path):
            raise ValueError(
                f"Path {path} not a regular or linked file (inside HashMD5.from_path)"
            )
        h = hashlib.md5()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                h.update(chunk)
        return cls(bytes=h.digest(), hex=h.hexdigest())
