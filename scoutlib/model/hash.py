# TODO: Add crc32
import hashlib
import os
from typing import Optional


class HashMD5:
    """
    Represents a hash of a file in the MD5 algorithm.
    Provides member & class methods to create and compare hashes.
    """

    bin: bytes

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
        return cls(h.digest())

    def __init__(self, bin: Optional[bytes] = None, hex: Optional[str] = None):
        """
        Initialize a HashMD5 object with either bytes or hex string.
        """
        if bin is not None:
            self.bin = bin
        elif hex is not None:
            self.bin = bytes.fromhex(hex)
        else:
            raise ValueError(
                "Either bin or hex must be provided to HashMD5 constructor."
            )

    def __str__(self) -> str:
        """
        Returns the hex representation of the hash.
        """
        return self.bin.hex()

    def __repr__(self) -> str:
        """
        Returns the hex representation of the hash.
        """
        return f"HashMD5(hex={self.__str__()})"

    @property
    def hex(self) -> str:
        """
        Returns the hex representation of the hash.
        """
        return self.bin.hex()
