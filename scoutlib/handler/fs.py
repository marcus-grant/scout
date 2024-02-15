# TODO: Make a Hash dataclass to store and cast to different types
import hashlib
import os
from typing import Any, Union


def hash_md5(path: Union[str, os.PathLike], chunk_size: int = 4096) -> Any:
    """
    Hashes a file using the MD5 algorithm.
    Outputs as a hashlib._Hash object.
    Use .hexdigest() to get the string representation.
    Use .digest() to get the bytes representation.
    """
    if not os.path.isfile(path):
        raise ValueError(f"Path {path} not a regular or linked file")
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h


def hash_md5_str(path: Union[str, os.PathLike], chunk: int = 4096) -> str:
    """
    Hashes a file using the MD5 algorithm.
    Outputs as a string of hexadecimals.
    """
    return hash_md5(path, chunk).hexdigest()


def hash_md5_bytes(path: Union[str, os.PathLike], chunk: int = 4096) -> bytes:
    """
    Hashes a file using the MD5 algorithm.
    Outputs as a bytes object.
    """
    return hash_md5(path, chunk).digest()
