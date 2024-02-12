import hashlib
import os


def hash_file(fpath: str, hash_func_str: str = "md5") -> str:
    """
    Hashes a file using the specified string of the algorithm.
    """
    if not os.path.isfile(fpath):
        raise ValueError(f"Path {fpath} not a regular or linked file")

    h = hashlib.new(hash_func_str)
    with open(fpath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest()


def readdir(dpath: str) -> Directory:
    pass
