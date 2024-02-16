# TODO: Come up with plan on how or if to handle file creation times
# TODO: Add magic byte class member that can identify file types
from attrs import define  # , field
from datetime import date, time, datetime
import os
from typing import Optional, Union


@define
class FsFile:
    """
    Represents metadata about a file.
    """

    name: str
    size: int
    mtime: datetime
    hash_str: Optional[str] = None
    hash_bytes: Optional[bytes] = None

    @classmethod
    def from_path(cls, path: Union[str, os.PathLike]) -> "FsFile":
        """
        Creates a File object from a file path.
        """
        if not os.path.isfile(path):
            raise ValueError(f"Path {path} not a regular or linked file")
        # Get filename from path
        name = os.path.basename(path)
        size = os.path.getsize(path)
        mtime = datetime.fromtimestamp(os.path.getmtime(path))
        return cls(name=name, size=size, mtime=mtime)
