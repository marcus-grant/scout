from attrs import define
from datetime import datetime
import os
from typing import Optional, Union

from scoutlib.model.fs_file import FsFile


@define
class FsDir:
    """
    Represents metadata about a directory.
    """

    name: str
    path: Union[str, os.PathLike]

    @classmethod
    def from_path(cls, path: Union[str, os.PathLike]) -> "FsDir":
        """
        Creates a Dir object from a directory path.
        """
        if not os.path.isdir(path):
            raise ValueError(f"Path {path} not a directory")
        # Get dirname from path
        name = os.path.basename(path)
        return cls(name=name, path=path)
