# TODO: Figure out how to make attrs properly validate optional ints above -1
import attrs

# from datetime import datetime as dt
import os
from pathlib import Path
from typing import Any, Optional

from scoutlib.model.hash import HashMD5

PathLike = os.PathLike


class Validator:
    @staticmethod
    def id(value: Optional[int]):
        if value is None:
            return
        if not isinstance(value, int):
            raise ValueError(
                (f"Invalid Directory.id value: {value}, must be int or None")
            )
        if value < 0:
            raise ValueError(f"Invalid Directory.id value: {value}, must be >= 0")


@attrs.define
class Directory:
    """Represents a single directory"""

    name: str = attrs.field(default="")  # Base name of the directory
    path: str = attrs.field(default="")  # Full path to the directory
    id: Optional[int] = attrs.field(default=None)

    @classmethod
    def from_path(cls, path: str, **kwargs) -> "Directory":
        name = os.path.basename(path)
        id = kwargs.get("id", None)
        return cls(name=name, path=path, id=id)


@attrs.define(kw_only=True)
class File:
    """Represents a single file"""

    name: str
    parent: Directory
    id: Optional[int] = attrs.field(default=None)

    @classmethod
    def from_path(cls, path: PathLike, **kwargs) -> "File":
        name = os.path.basename(path)
        parent = Directory.from_path(os.path.dirname(path))
        id = kwargs.get("id", None)
        return cls(name=name, parent=parent, id=id)

    def path(self) -> str:
        return os.path.join(self.parent.path, self.name)
