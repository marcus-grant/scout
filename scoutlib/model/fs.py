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

    def parent_path(self) -> str:
        return os.path.dirname(self.path)

    def find_subdirs(self, all_dirs: list["Directory"]) -> list["Directory"]:
        return [d for d in all_dirs if d.parent_path() == self.path]

    def find_files(self, all_files: list["File"]) -> list["File"]:
        return [f for f in all_files if f.parent.path == self.path]


@attrs.define(kw_only=True)
class File:
    """Represents a single file"""

    name: str
    parent: Directory
    id: Optional[int] = attrs.field(default=None)

    # TODO: Add ability to search Directories for parent
    @classmethod
    def from_path(cls, path: PathLike, **kwargs) -> "File":
        name = os.path.basename(path)
        parent = Directory.from_path(os.path.dirname(path))
        id = kwargs.get("id", None)
        return cls(name=name, parent=parent, id=id)

    def path(self) -> str:
        return os.path.join(self.parent.path, self.name)


# NOTE: Don't know if this is best approach, str handling paths might be simpler
# @attrs.define(kw_only=True)
# class DirTreeNode:
#     """
#     Represents the node of a directory tree.
#     Use this to help with the relationships between directories.
#     While keeping Directories & Files available as flat colelctions.
#     """
#
#     dir: Directory
#     parent: Optional["DirTreeNode"] = attrs.field(default=None)
#     subdirs: list["DirTreeNode"] = attrs.field(factory=list)
#     depth: int = attrs.field(default=0)
#
#     @classmethod
#     def mkroot(cls, dir: Directory) -> "DirTreeNode":
#         return cls(dir=dir)
#
#     def mksubdir(self, subdir: Directory) -> None:
#         subdepth = self.depth + 1
#         subnode = DirTreeNode(dir=subdir, parent=self, depth=subdepth)
#         self.subdirs.append(subnode)
#
#     # def rmsubdir(self, subdir: Directory) -> None:
#     #     for i, node in enumerate(self.subdirs):
#     #         if node.dir == subdir:
#     #             del self.subdirs[i]
#     #             return
#
#     # def cd( self, rel_path: str) -> Optional["DirTreeNode"]:
#     #     if rel_path == "":
#     #         return self
#     #     for node in self.subdirs:
#     #         if rel_path in node.dir.path:
#     #             return node.cd(rel_path)
#     #     return None
