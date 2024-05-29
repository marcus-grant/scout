# TODO: Figure out how to make attrs properly validate optional ints above -1
# TODO: Should we even use attrs?
import attrs

# from datetime import datetime as dt
import os
from pathlib import Path, PurePath
from typing import Any, Optional, Union

from lib.model.hash import HashMD5

PathLike = os.PathLike


class Validator:
    @staticmethod
    def id(value: Optional[int]):
        if value is None:
            return
        if not isinstance(value, int):
            raise ValueError((f"Invalid Dir.id value: {value}, must be int or None"))
        if value < 0:
            raise ValueError(f"Invalid Dir.id value: {value}, must be >= 0")


class Dir:
    # TODO: Since name depends on path, should be changed to computed field
    # TODO: Refactor to use path objects along with str to path
    """Represents a single directory"""

    path: PurePath
    id: Optional[int]

    def __init__(self, path: Union[str, PurePath], id: Optional[int] = None):
        self.path: PurePath = PurePath(path) if isinstance(path, str) else path
        self.id = id

    @classmethod
    def from_path(cls, path: str, id: Optional[int] = None) -> "Dir":
        return cls(path=path, id=id)

    @property
    def name(self) -> Optional[str]:
        # raise LookupError(f"Dir.name = {self.path.name}")
        return self.path.name

    @property
    def parent(self) -> "Dir":
        return Dir(self.path.parent)

    def find_subdirs(self, all_dirs: list["Dir"]) -> list["Dir"]:
        return [d for d in all_dirs if d.path.parent == self.path]

    def find_files(self, all_files: list["File"]) -> list["File"]:
        return [f for f in all_files if f.parent.path == self.path]

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Dir):
            return False
        for attr in ["name", "path", "id"]:
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __repr__(self) -> str:
        return f"Dir(path={self.path}, id={self.id})"


@attrs.define(kw_only=True)
class File:
    """Represents a single file"""

    name: str
    parent: Dir
    id: Optional[int] = attrs.field(default=None)

    # TODO: Add ability to search Directories for parent
    @classmethod
    def from_path(cls, path: PathLike, **kwargs) -> "File":
        name = os.path.basename(path)
        parent = Dir.from_path(os.path.dirname(path))
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
#     dir: Dir
#     parent: Optional["DirTreeNode"] = attrs.field(default=None)
#     subdirs: list["DirTreeNode"] = attrs.field(factory=list)
#     depth: int = attrs.field(default=0)
#
#     @classmethod
#     def mkroot(cls, dir: Dir) -> "DirTreeNode":
#         return cls(dir=dir)
#
#     def mksubdir(self, subdir: Dir) -> None:
#         subdepth = self.depth + 1
#         subnode = DirTreeNode(dir=subdir, parent=self, depth=subdepth)
#         self.subdirs.append(subnode)
#
#     # def rmsubdir(self, subdir: Dir) -> None:
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
