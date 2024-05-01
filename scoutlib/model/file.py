from datetime import datetime as dt
from pathlib import PurePath as PP
from typing import Optional, Union

from scoutlib.model.hash import HashMD5


class File:
    """
    Represents a single file on the filesystem or recorded in the database.
    """

    parent: PP
    name: str

    def __init__(
        self,
        path: Optional[Union[str, PP]] = None,
        parent: Optional[Union[str, PP]] = None,
        name: Optional[str] = None,
        id: Optional[int] = None,
        md5: Optional[HashMD5] = None,
        mtime: Optional[dt] = None,
        update: Optional[dt] = None,
    ):
        if not self.validate_init_args(path=path, parent=parent, name=name):
            msg = "File.__init__ requires either path or (parent and name) args."
            raise TypeError(msg)
        if path is not None:
            path = PP(path)
            self.parent = path.parent
            self.name = path.name

    @classmethod
    def validate_init_args(
        cls,
        path: Optional[Union[str, PP]] = None,
        parent: Optional[Union[str, PP]] = None,
        name: Optional[str] = None,
    ) -> bool:
        """Validates the arguments passed to the constructor"""
        return (path is not None) or (parent is not None) and (name is not None)
