from datetime import datetime as dt
from pathlib import PurePath as PP
from typing import Optional, Union

from scoutlib.model.hash import HashMD5


class File:
    """
    Represents a single file on the filesystem or recorded in the database.
    """

    def __init__(
        self,
        path: Union[str, PP],
        dir_id: Optional[int] = None,
        id: Optional[int] = None,
        size: Optional[int] = None,
        mtime: Optional[dt] = None,  # Assuming dt is an alias for datetime
        md5: Optional[HashMD5] = None,  # Assuming HashMD5 is defined elsewhere
        updated: Optional[dt] = None,
    ):
        # Initialize all other attributes
        self.path = PP(path)
        self.id = id
        self.dir_id = dir_id
        self.size = size
        self.md5 = md5
        self.mtime = mtime
        self.updated = updated

    def __eq__(self, value: object, /) -> bool:
        if not isinstance(value, File):
            return NotImplemented
        return (
            self.path == value.path
            and self.id == value.id
            and self.dir_id == value.dir_id
            and self.size == value.size
            and self.md5 == value.md5
            and self.mtime == value.mtime
            and self.updated == value.updated
        )
