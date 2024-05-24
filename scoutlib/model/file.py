from datetime import datetime as dt
from pathlib import PurePath as PP
from typing import Optional, Union

from scoutlib.model.hash import HashMD5


class File:
    """
    Represents a single file on the filesystem or recorded in the database.
    """

    dir_path: PP
    name: str

    def __init__(
        self,
        path: Optional[Union[str, PP]] = None,
        dir_path: Optional[Union[str, PP]] = None,
        name: Optional[str] = None,
        dir_id: Optional[int] = None,
        id: Optional[int] = None,
        size: Optional[int] = None,
        mtime: Optional[dt] = None,  # Assuming dt is an alias for datetime
        md5: Optional[HashMD5] = None,  # Assuming HashMD5 is defined elsewhere
        updated: Optional[dt] = None,
    ):
        if not self._validate_init_args(path=path, dir_path=dir_path, name=name):
            msg = "File.__init__ requires either path or (dir_path and name) args."
            raise TypeError(msg)

        if name:  # If ane is given assign it
            self.name = name
        if dir_path:  # Same for dir_path
            self.dir_path = PP(dir_path)
        if path:  # If path is given it takes precedence by override
            path = PP(path)
            self.dir_path = path.parent
            self.name = path.name

        # Initialize all other attributes
        self.id = id
        self.dir_id = dir_id
        self.size = size
        self.md5 = md5
        self.mtime = mtime
        self.updated = updated

    @classmethod
    def _validate_init_args(
        cls,
        path: Optional[Union[str, PP]] = None,
        dir_path: Optional[Union[str, PP]] = None,
        name: Optional[str] = None,
    ) -> bool:
        """Validates the arguments passed to the constructor"""
        return (path is not None) or ((dir_path is not None) and (name is not None))

    @property
    def path(self) -> PP:
        return self.dir_path / self.name
