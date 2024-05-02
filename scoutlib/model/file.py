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
        md5: Optional[HashMD5] = None,  # Assuming HashMD5 is defined elsewhere
        mtime: Optional[dt] = None,  # Assuming dt is an alias for datetime
        update: Optional[dt] = None,
    ):
        if not self._validate_init_args(path=path, parent=parent, name=name):
            msg = "File.__init__ requires either path or (parent and name) args."
            raise TypeError(msg)

        if name:  # If ane is given assign it
            self.name = name
        if parent:  # Same for parent
            self.parent = PP(parent)
        if path:  # If path is given it takes precedence by override
            path = PP(path)
            self.parent = path.parent
            self.name = path.name

        # Initialize all other attributes
        self.id = id
        self.md5 = md5
        self.mtime = mtime
        self.update = update

    @classmethod
    def _validate_init_args(
        cls,
        path: Optional[Union[str, PP]] = None,
        parent: Optional[Union[str, PP]] = None,
        name: Optional[str] = None,
    ) -> bool:
        """Validates the arguments passed to the constructor"""
        return (path is not None) or ((parent is not None) and (name is not None))

    @property
    def path(self) -> PP:
        return self.parent / self.name
