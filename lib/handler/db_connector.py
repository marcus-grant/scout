from contextlib import contextmanager
import os
from pathlib import PurePath as PP
import sqlite3 as sql
from typing import Optional, Union, Generator, List

from lib.model.dir import Dir


class DBConnectorError(Exception):
    """Base class for DBConnector errors."""

    pass


class DBNotInDirError(DBConnectorError):
    """Raised when a path for a Scout database doesn't have a parent directory."""

    def __init__(self, path):
        self.message = f"Given path:\n{path}\nmust exist inside a valid directory."
        super().__init__(self.message)


class DBFileOccupiedError(DBConnectorError):
    """Raised when a path for a potential Scout database file is already
    occupied by a non-scout db file."""

    def __init__(self, path):
        self.message = "Given path:\n"
        self.message += f"{path}\nis occupied by a non-scout database file."
        super().__init__(self.message)


# TODO: Rename Root to Target to better fit CLI commands
class DBRootNotDirError(DBConnectorError):
    """Raised when the root path for a Scout database is not a directory."""

    def __init__(self, root):
        self.message = "Given root path:\n"
        self.message += f"{root}\nis not a valid directory."
        super().__init__(self.message)


class DBNoFsMetaTableError(DBConnectorError):
    """Raised when the fs_meta table is missing from the database."""

    def __init__(self):
        self.message = "The fs_meta table is missing from the database."
        super().__init__(self.message)


class DBTargetPropMissingError(DBConnectorError):
    """Raised when the target property is missing from the fs_meta table."""

    def __init__(self):
        self.message = "The target property is missing from the fs_meta table."
        super().__init__(self.message)


class DBPathOutsideTargetError(DBConnectorError):
    """Raised when a path is outside the target directory tree of the database."""

    def __init__(self, path, target):
        self.message = f"Given path:\n{path}\n"
        self.message += f"is outside of the target directory:\n{target}\n"
        super().__init__(self.message)


class DBPathNotSupportedError(DBConnectorError):
    """Raised when something about the path syntax is not supported."""

    def __init__(self, path):
        if ".." in str(path):
            self.message = f"Relative ancestor paths (..) of {path} not supported."
        else:
            self.message = f"Path {path} not supported."
        super().__init__(self.message)


class DBConnector:
    """
    A class for managing connections to a scout database file and
    maintaining consistent paths to and from the database.
    This class focuses on the fs_meta table.
    """

    path: PP  # Path to the db file
    root: PP  # Path to the relative root of the db paths inside repos

    @classmethod
    def is_db_file(cls, path) -> bool:
        """
        Check if the file at the given path is a SQLite database file.

        Args:
            path (str or Path): The path to the file to check.

        Returns:
            bool: True if the file is a SQLite database file, False otherwise.
        """
        with open(path, "rb") as f:
            header = f.read(16)
            return header == b"SQLite format 3\x00"

    @classmethod
    def is_scout_db_file(cls, path) -> bool:
        """
        Check if the file at the given path is a scout database file.

        Args:
            path (str or Path): The path to the file to check.

        Returns:
            bool: True if the file is a scout database file, False otherwise.
        """
        if not cls.is_db_file(path):
            return False
        with sql.connect(path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [t[0] for t in cursor.fetchall()]
            if "fs_meta" not in tables:
                return False
            cursor.execute("SELECT property FROM fs_meta;")
            for row in cursor.fetchall():
                if row[0] == "root":
                    return True
            return False

    @classmethod
    def validate_arg_path(cls, path: Union[PP, str]) -> PP:
        """
        Validate and normalize the 'path' argument for the constructor.

        Args:
            path (Union[PP, str]): The path to validate.

        Returns:
            PP: The validated and normalized path.

        Raises:
            TypeError: If the path is not a string or PurePath.
            DBNotInDirError: If the parent directory of the path does not exist.
            DBFileOccupiedError: If the path exists but is not a scout database file.
        """
        if isinstance(path, str):
            result = PP(path)
        elif isinstance(path, PP):
            result = path
        else:
            raise TypeError(f"path {path} must be a PurePath or str")

        if not os.path.isdir(result.parent):
            raise DBNotInDirError(str(path))
        if os.path.exists(result) and not cls.is_scout_db_file(result):
            raise DBFileOccupiedError(str(path))

        return result

    # NOTE: If we ever implement remote fs or db handling,
    # the raise here needs to be handled accordingly to the remote case.
    # Same for sneaker net scenarios.
    @classmethod
    def validate_arg_root(cls, path: PP, root: Optional[Union[PP, str]]) -> PP:
        """
        Validate and normalize the 'root' argument for the constructor.

        Args:
            path (PP): The path to the database file.
            root (Optional[Union[PP, str]]): The root path to validate.

        Returns:
            PP: The validated and normalized root path.

        Raises:
            TypeError: If the root is not None, a string, or PurePath.
            DBRootNotDirError: If the root is not a valid directory.
        """
        if root is None:
            result = path.parent
        elif isinstance(root, str):
            result = PP(root)
        elif isinstance(root, PP):
            result = root
        else:
            raise TypeError(f"root must be PurePath or str, given {type(root)}")

        if not os.path.isdir(result):
            raise DBRootNotDirError(str(root))

        return result

    # TODO: Needs to rename property.root to property.target to match CLI commands
    @classmethod
    def read_root(cls, path: PP) -> PP:
        """
        Read the 'root' property from the fs_meta table in the database.

        Args:
            path (PP): The path to the database file.

        Returns:
            PP: The root property value as a PurePath.

        Raises:
            DBNoFsMetaTableError: fs_meta table is missing from the database.
            DBTargetPropMissingError: target property is not in the fs_meta table.
        """
        with sql.connect(path) as conn:
            c = conn.cursor()
            # Check if fs_meta table exists
            c.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = c.fetchall()
            if ("fs_meta",) not in tables:
                raise DBNoFsMetaTableError()
            c.execute("SELECT value FROM fs_meta WHERE property='root';")
            res = c.fetchone()
            if res is None:
                raise DBTargetPropMissingError()
            return PP(res[0])

    @classmethod
    def init_db(cls, path: PP, root: PP) -> None:
        """
        Initialize a SQLite database file with the fs_meta table.

        Args:
            path (PP): The path to the database file.
            root (PP): The root directory path to store in the fs_meta table.
        """
        with sql.connect(path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS fs_meta (
                            property TEXT PRIMARY KEY, value TEXT);""")
            conn.execute(
                f"INSERT INTO fs_meta (property, value) VALUES ('root', '{root}');"
            )
            conn.commit()

    def __init__(
        self, path: Union[PP, str], root: Optional[Union[PP, str]] = None
    ) -> None:
        """
        Initialize the DBConnector with the given path and root.

        Args:
            path (Union[PP, str]): The path to the database file.
            root (Optional[Union[PP, str]]): The root directory path.

        Raises:
            ValueError: If the path exists but is not a scout database file.
        """
        self.path = self.validate_arg_path(path)
        self.root = self.validate_arg_root(self.path, root)

        if not os.path.exists(self.path):
            self.init_db(self.path, self.root)
        elif self.is_scout_db_file(self.path):
            self.root = DBConnector.read_root(self.path)
        else:
            raise DBFileOccupiedError(str(self.path))

    ### Path Utility Methods
    def normalize_path(self, denormalized_path: Union[Dir, PP, str]) -> PP:
        """
        Normalize a path relative to the root directory this database tracks.
        Relative paths are kept relative on
        assumption it's relative to root already & thus already normalized.
        Args:
            denormalized_path (Union[PurePath, str]): The path to normalize.
        Returns:
            PP: The normalized PurePath relative to the root directory.
        Raises:
            ValueError: If the path is not relative to the root directory.
        """
        # Check for unresolvable path syntax
        if ".." in str(denormalized_path):
            raise DBPathNotSupportedError(denormalized_path)
        # Coerce to PP type
        path = None
        if isinstance(denormalized_path, Dir):
            path = denormalized_path.path
        elif isinstance(denormalized_path, str):
            path = PP(denormalized_path)
        elif isinstance(denormalized_path, PP):
            path = denormalized_path
        else:
            raise TypeError(f"path {denormalized_path} must be a Dir, PurePath or str")

        # If relative prepend the root so we can use pathlib's relative_to
        if not path.is_absolute():
            path = self.root / path
        try:  # Check for paths outside target when conducting relative_to
            path = path.relative_to(self.root)  # Finally normalize
        except ValueError as e:
            raise DBPathOutsideTargetError(path, self.root) from e
        return path

    def denormalize_path(self, normalized_path: Union[PP, str]) -> PP:
        """
        Denormalize a path relative to the root directory this database tracks.
        Basically appending the normalized path to the root directory.
        Args:
            normalized_path (Union[PurePath, str]): The path to denormalize.
        Returns:
            PP: The denormalized PurePath relative to the root directory.
        Raises:
            ValueError: If the path is not relative to the root directory.
        """
        if ".." in str(normalized_path):
            msg = f"Relative ancestor paths (..) of {normalized_path} not supported."
            # TODO: Needs own DBConnectorError subclass
            raise ValueError(msg)
        path = PP(normalized_path)
        if path.is_absolute():
            # Raise if path outside root
            try:
                path = path.relative_to(self.root)
            except:  # noqa
                # TODO: Needs own DBConnectorError subclass
                raise ValueError(f"{path} is outside of {self.root}")
        path = self.root / path
        return path

    def ancestor_paths(self, path: Union[PP, str]) -> List[PP]:
        """
        Generate all ancestor paths of a given path.
        Args:
            path (Union[PurePath, str]): The path to generate ancestors of.
        Yields:
            PP: The ancestor paths of the given path ordered from root to path.
        """
        current = self.normalize_path(path)
        ancestors = []
        while current != PP():
            ancestors.append(current)
            current = current.parent
        return ancestors[::-1]

    ### Connect methods
    # TODO: Come up with way to close cleanly if leaks are a concern
    @contextmanager
    def connect(self) -> Generator[sql.Connection, None, None]:
        """"""
        with sql.connect(self.path) as conn:
            yield conn

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.
        Args:
            table (str): The name of the table to check.
        Returns:
            bool: True if the table exists, False otherwise.
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            return (table_name,) in tables
