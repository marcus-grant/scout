from typing import Optional

from lib.handler.db_connector import DBConnector, DBFileOccupiedError


class ScoutManagerError(Exception):
    pass


class ScoutAlreadyInitError(ScoutManagerError):
    """Should be raised when trying to initialize a pre-existing scout repository."""

    def __init__(self, path: str):
        msg = f"Scout DB file @ {path} already exists!"
        super().__init__(msg)


class ScoutManager:
    @classmethod
    def init_db(cls, repo: str, target: Optional[str]) -> DBConnector:
        """Initialize a scout repository without starting a scout session.
        Primarily used in cli.subcmd.init.handle_subcommand."""
        try:  # First determine if init'ing a pre-existing scout repository
            is_scout = DBConnector.is_scout_db_file(repo)
        except FileNotFoundError:
            is_scout = False
        if is_scout:  # If so raise the ScoutManagerError
            raise ScoutAlreadyInitError(repo)
        db = DBConnector(repo, target)  # Otherwise init with DBConnector
        return db  # Return DBConnector object to help interface report on it

    def __init__(self, repo: str, target: Optional[str]):
        self.db = DBConnector(repo, target)
