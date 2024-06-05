from typing import Optional

from lib.handler.db_connector import DBConnector, DBFileOccupiedError


class ScoutManager:
    @classmethod
    def init_db(cls, repo: str, target: Optional[str]) -> DBConnector:
        """Initialize a scout repository without starting a scout session.
        Primarily used in cli.subcmd.init.handle_subcommand."""
        db = DBConnector(repo, target)
        return db

    def __init__(self, repo: str, target: Optional[str]):
        self.db = DBConnector(repo, target)
