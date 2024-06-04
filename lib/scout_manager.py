from typing import Optional

from lib.handler.db_connector import DBConnector


class ScoutManager:
    def __init__(self, repo: str, target: Optional[str]):
        self.db = DBConnector(repo, target)
