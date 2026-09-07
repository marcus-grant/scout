# TODO: Init needs to check for preexisting db & use future CfgRepo to read it before other init steps.
#       This will also require extra rewrites of init.
from pathlib import PurePath as PP

from scout.lib.repo.db_connector import DBConnector
from scout.lib.repo.dir_repo import DirRepo
from scout.lib.repo.file_repo import FileRepo


class DBManager:
    """
    The root class for managing the sqlite database.
    Sets up the database where it is configured to be.
    Then it sets up the Repo classes for their associated tables.
    """

    db: DBConnector  # Database connector class
    dir_repo: DirRepo  # Repo class for the Dir table
    file_repo: FileRepo  # Repo class for the File table

    # TODO: Modify {Dir,File}Repo to accept DBConnector instead of path and root & its methods replace their versions.
    def __init__(
        self,
        path: PP | str,
        root: PP | str | None = None,
    ):
        # Initialize and assign DBConnector
        self.db = DBConnector(path, root)

        # Initialize Repo members
        # self.dir_repo = DirRepo(self.db.path, self.db.root)
        # self.file_repo = FileRepo(self.db.path, self.db.root)
