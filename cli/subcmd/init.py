import argparse
import os

# from cli.defaults import MAX_WIDTH, MAX_HELP_POSITION, INDENT_INCREMENT
from cli.help_formatter import HelpFormatter

SUBCMD_DESCRIPTION = """Initialize a new scout repository.
This command will create a new `.scout.db` file for the repository.
Then initialize the sqlite database with the necessary tables.
The repository will be empty until you sync file metadata to it.
If the root directory is not specified, the repository will be created in the current directory.
If the destination directory is not specified, the repository will be created in the root directory and named `.scout.db`.
"""


def add_subcommand(subparsers: "argparse._SubParsersAction") -> None:
    """
    Add the 'init' subcommand to the given subparsers.

    This subcommand initializes a new scout repository. It creates a new `.scout.db` file
    for the repository and initializes the SQLite database with the necessary tables.
    The repository will be empty until you sync file metadata to it.

    Parameters:
    subparsers (argparse._SubParsersAction): The subparsers action object to which the
                                             'init' subcommand will be added.
    """
    parser = subparsers.add_parser(
        "init",
        help="Initialize a new scout repository.",
        description=SUBCMD_DESCRIPTION,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "target",
        nargs="?",
        default=".",
        help="The target root directory for the repo. Defaults to the current directory.",
    )
    parser.add_argument(
        "-r",
        "--repo",
        type=str,
        help="The path where the repo will be stored. Can be separate from the root directory. Defaults to the root directory.",
    )
    parser.set_defaults(func=handle_subcommand)


def handle_subcommand(args):
    """
    Handle the 'init' subcommand.

    This function initializes the repository by creating a new `.scout.db` file
    and setting up the necessary tables in the SQLite database. The repository
    will be empty until file metadata is synced to it.

    Parameters:
    args (argparse.Namespace): The parsed arguments. Should contain 'target' for the root
                               directory and 'repo' for the repository path.
    """
    target = args.target
    repo = args.repo
    if repo is None:
        repo = f"{target}/.scout.db"
    # TODO: Implement the initialization of the project
    # Implementation will be added later
    print(f"Initializing repo for {target} with repo stored at {repo}")
