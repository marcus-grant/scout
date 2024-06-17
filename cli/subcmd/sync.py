import argparse
import os
import sys

from cli.help_formatter import HelpFormatter
from lib.scout_manager import ScoutManager

SUBCMD_DESCRIPTION = """Synchronize filesystem metadata of
the target directory tracked by the scout repository."""


def add_subcommand(subparsers: "argparse._SubParsersAction") -> None:
    """
    Add the 'sync' subcommand to the given subparsers.
    This subcommand synchronizes the filesystem metadata of the target directory
    tracked by the scout repository.
    Parameters:
    subparsers (argparse._SubParsersAction): The subparsers action object to which the
                                             'sync' subcommand will be added.
    """
    parser = subparsers.add_parser(
        "sync",
        help="Synchronize filesystem metadata of the target directory with repo.",
        description=SUBCMD_DESCRIPTION,
        formatter_class=HelpFormatter,
    )
    parser.add_argument(
        "repo",
        nargs=1,
        default=".",
        type=str,
        help="The scout repo to sync with.",
    )
    parser.set_defaults(func=handle_subcommand)


def handle_subcommand(args: argparse.Namespace) -> int:
    """
    Handle the 'sync' subcommand.
    This function is called when the 'sync' subcommand is used.
    It synchronizes the filesystem metadata of the target directory tracked by the scout repository.
    Parameters:
    args (argparse.Namespace): The parsed arguments for the 'sync' subcommand.
    Returns:
    int: The return code of the operation.
    """
    repo = args.repo[0]
    scout_manager = ScoutManager(repo)
    try:
        scout_manager.sync()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    return 0
