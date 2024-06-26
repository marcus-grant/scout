import argparse
import sys
from typing import List, Optional

from cli.defaults import MAX_WIDTH, MAX_HELP_POSITION, INDENT_INCREMENT
from cli.help_formatter import HelpFormatter
import cli.subcmd.init

# Text Constants for Argparse
NAME = "scout"
VERSION = "0.0.1"
VERSION_STR = f"{NAME} {VERSION}"
DESCRIPTION = """Scout is a file metadata management tool.
This is its CLI to interact with the metadata database and
perform various operations on the files described in the database.
It also helps analyze the files in the database on and off the filesystem.
"""


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) > 0:
        if "scout" in argv[0] or "./" in argv[0]:
            argv = argv[1:]

    formatter = HelpFormatter(
        prog=NAME,
        indent_increment=INDENT_INCREMENT,
        max_help_position=MAX_HELP_POSITION,
        width=MAX_WIDTH,
    )
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=(lambda prog: formatter),
    )
    formatter.set_description(DESCRIPTION)

    # Add root level arguments here if needed (no -h/--help as it is default)
    parser.add_argument("-v", "--version", action="version", version=VERSION_STR)

    subparsers = parser.add_subparsers(title="subcommands", dest="command")

    # Add more subcommands here as needed
    cli.subcmd.init.add_subcommand(subparsers)

    # Parse arguments
    # args = parser.parse_args()
    args, unknown = parser.parse_known_args(argv)

    # Handle unknown arguments
    if unknown:
        msg = f"Unknown arguments: {unknown}\n"
        msg += "Please use a subcommand to interact with the CLI instead.\n"
        print(msg, file=sys.stderr)
        parser.print_help(sys.stderr)
        return 2

    # Check if a command was given
    if args.command is None:
        # If none given we are in the main command scope - later the TUI
        # TODO: Does this even get called?
        if any(arg in ("-h", "--help") for arg in parser._get_args()):
            parser.print_help()
            return 0
        else:
            msg = "Warning: TUI is not implemented yet.\n"
            msg += "Please use a subcommand to interact with the CLI instead.\n"
            print(msg, file=sys.stderr)
            parser.print_help(sys.stderr)
            return 2
    else:
        # Call the function associated with the command if command parsed
        return args.func(args, parser.print_help)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
