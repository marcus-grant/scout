import argparse
from sys import stderr

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


def main():
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
    args, unknown = parser.parse_known_args()

    # Handle unknown arguments
    if unknown:
        msg = f"Unknown arguments: {unknown}\n"
        msg += "Please use a subcommand to interact with the CLI instead.\n"
        print(msg, file=stderr)
        parser.print_help(stderr)
        exit(2)

    # Check if a command was given
    if args.command is None:
        # If none given we are in the main command scope - later the TUI
        if any(arg in ("-h", "--help") for arg in parser._get_args()):
            parser.print_help()
            exit(0)
        else:
            msg = "Warning: TUI is not implemented yet.\n"
            msg += "Please use a subcommand to interact with the CLI instead.\n"
            print(msg, file=stderr)
            parser.print_help(stderr)
            exit(2)
    else:
        # Call the function associated with the command if needed
        args.func(args, parser.print_help)


if __name__ == "__main__":
    main()
