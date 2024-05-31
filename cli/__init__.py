import argparse

from cli.help_formatter import HelpFormatter
# import cli.subcmd.init

# Text Constants for Argparse
NAME = "scout"
# VERSION = "0.0.1"
DESCRIPTION = """Scout is a file metadata management tool.
This is its CLI to interact with the metadata database &
perform various operations on the files described in the database.
This makes life easier when tracking & managing files on different devices.
"""


def main():
    # Init argparser
    parser = argparse.ArgumentParser(
        description=DESCRIPTION,
        formatter_class=HelpFormatter,
        prog=NAME,
    )
    # Parse arguments
    args = parser.parse_args()

    # Check if a command was given
    if args.command is None:
        parser.print_help()
    else:
        # Call the function associated with the command
        args.func(args)


if __name__ == "__main__":
    main()
