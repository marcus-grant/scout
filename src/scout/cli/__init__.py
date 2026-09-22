# src/scout/cli/__init__.py
"""Click entry point for the scout command line adapter.

Author: Marcus
Created: 2026-09-04
License: AGPL-3.0-or-later
"""

import click

NAME = "scout"
VERSION = "0.0.1"
VERSION_STR = f"{NAME} {VERSION}"
DESCRIPTION = "Inventory a file tree into a SQLite manifest keyed on content hash."

CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
    "max_content_width": 88,
}


@click.group(name=NAME, help=DESCRIPTION, context_settings=CONTEXT_SETTINGS)
@click.version_option(VERSION, "-v", "--version", message=VERSION_STR)
def main() -> None:
    """Root command group; subcommands register on it in cli/subcmd/."""


# Add commands with imports below click main
from scout.cli.subcmd.init import init
from scout.cli.subcmd.scan import scan

main.add_command(init)
main.add_command(scan)
