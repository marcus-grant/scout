# test/e2e/test_command.py
"""Pin the subcommand error boundary through the CLI, as a user sees it.
Author: Marcus
Created: 2026-09-10
License: AGPL-3.0-or-later
"""

from collections.abc import Callable

import click
import pytest
from click.testing import CliRunner

import scout.lib.error as Err
from scout.cli.command import ISSUES_URL, scout_command


def mk_group(fn: Callable[[], None]) -> click.Group:
    """Return a throwaway group with fn registered as the command 'boom'."""
    group = click.Group()
    group.add_command(scout_command("boom")(fn))
    return group


def _boom() -> None:
    raise RuntimeError("kaput")


def _domain() -> None:
    raise Err.NotAManifest("foobar")


@pytest.mark.e2e
class TestScoutCommand:
    """scout_command maps lib errors to messages and rewraps the unknown."""

    def test_unknown_error_reports_traceback_and_url(self) -> None:
        """A plain RuntimeError exits 70 with the traceback and the issues URL."""
        result = CliRunner().invoke(mk_group(_boom), ["boom"])
        assert result.exit_code == 70
        for word in ("Traceback", "RuntimeError: kaput", ISSUES_URL):
            assert word in result.output, word

    def test_domain_error_reports_message_only(self) -> None:
        """An Err.ScoutDomain exits 1 with its message and no traceback."""
        result = CliRunner().invoke(mk_group(_domain), ["boom"])
        assert result.exit_code == 1
        assert "foobar" in result.output
        assert "traceback" not in result.output.lower()
