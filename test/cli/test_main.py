# test/cli/test_main.py
"""Pin the CLI entry point: usage, version, and bad-flag exit codes.

Author: Marcus
Created: 2026-09-04
License: AGPL-3.0-or-later
"""

import pytest
from click.testing import CliRunner

from cli import DESCRIPTION, NAME, VERSION_STR, main


@pytest.mark.e2e
@pytest.mark.parametrize("option", ["-h", "--help"])
def test_help_prints_usage_and_exits_zero(option: str) -> None:
    """Help flags print usage on stdout and exit 0."""
    runner = CliRunner()
    result = runner.invoke(main, [option])
    assert result.exit_code == 0
    assert "usage" in result.output.lower()
    assert all(s in result.output for s in [DESCRIPTION, NAME])


@pytest.mark.e2e
@pytest.mark.parametrize("option", ["-v", "--version"])
def test_version_prints_version_and_exits_zero(option: str) -> None:
    """Version flags print VERSION_STR on stdout and exit 0."""
    result = CliRunner().invoke(main, [option])
    assert result.exit_code == 0
    assert result.output.strip() == VERSION_STR


@pytest.mark.e2e
def test_unknown_flag_exits_two() -> None:
    """An unrecognized flag exits 2."""
    result = CliRunner().invoke(main, ["--foobar"])
    assert result.exit_code == 2
