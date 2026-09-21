# src/scout/cli/command.py
"""scout_command: the one decorator every subcommand uses; owns the error boundary.
Author: Marcus
Created: 2026-09-10
License: AGPL-3.0-or-later
"""

import functools
import traceback
from collections.abc import Callable
from typing import Any

import click

import scout.lib.error as Err
from scout.cli import NAME, VERSION_STR

ISSUES_URL = "https://github.com/marcus-grant/scout/issues"
EX_SOFTWARE = 70

_ANY_FUNC = Callable[..., Any]
_CMD_DECO = Callable[[_ANY_FUNC], click.Command]


def scout_command(*args: Any, **kw: Any) -> _CMD_DECO:
    """click.command with the shared error boundary installed around the callback."""

    def decorator(fn: _ANY_FUNC) -> click.Command:
        @functools.wraps(fn)
        def guarded(*a: Any, **k: Any) -> Any:
            try:
                return fn(*a, **k)
            except Err.ScoutDomain as e:
                click.echo(f"{NAME}: {e}", err=True)
                raise SystemExit(1)
            except click.ClickException:
                raise
            except Exception as e:  # noqa: BLE001 everything unknown is rewrapped
                _report_unknown(Err.ScoutUnknown.wrap(e))
                raise SystemExit(EX_SOFTWARE)

        return click.command(*args, **kw)(guarded)

    return decorator


def _report_unknown(err: Err.ScoutUnknown) -> None:
    """Print the traceback, the version, and ISSUES_URL to stderr."""
    click.echo("".join(traceback.format_exception(err.__cause__)))
    click.echo(f"{VERSION_STR}: {err}", err=True)
    click.echo(f"Please report this at {ISSUES_URL}", err=True)
