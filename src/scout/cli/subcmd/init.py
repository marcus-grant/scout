# src/scout/cli/subcmd/init.py
"""The scout init subcommand: create a manifest for a target directory.
Author: Marcus
Created: 2026-09-16
License: AGPL-3.0-or-later
"""

from collections.abc import Callable, Mapping
from pathlib import Path

import click

import scout.cli.event as events
from scout.cli.command import scout_command
from scout.cli.render import porcelain
from scout.lib.fs import meta as fs_meta
from scout.lib.manifest import Manifest


def run_init(
    target: Path,
    repo: Path | None,
    comment: str | None,
    emit: Callable[[events.Event], None],
    detail: Mapping[str, str | None] | None = None,
) -> None:
    """Create a manifest for target and emit one InitDone."""
    # Normalize inputs
    target = target.resolve()
    repo = repo if repo is not None else target / Manifest.DEFAULT_NAME
    detail = detail if detail is not None else fs_meta.read_all(target)
    missing = tuple(k for k, v in detail.items() if not v)

    # Run library function associated with init subcommand
    Manifest.init(repo, target, comment=comment, detail=detail)

    # Collect emitted events for prints, logging, and testing
    emit(events.InitDone(repo, target, missing=missing))


def _echo(event: events.Event) -> None:
    """Render event with porcelain and echo out and err lines."""
    output = porcelain(event)
    for line in output.out:
        click.echo(line)
    for line in output.err:
        click.echo(line, err=True)


@scout_command("init")
@click.argument("target", type=click.Path(path_type=Path), default=".")
@click.option("-r", "--repo", type=click.Path(path_type=Path), default=None)
@click.option("--comment", default=None)
def init(target: Path, repo: Path | None, comment: str | None) -> None:
    """Create a manifest for TARGET; -r places the db file elsewhere."""
    run_init(target, repo, comment, _echo)
