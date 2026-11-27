# src/scout/cli/subcmd/init.py
"""The scout init subcommand: create a manifest for a target directory.
Author: Marcus
Created: 2026-09-16
License: AGPL-3.0-or-later
"""

import sys
from collections.abc import Callable, Mapping
from pathlib import Path

import click

from scout.cli import event
from scout.cli.command import scout_command
from scout.cli.progress import OutputSink
from scout.cli.render import porcelain
from scout.lib.fs import meta as fs_meta
from scout.lib.manifest import Manifest


def run_init(
    target: Path,
    repo: Path | None,
    comment: str | None,
    emit: Callable[[event.CliEvent], None],
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
    emit(event.InitDone(repo, target, missing=missing))


@scout_command("init")
@click.argument("target", type=click.Path(path_type=Path), default=".")
@click.option("-r", "--repo", type=click.Path(path_type=Path), default=None)
@click.option("--comment", default=None)
def init(target: Path, repo: Path | None, comment: str | None) -> None:
    """Create a manifest for TARGET; -r places the db file elsewhere."""
    sink = OutputSink(sys.stdout, sys.stderr)
    run_init(target, repo, comment, lambda evt: sink.write(porcelain.render(evt)))
