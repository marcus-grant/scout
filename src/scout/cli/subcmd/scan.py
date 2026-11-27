# src/scout/cli/subcmd/scan.py
"""The scout scan subcommand: bring a manifest's rows current with its root.
Author: Marcus
Created: 2026-09-21
License: AGPL-3.0-or-later
"""

import sys
from collections.abc import Callable
from dataclasses import astuple
from pathlib import Path
from pathlib import PurePosixPath as PPP

import click

from scout.cli import event as cli_event
from scout.cli.command import scout_command
from scout.cli.emit import make_emit
from scout.cli.progress import open_sink
from scout.cli.render import porcelain
from scout.lib import event as lib_event
from scout.lib.fs import meta as fs_meta
from scout.lib.manifest import Manifest
from scout.lib.scan import ScanOptions
from scout.lib.scan import scan as lib_scan
from scout.lib.scan.hashing import HashingPolicy
from scout.lib.scan.tally import ScanTally, Summary

CliEvent = cli_event.CliEvent


def _translate_event(event: lib_event.ScanEvent | Summary) -> CliEvent:
    """Return the CliEvent for one lib scan event; raise TypeError if unknown."""
    match event:
        case lib_event.FileScanned():
            return cli_event.ScanFile(event.path, event.record, event.change)
        case lib_event.RecordGone():
            return cli_event.ScanGone(event.path)
        case lib_event.ReadFailed():
            return cli_event.ScanError(event.path, event.error)
        case lib_event.AccessLost():
            return cli_event.ScanAccessLost(event.path)
        case lib_event.RecordExcluded():
            return cli_event.ScanExcluded(event.path)
        case Summary():
            return cli_event.ScanFinished(*astuple(event))
    raise TypeError(f"no CliEvent translation for scan event: {type(event)}")


def run_scan(
    path: Path, opt: ScanOptions, tally: ScanTally, emit: Callable[[CliEvent], None]
) -> None:
    """Open the manifest at path, refresh its fs detail, scan it with opt
    counting into tally, and emit each event translated to its CliEvent."""
    # Open and update manifest with fs meta details
    manifest = Manifest.open(path)
    root = Path(manifest.meta.root)
    manifest.meta.write_fs_detail(fs_meta.read_all(root))

    # Scan and emit each lib event as its CliEvent
    for result in lib_scan(manifest, tally, opt):
        emit(_translate_event(result))


_HELP_SCAN_EXCLUDE = "Skip PATH under the root; repeatable"


@scout_command("scan")
@click.option(
    "-r",
    "--repo",
    type=click.Path(path_type=Path),
    default=".",
    help="Manifest file, or the directory holding .scout.db.",
)
@click.option("--no-hash", is_flag=True, help="Stat only; leave hashes untouched.")
@click.option("--rehash", is_flag=True, help="Hash every file, matching or not.")
@click.option("-v", "--verbose", is_flag=True, help="One line per file.")
@click.option("-p", "--progress", is_flag=True, help="Progress on stderr.")
@click.option("--exclude", multiple=True, metavar="PATH", help=_HELP_SCAN_EXCLUDE)
def scan(
    repo: Path,
    no_hash: bool,
    rehash: bool,
    verbose: bool,
    progress: bool,
    exclude: tuple[str, ...],
) -> None:
    """Bring the manifest at REPO current with its root; REPO may be its directory."""
    if no_hash and rehash:
        raise click.UsageError("--rehash cannot be used with --no-hash")
    if no_hash:
        hash_policy = HashingPolicy.OFF
    elif rehash:
        hash_policy = HashingPolicy.ALL
    else:
        hash_policy = HashingPolicy.NEEDED
    for p in exclude:
        if ".." in PPP(p).parts:
            raise click.UsageError(f"--exclude {p} reaches outside the root")
    excluded_rels = frozenset(PPP(p.lstrip("/")) for p in exclude)
    tally = ScanTally()
    with open_sink(progress, tally, sys.stdout, sys.stderr) as sink:
        opt = ScanOptions(hash_policy, on_progress=sink.hashed, exclude=excluded_rels)
        run_scan(repo, opt, tally, make_emit(verbose, porcelain.render, sink))
