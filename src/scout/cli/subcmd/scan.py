# src/scout/cli/subcmd/scan.py
"""The scout scan subcommand: bring a manifest's rows current with its root.
Author: Marcus
Created: 2026-09-21
License: AGPL-3.0-or-later
"""

import itertools
import sys
from collections.abc import Callable
from dataclasses import astuple
from pathlib import Path

import click

import scout.cli.event as events
from scout.cli.command import scout_command
from scout.cli.render import echo_porcelain
from scout.lib.event import AccessLost, FileScanned, ReadFailed, RecordGone
from scout.lib.fs import meta as fs_meta
from scout.lib.manifest import Manifest
from scout.lib.scan import ScanOptions
from scout.lib.scan import scan as lib_scan
from scout.lib.scan.hashing import HashingPolicy
from scout.lib.scan.tally import ScanTally, Summary


def _translate_event(
    event: FileScanned | RecordGone | ReadFailed | AccessLost | Summary,
) -> events.CliEvent:
    """Return the CliEvent for one lib scan event; raise TypeError if unknown."""
    match event:
        case FileScanned():
            return events.ScanFile(event.path, event.record, event.change)
        case RecordGone():
            return events.ScanGone(event.path)
        case ReadFailed():
            return events.ScanError(event.path, event.error)
        case AccessLost():
            return events.ScanAccessLost(event.path)
        case Summary():
            return events.ScanFinished(*astuple(event))
    raise TypeError(f"no CliEvent translation for scan event: {type(event)}")


def run_scan(
    path: Path,
    opt: ScanOptions,
    make_emit: Callable[[ScanTally], Callable[[events.CliEvent], None]],
) -> None:
    """Open the manifest at path, refresh its fs detail, scan it with opt,
    and emit each event translated to its CliEvent through make_emit(tally)."""
    # Open and update manifest with fs meta details
    manifest = Manifest.open(path)
    root = Path(manifest.meta.root)
    manifest.meta.write_fs_detail(fs_meta.read_all(root))

    # Scan and emit each lib event as its CliEvent, sharing one tally:
    # lib_scan counts into it, and the emitter make_emit builds reads it
    emit = make_emit(tally := ScanTally())
    for result in lib_scan(manifest, tally, opt):
        emit(_translate_event(result))


def _emitter(verbose: bool, progress: bool) -> Callable[[events.CliEvent], None]:
    """Build the emit for the command: echo every event through porcelain,
    except ScanFile which is echoed only when verbose; when progress is set,
    also write one 'N files' line to stderr per ScanFile and ScanGone."""
    done = 0

    def emit(event: events.CliEvent) -> None:
        nonlocal done
        if progress and isinstance(event, (events.ScanFile, events.ScanGone)):
            done += 1
            click.echo(f"{done} files", err=True)
        if isinstance(event, events.ScanFile) and not verbose:
            return
        echo_porcelain(event)

    return emit


_SPINNER = "-\\|/"
_spin = itertools.cycle(_SPINNER)


def _bytes_progress(done: int) -> None:
    """The hashing callback under --progress:
    when stderr is a terminal, overwrite one spinner character;
    otherwise write 'done bytes' as a line."""
    if sys.stderr.isatty():
        click.echo(f"\r{next(_spin)}", err=True, nl=False)
    else:
        click.echo(f"{done} bytes", err=True)


@scout_command("scan")
@click.argument("path", type=click.Path(path_type=Path), default=".")
@click.option("--no-hash", is_flag=True, help="Stat only; leave hashes untouched.")
@click.option("--rehash", is_flag=True, help="Hash every file, matching or not.")
@click.option("-v", "--verbose", is_flag=True, help="One line per file.")
@click.option("--progress", is_flag=True, help="Files done and bytes on stderr.")
def scan(
    path: Path, no_hash: bool, rehash: bool, verbose: bool, progress: bool
) -> None:
    """Bring the manifest at PATH current with its root; PATH may be the root."""
    if no_hash and rehash:
        raise click.UsageError("--rehash cannot be used with --no-hash")
    if no_hash:
        hash_policy = HashingPolicy.OFF
    elif rehash:
        hash_policy = HashingPolicy.ALL
    else:
        hash_policy = HashingPolicy.NEEDED
    opt = ScanOptions(hash_policy, on_progress=_bytes_progress if progress else None)
    run_scan(path, opt, lambda _: _emitter(verbose, progress))
