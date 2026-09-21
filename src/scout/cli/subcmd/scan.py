# src/scout/cli/subcmd/scan.py
"""The scout scan subcommand: bring a manifest's rows current with its root.
Author: Marcus
Created: 2026-09-21
License: AGPL-3.0-or-later
"""

import itertools
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from pathlib import PurePosixPath as PPP

import click

import scout.cli.event as events
import scout.lib.error as Err
from scout.cli.command import scout_command
from scout.cli.render import echo_porcelain
from scout.lib.fs import meta as fs_meta
from scout.lib.manifest import Manifest
from scout.lib.scan import Gone, Scanned, Summary
from scout.lib.scan import scan as lib_scan


def run_scan(
    path: Path,
    emit: Callable[[events.Event], None],
    *,
    hash: bool = True,
    force: bool = False,
    on_progress: Callable[[int], None] | None = None,
    detail: Mapping[str, str | None] | None = None,
) -> None:
    """Open the manifest at path (a directory means its DEFAULT_NAME),
    refresh the fs detail rows, then run scan and emit one event per record:
    Scanned to ScanFile, Gone to ScanGone,
    Unreadable to ScanError, Summary to ScanFinished.
    Detail replaces fs_meta.read_all(root) when given, for tests."""
    # Open and update manifest with fs meta details
    manifest = Manifest.open(path)
    root = Path(manifest.meta.root)
    detail = detail if detail is not None else fs_meta.read_all(root)
    manifest.meta.write_fs_detail(detail)

    # Start the scan and iterate results to be mapped to CLI events
    results = lib_scan(manifest, hash=hash, force=force, on_progress=on_progress)
    for result in results:
        match result:
            case Scanned():
                emit(events.ScanFile(result.path, result.file, result.outcome))
            case Gone():
                emit(events.ScanGone(result.path))
            case Err.Unreadable():
                emit(events.ScanError(result.path or PPP("?"), result))
            case Summary():
                emit(
                    events.ScanFinished(
                        result.started,
                        result.finished,
                        result.added,
                        result.updated,
                        result.matched,
                        result.errors,
                        result.gone,
                    )
                )


def _emitter(verbose: bool, progress: bool) -> Callable[[events.Event], None]:
    """Build the emit for the command: echo every event through porcelain,
    except ScanFile which is echoed only when verbose; when progress is set,
    also write one 'N files' line to stderr per ScanFile and ScanGone."""
    done = 0

    def emit(event: events.Event) -> None:
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
    hsh, force = not no_hash, rehash
    on_progress = _bytes_progress if progress else None
    run_scan(
        path,
        _emitter(verbose, progress),
        hash=hsh,
        force=force,
        on_progress=on_progress,
    )
