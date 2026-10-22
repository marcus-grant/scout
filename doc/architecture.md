# Scout architecture

What the code is; how work is done lives in `CONTRIBUTE.md`.

## Layers

- `lib/models.py`: the vocabulary every layer passes around.
- `lib/fs/`: observation atoms; read the disk, never the manifest.
- `lib/manifest/`: the only place a transaction commits.
  - `repo/`: one class per table; the only writer of that table.
  - `service.py`: one concern's writes across repos (`GoneSubtree`).
  - `query.py`: reads that join across tables, never writes (`Claimed`).
  - `clause.py`: SQL pieces shared inside the package (`SqlWhere`).
  - `Manifest` composes them and implements none.
  - Ids cross these as Python values, never as joins into another
    repo's statement.
- `lib/scan/`: the scan operation; yields typed events (`lib/event.py`).
  - `__init__.py`:
    - `scan`, the session:
      - it binds `walk` and `hash_file`,
      - opens the scan row and the commit window, and counts.
    - `ScanOptions`: what an adapter decides before a run.
    - `ScanContext`: what one run holds fixed, passed down.
    - `update_from_walk`: the per-dir loop and the gone pass,
      over walk output given as values.
    - Each step ticks the manifest right after the row it writes.
  - `reconcile.py`:
    - `WalkedPathSets`:
      - what one walk covered,
      - and the pure reconciliations of records against stats,
      - policy and that coverage; they write nothing.
  - `hashing.py`: `HashingPolicy` and `hash_record`, the hash step.
  - `tally.py`: `ScanTally` counts events; `Summary` is its frozen end.
  - These parts are likely shared by later operations;
    - they live here provisionally, see ROADMAP `### verb-architecture`.
- `cli/`: input, output, exit codes; renderers turn events into lines.
  - A Click callback turns argv into the lib's options, nothing else.
  - `run_<verb>`:
    - opens the manifest, runs the lib operation,
    - and emits each lib event translated to its `CliEvent`.
  - The translation is one pure function per verb;
    - an event it has no case for raises.

## Vocabulary

- stat: live fs truth (`FileStat`).
- record:
  - the manifest's last claim (`FileRecord`, `DirRecord`);
    - `FileRecord` embeds the stat it last saw.
- change:
  - what stat proves a record needs (`RecordChange.classify`);
    - `VERIFIED` is claimed only after a hash.
- event: a fact a verb emits;
  - lib events are non-prefixed under `Event`,
  - adapter events are prefixed (`CliEvent`);
  - a verb's events are named once as a union beside them (`ScanEvent`),
    - so a new event touches that union, not every signature.
- tally: progress counting; summary: the run's contract counts.

## Walk

- `walk` yields one `WalkedDir` per directory.
  - In sorted DFS path order.
- A directory that can't be listed is flagged `unlistable`.
  - It is not descended into.
  - Nothing inside it is seen;
    - so nothing inside it is claimed present or gone.
  - `scan` reports `AccessLost` when the manifest claims anything under it.
- Entries that can't be read are reported in their directory's `errors`.
  - The walk continues without raising.
    - It's something to report, not an error to stop execution.
  - `scan` keeps their records as they were, since they were listed.
- Entries that vanished after the listing are left out.
  - `scan` compares the listing with what is already recorded;
    - recorded entries now missing from it are marked gone.
- Excluded paths are not listed;
  - their names are reported in `excluded`.
  - Excluded directories are not descended into.
  - Nothing excluded is seen, as with an unlistable directory;
    - so nothing excluded is claimed present or gone.
  - `scan` reports `RecordExcluded` for each one the manifest has a row for.
  - An exclude that names nothing under root stops the scan before it starts.

## Promotion

A definition moves to a shared module on its second independent consumer.
Exception: `RecordChange` lives in `models` so no verb imports another.
