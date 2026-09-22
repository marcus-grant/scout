# Scout - Roadmap

Post-MVP work not yet scheduled.
Pick a section, move it into `doc/TODO.md`, and detail it there before
starting.
`doc/TODO.md` is the tracker; this file only holds what is not yet
planned.

## Versioning until v1.0.0

- MVP is `v0.1.0`: `init`, `scan`, and raw `sqlite3` queries against
  the manifest.
- Minor bumps for schema or other breaking changes; patch for the rest.
- `v1.0.0` is the stable API for wide release.

## Reminders

- The moment `schema_version` changes: map each version to the last
  package version that reads it, and have `Manifest.open` name that
  release in `BadSchemaVersion`.

## Unscheduled

### root

- `scout root [repo] [new-root]`:
  - bare prints the stored root;
    - with an argument rewrites it,
    - hostname-style get and set.
- Covers out-of-tree manifests and removable drives.
  - Also drives that remount at a new path:
    - plain `mv` moves the file,
    - `scout root` re-targets it.
- Distinguish "manifest moved, same tree" from "root re-targeted at a
  different tree"; the second invalidates stored relative paths.

### ignore

- Ignore patterns for scan:
  - likely one `ignore` table with its repo;
    - patterns applied by the scanner at walk time.
- Needs a pattern language decision;
  - *(gitignore syntax or globs)* and management verbs;
    - interaction with `gone` when patterns change.
- Scan already skips its own repo file without this;
  - that is hard-coded, not a pattern.

### comm

- `scout comm <a> <b>`: join two manifests on `hash`, report three
  buckets: only in a, only in b, in both (with both paths).
- Stage role: a source, and a shortcut for `ls a | has b` when both sides
  are manifests. Content-level comparison; not `diff`, which is reserved
  for path-exact tree comparison.

### status

- `scout status [target]`: `scan` without writing. Walk the disk, compare
  stats to the manifest, report new, missing, changed. No hashing.
- Stage role: a source of records for files that differ from the
  manifest, so `scout status | scout has nas.db` answers "of what changed
  here, what does the NAS already have".

### dupes

- `scout dupes [-r repo]`: group live rows by `hash`, print groups with
  more than one member. One SQL statement plus the renderer; trivial
  enough to add early.
- Stage role: a source. Its output is a list of candidates for deletion
  that `has` can check against another manifest before anything is
  removed.

### comment

- `scout comment [repo] [TEXT]`: get-set verb like `scout root`.
  Bare prints `meta.comment`; with an argument it replaces it.
  Dropped from `scan`, where one-per-manifest state has no business.

### rehash-budget

- `rehash_after` in `meta`: a file whose `hashed` is older than it is
  rehashed even when its stat matches.
- Pair it with a per-run budget (a count or a fraction of files) and an
  order (oldest `hashed` first) so a giant NAS collection is rehashed
  incrementally rather than in one burden.
- Enters the decision as one keyword parameter and one comparison.

### observation-log

- A possible table: one row per file per scan with its outcome, and
  `UNREADABLE` as a state, which the file table cannot hold.
- Weigh it before building: Scout is a claim about the last scan, not a
  history; `gone`, `hashed`, and the `scan` rows may be all the history
  wanted.

### verb-architecture

- Decide, with `status` in hand, the shape of verbs that reconcile a
  manifest against disk.
  - The read-compare-apply split: `decide` is pure, `_scan_file` both
    decides and writes; the second verb shows whether that holds.
  - Cross-table writes: `_mark_dir_gone` lives in `lib/scan.py`; a
    named kind (`Op`, the write mirror of `View`, holding no SQL and
    composing repo calls) is the candidate home once a second exists.
  - The parameter threading through `_scan_file` and `_scan_dir` wants
    an options object or a scan context.
  - `scan` still holds setup, the counting loop, and the gone sweep;
    a tally and a sweep helper are the next cuts.
  - `decide`, `Outcome`, `walk`, and `hash_file` are shared readers and
    lift out of `lib/scan.py` when `status` uses them.

### progress

- Rich renderer: the resetting stderr line for files done and bytes of
  the current file; porcelain keeps its plain lines.
- b3c32: a defined error contract for path and stream failures, or
  confirmation that `OSError` pass-through is the contract.

### Undecided

Needs more thought before any of these become tasks.

- `verify`:
  - rehash rows whose stats match and report hash mismatches (bit rot).
  - `md5sum -c` precedent.
- `diff`:
  - path-exact tree comparison between two manifests
  - coreutils meaning.
  - Low priority.
- `prune [--older-than]`:
  - delete gone rows past a cutoff.
  - The only thing that removes gone rows.
- JSON and Rich renderers behind the existing interface.
- Named filters (`--new`, `--missing`, `--changed`) once `status` exists.
- Datasette adapter:
  - metadata and canned queries over one or more attached manifests.
- Configuration stack:
  - args, env, config file, `meta` table
    - in precedence order
  - feeding `select_renderer` and policy values like `rehash_after`.
- Symlink ladder:
  - model as records;
    - resolve targets within the same manifest
    - full handling.
- `ctime` and `inode` columns for a stronger skip heuristic.
  - Meaningful on ext4/btrfs/xfs,
    - unreliable or absent on:
      - `exfat`, `fat`, `ntfs`, `apfs`.
- `note` column on `file` for human annotation of gone rows.
- `parent_id` on `dir` if a direct-children query needs it.
- DB abstraction one step past row mappers.
  - Not an ORM.
  - Two candidates:
    - derive `SCHEMA`, `to_row`, `from_row`,
    - and select/insert column lists from dataclass fields;
      - so DDL and mappers cannot drift;
    - and small filter builder turning `get(**filters)` `kwargs` into
      - `WHERE` clause
    - and parameters once for all repos.
  - No sessions, identity maps, or lazy loading.
  - Decide after the repos have settled.
- Prefix matching across hash widths,
  - *(BLAKE3 XOF property)*,
  - so wider digests still match 120-bit manifests.
- Extra hash columns:
  - *(crc32, sha256)*,
  - unindexed,
  - for external cross-referencing.
- `dir.path` versus derived-from-hierarchy as source of truth.
  - Currently path is the only truth after `dir_ancestor` is dropped;
    - revisit if a hierarchy table returns.
- Explore hand-rolled abstractions past the above as per interest:
  - lazy loading and caching first. Roadmap material.
- `report` subcommand:
  - takes the log file,
  - prunes oldest first to a count,
  - opens the browser on a prepopulated issue URL
- Test suite cleanup:
  - shared arranges as fixtures,
  - `test_dir_repo.py` the worst case
- Threaded reads:
  - `DBConnector.reader()` returning a fresh read-only connection per thread;
    - WAL mode at `init`
