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

- `scout dupes [manifest]`: group live rows by `hash`, print groups with
  more than one member. One SQL statement plus the renderer; trivial
  enough to add early.
- Stage role: a source. Its output is a list of candidates for deletion
  that `has` can check against another manifest before anything is
  removed.

### missing-dirs-per-dir

- Today missing dirs are found after the walk: every live dir is
  loaded via `dirs.descendants(PPP("."))` and checked in Python against
  the walked and unreadable paths.
  - Roughly 1.5 KB per dir at peak; a home dir of 188,800 dirs
    estimates near 280 MB. Acceptable for MVP, not measured.
- Preferred fix: compare each `WalkedDir`'s `subdirs` against the live
  dirs recorded directly under it, as missing files are found today.
  - One query per walked dir, cheap next to hashing.
  - Every missing dir found is topmost, since its parent was walked;
    an unlistable dir has no `subdirs`, so nothing under it is missed.
  - Removes the walked set, the unreadable set and the full dir list.
  - Needs one new `DirRepo` method: live dirs directly under a path.
- Trigger: measure peak memory (`/usr/bin/time -v`) in the NAS
  acceptance run.
  - Around 300 MiB is acceptable but unwanted; 1 GiB is not acceptable
    even for MVP.
  - When to adopt weighs the measured burden against the change's
    complexity and other priorities.

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
- Resume is this same policy, not transaction recovery: order by
  oldest `hashed` first and skip files hashed within a recency window
  given on the CLI, so an interrupted run picks up where the budget
  left off.

### observation-log

- A possible table: one row per file per scan with its outcome, and
  `UNREADABLE` as a state, which the file table cannot hold.
- Weighed and declined 2026-09-22: history stays `gone` and `hashed`
  (scan-FK timestamps); unreachability is reported in the event
  stream (`AccessLost`), never recorded. Revisit only if dogfooding
  demands it.

### verb-architecture

- Scan is all-encompassing and needed early, so parts later operations
  are likely to share are built inside `src/scout/lib/scan/` first.
  - Grouped by subject (`file_stats.py`, `missing_files.py`,
    `missing_dirs.py`), each a reconciliation that writes nothing
    paired with an apply that writes; provisional.
  - Lib events are lib-wide from the start, in `src/scout/lib/event.py`.
- Grouping by subject is expected to get in the way once `ls`, `has`,
  `diff`, `comm` or `status` arrive.
  - Each of those starts by deciding what moves out of scan's package
    into cross-cutting structures, from what it actually reuses.
- Open until then:
  - what the lib's operation layer is called; "verb" is the CLI's word,
    "operation" is a working name, "use case" the textbook one;
  - "verb" is used for the lib layer across the docs and code, so the
    rename is decided once, for all of them;
  - where shared reconciliations, applies and events finally live.

### progress

- Sequenced 2026-09-22: the status-line object is in `doc/TODO.md`
  (`scan-cli`); Rich later replaces it behind the same `log`/`status`
  seams.
- Still here: b3c32's defined error contract for path and stream
  failures, or confirmation that `OSError` pass-through is the
  contract.
- Pre-scan tally / `expected` totals for X-of-N progress: db estimate
  (`~N`, free) or exact pre-walk behind a flag; deferred until
  dogfooding asks.

### resume

- `--resume`, if ever wanted: derivable entirely from existing schema —
  records whose `hashed` FKs to the newest unfinished scan row are
  provably done; build an exclude set, optionally widened by scan age.
  No new columns. Only if the `MATCHED`-skip rerun proves too slow on
  stat-heavy trees.

### commit-trigger

- `commit_every` counts rows written; on slow media one large file is
  one count, so a crash can lose an hour inside a single batch.
- A time or byte trigger beside the count, same `wrote()` seam, when
  dogfooding on the NAS shows the loss.

### claimed-files

- `Claimed.files(path)`: one join returning claimed file paths under
  a path, replacing the per-dir `files.in_dir` N+1 in
  `GoneSubtree.mark`.
- Repo docstrings say claimed where they say live.

### sql-layer

- Post-MVP: thin the repos by moving SQL composition out of them.
  - Repos supply what is inherent to their table: predicates as
    `SqlWhere` from `where_<relation>` methods, statement heads with
    one `{}` hole, someday a column tuple.
  - `clause.py` grows `Statement(sql, params)` and
    `compose(head, where, *leading)`; queries and services assemble,
    never write fragments.
  - The `FROM file JOIN dir ... gone IS NULL` tail in `Claimed` is
    the first shared source; it moves to `clause.py` on its second
    consumer.
- Held to today: every fragment leaving a repo is a `SqlWhere`; own
  params precede the predicate's; `clause.py` stays table-agnostic.

### fs-detail

- The `detail=` seam on `run_scan` and `Manifest.init` keeps tests
  isolated from the host's filesystem readers; any rework keeps it.
- Its name is poor: the values identify the filesystem and host the
  root lives on (`fs_type`, `fs_uuid`, `fs_label`, `fs_model`,
  `hostname`).
- Open: whether refreshing them is a scan policy, a `ScanOptions`
  field, or a CLI-wide option shared by most operations;
  - decided together with the rename, across `run_scan`,
    `Manifest.init` and `MetaRepo.write_fs_detail`.

### error-members

- `doc/CONTRIBUTE.md` rules that a domain declares a member
  only when every child inherently has it,
  and types it optional only when its value can be unknown.
- `Err.FsDomain` follows it: `path` required, `errno` optional.
- `Err.ManifestDomain` and `Err.PathDomain` still carry an optional
  `path`; check per child whether the path is inherent:
  - `ManifestDomain`: likely the manifest each error concerns,
    unless a raise site such as `NestedTransaction`'s cannot know it;
  - `PathDomain`: every child concerns a path the user passed in.

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
