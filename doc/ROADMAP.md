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

### Undecided

Needs more thought before any of these become tasks.

- `verify`:
  - rehash rows whose stats match and report hash mismatches (bit rot).
  - `md5sum -c` precedent.
- `diff`:
  - path-exact tree comparison between two manifests
  - coreutils meaning.
  - Low priority.
- `fs_meta` -> `meta` rename:
  - `fs_meta` is awkward, it's functionally the manifest meta.
  - So any other not strictly FS related thing lives alongside.
  - Remember this might trigger SCHEMA_VERSION bump.
    - That means you must implement migration infrastructure before.
- `prune [--older-than]`:
  - delete gone rows past a cutoff.
  - The only thing that removes gone rows.
- JSON and Rich renderers behind the existing interface.
- Named filters (`--new`, `--missing`, `--changed`) once `status` exists.
- Datasette adapter:
  - metadata and canned queries over one or more attached manifests.
- Configuration stack:
  - args, env, config file, `fs_meta`
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
