# Scout - TODO

## Before Working

### Crucial rules

<!-- Placeholder: improve with CONTRIBUTE.md and QA.md exist. Draft candidates: -->
- Strict lib/adapter split.
  `lib/` emits typed records and never formats or prints.
  Adapters (CLI only for now) handle input, output, and exit codes.
- Every operation follows coreutils conventions:
  - stdin/stdout, one record per line, meaningful exit codes.
  - Operations must compose with each other in pipelines.
- Rich is imported only inside the Rich renderer.
  - Porcelain and JSON output must never contain terminal decoration.
- The manifest schema is versioned via `fs_meta.schema_version`.
  - Schema changes bump the version; unversioned or mismatched manifests are refused.
- One branch per PR.
  No direct commits to `main`.
  `just check` passes before a PR is opened.
- Stored paths are `PurePosixPath`,
  - imported everywhere as `from pathlib import PurePosixPath as PPP`,
  - relative to `root`.
- `Path` exists only at the I/O boundary
  - *(scanner, `DBConnector`, CLI)*
  - and is converted on entry with:
    - `PPP(path.relative_to(root).as_posix())`.
- The lib raises typed exceptions from one hierarchy rooted at
  `ScoutError` (`lib/errors.py`) and never prints or exits. The CLI
  catches at the subcommand boundary and maps each error to a message
  and exit code. New failure modes get a new subclass in the same PR
  that introduces them, never a bare `Exception` or a string check.

### Required reading

- `doc/CONTRIBUTE.md` (mandatory)
- `doc/QA.md` (mandatory)
- `README.md` (optional)

## Sequenced PRs to MVP

### Test foundation

- Salvage
  - `git mv test/ salvage/test/`. Nothing under `salvage/` is collected
    (`norecursedirs` in `pyproject.toml`).
  - Each later PR, for the modules it touches, either moves a salvaged
    test module back (with or without edits) or rewrites it and deletes
    the salvaged one. The PR description says which.
  - `salvage/` is deleted when empty. Target: gone by the end of MVP.
- Conventions (record in `doc/QA.md`)
  - No pyfakefs. Real files under pytest `tmp_path`.
  - Factories with default outputs and override kwargs (`make_tree`,
    `make_manifest`, `make_file_entry`). Fixtures are the zero-arg
    factory calls. Factories compose: a manifest factory takes a tree.
  - PR workflow: write the e2e test for the PR's feature first, marked
    skip. Green the unit tests it depends on. Unskip as acceptance.
  - e2e tests touch only the CLI (`CliRunner`, in-process) and the DB file
    (sqlite3), never `lib/` internals. Marked `@pytest.mark.e2e`;
    `just test -m "not e2e"` is the fast loop. Factory trees stay tiny.
- Scope: factories and conftest only. No production code changes.

### Schema

- Table ownership
  - Each repo owns its DDL as a class constant: `DirRepo.SCHEMA`,
    `FileRepo.SCHEMA`, `MetaRepo.SCHEMA`, `ScanRepo.SCHEMA`.
  - Table creation moves out of repo `__init__`.
- `Manifest` (new), the composite over one `.scout.db`
  - Exposes `.meta`, `.dirs`, `.files`, `.scans`, sharing one
    `DBConnector`.
  - `Manifest.init(path, root, ...)`: creates the file, runs each repo's
    `SCHEMA` in order, writes `schema_version` and `hash_algo=b3c32`
    through `MetaRepo`. Holds `SCHEMA_VERSION`.
  - `Manifest.open(path)`: opens and runs validation.
  - Transaction boundary in one place: `with manifest:` commits or rolls
    back across all repos.
  - The CLI adapter imports only `Manifest`, never a repo.
  - Migrations, when needed, go wherever makes sense then.
- `MetaRepo` (new), owns `fs_meta`
  - Typed getters and setters for `root`, `schema_version`, `hash_algo`,
    `comment`, `fs_type`, `fs_uuid`, `fs_label`, `fs_model`, `hostname`.
  - Nothing else writes `fs_meta` by hand.
- `ScanRepo` (new), owns `scan(id, started, finished, files_seen)`
  - One row per scan run. `finished` of the previous scan is what `gone`
    is set to.
- `DBConnector`
  - Opens and validates only. Validation adds a `schema_version` check;
    refuses unversioned or mismatched manifests with a clear message.
  - `init_db` removed.
- `dir` table
  - Drop `dir_ancestor`. Nothing outside `DirRepo` reads it and ancestors
    are already derived from the path string.
  - `dir(id, path)` with the existing `UNIQUE(path)` index is sufficient.
  - `get_ancestors`: `WHERE path IN (prefixes)`.
  - `get_descendants`: range scan `path >= 'p/' AND path < 'p0'`.
  - Interface and return types unchanged. No `parent_id` until a query
    needs it.
- `file` table: `file(id, dir_id, name, hash, size, mtime, hashed, gone)`
  - `md5` renamed to `hash` (text, indexed); `fs_meta.hash_algo` says
    what it is. `updated` dropped.
  - `hashed`: when the hash was last computed. Drives the rehash margin.
  - `gone`: null for live rows; otherwise the `finished` of the last scan
    that saw the file. Set for rows a scan did not encounter; cleared by
    the upsert when the path reappears. One row per path, always.
  - Repo queries filter `gone IS NULL` by default.
  - `UNIQUE(dir_id, name)`.
  - Timestamps are integer epoch seconds; `mtime` is integer nanoseconds
    as returned by `os.stat`.
- Hash model
  - `HashMD5` replaced by a b3c32-backed model in `lib/model/hash.py`.
  - Width is derived from encoded length, not stored.
- Tests
  - Schema creation, version check both ways, `MetaRepo` and `ScanRepo`
    round-trips, `DirRepo` ancestor and descendant queries, `gone` set
    and cleared, hash model against b3c32.
  - `test_dir_repo.py` loses its `dir_ancestor` tests with the table.
- Errors
  - `lib/error.py` with `ScoutDomain` and the first subclasses:
    `ManifestDomain`, `NotAManifest`, `SchemaVersionMismatch`,
    `ManifestExists`. `DBConnector` validation raises these.
  - `ScoutUnknown` is a subclass of `ScoutDomain`.
    - Handles uncaught exceptions we haven't predicted yet.
    - CLI layer should always try and catch and print as much detail as possible
    - Can be used to rewrap another non ScoutDomain error
  - Later PRs add their own (`ScanDomain`, `HashDomain`, and so on).
  - Make it convention to import `lib.error as Err`
    - Select the error with that namespace.
    - When you encounter errors the belong in a domain,
      - Subclass those to an error class with `Domain` in its name.
    - This reads cleaner as `except Err.NotAManifest:`
      - ...than `except Err.NotAManifestError:`.
      - And it keeps imports from exploding when a new subclass is added.
      - Allows common handlings of domains with `except Err.ManifestDomain as e:`.

### Init port

- Click
  - `scout` becomes a Click group in `adapter/cli/`; `init` is its first
    subcommand. argparse removed.
  - `target` defaults to cwd; `-r/--repo` is the full path to the DB file,
    defaulting to `target / ".scout.db"`. Built with `Path`, not string
    formatting.
  - New `--comment` for the human-readable disk description.
- What init writes (through `MetaRepo`)
  - `schema_version`, `hash_algo`, `root`, `comment`.
  - fs detail rows if readable, null otherwise. Each reader is a thin
    function (`/proc/mounts`, `/dev/disk/by-*`, `lsblk`, hostname) so
    tests can stub it. Linux only for now.
- Tests
  - e2e: `scout init` via `CliRunner` against a factory tree; assert
    `fs_meta` contents through sqlite3.
  - fs detail readers stubbed; one unit test per reader against canned
    input.

### Rename

- `lib/handler/` to `lib/repo/`, `cli/` to `adapter/cli/`. Pure `git mv`
  plus import fixes. No other changes, so history and blame survive.

### Repo refactor

- Connector injection
  - `DirRepo`, `FileRepo`, `MetaRepo` accept a `DBConnector`; they no
    longer take `path` and `root` or open their own connection.
  - `Manifest` (see Schema) constructs all three over one connector.
- Path types
  - Models and repos hold `PurePosixPath`, never `PurePath` or `Path`.
  - `os.path.*` calls in models and `DBConnector` replaced with `Path`
    methods; `DBConnector` validation accepts `Path`.
  - Conversion happens once at entry, in the scanner and CLI.
- Row mappers
  - `Dir.from_row` / `Dir.to_row`, `File.from_row` / `File.to_row`.
  - Tested in isolation against plain tuples or `sqlite3.Row`.
  - Repos reduce to SQL plus a mapper call; no column-order knowledge in
    `get()` or `put()`.
- `FileRepo.put()`
  - `INSERT ... ON CONFLICT(dir_id, name) DO UPDATE`, `updated` set only
    when a column actually changed. Row ids stay stable across reruns.
  - Same upsert shape for `DirRepo.add()` on `path` if it does not already
    behave that way.
- Ids
  - Row ids are local to one manifest. They never appear in output or in
    cross-manifest comparison; those speak in paths and hashes only.
- Tests
  - Mapper round-trips, `put()` insert then update with stable id,
    `Manifest` wiring end to end against a factory DB.

### Scanner

- `lib/scanner.py`: walk and stat only. Yields one record per file in DFS
  path order (`dirs_sorted_dfs`), with `hash` computed only when the
  caller asks for that file.
- Records
  - `FileEntry` dataclass: `PPP` path, `size`, `mtime` (ns), optional
    `hash`.
  - Unreadable files and dirs yield an error record (path, reason); the
    walk never aborts.
- Rehash decision belongs to the caller (`scan`, later `verify`), per
  file, in this order: no row, or size/mtime differ, or `rehash_after`
  policy is set and `hashed` is older than it.
- `rehash_after` lives in `fs_meta`, default never.
- Hashing
  - b3c32 currently exposes `hash_b32(data: bytes, digest_len: int)`,
    120 only. Whole-file bytes will not do for gigabyte files.
  - Blocking dependency: b3c32 gains a streaming entry point (chunked
    `update()` style, or a from-path helper) before this PR starts.
    Scout does not work around it by calling blake3 directly.
  - Spec first: loop in the co-maintainers of `depo` and `normpic`
    alongside `b3c32` to agree a hard spec for the streaming interface,
    since all three projects will implement against it.
- Symlinks: not followed, not recorded. Ladder deferred to optional.
- Tests
  - Factory trees under `tmp_path`: ordering, stat fields, error record
    on an unreadable file, hash present only when requested.

### Renderer

- Interface, in `adapter/cli/render/`
  - `Renderer` protocol: `record(r)` called per record as the lib yields
    it, `finish()` called once at the end. Nothing else.
  - Every subcommand takes a `Renderer` as a parameter and never prints
    directly. Exit codes stay in the subcommand.
- Selection
  - One function, `select_renderer(config) -> Renderer`, is the only
    place a renderer is chosen. For MVP `config` is the parsed CLI args.
    Later the configuration stack (args, env, config file, `fs_meta`, in
    precedence order) produces the same `config` and nothing downstream
    changes.
  - Shared Click options (`--porcelain` for now; `--json`, `-v` later)
    declared once and attached to every subcommand.
- Porcelain (only implementation in MVP)
  - One record per line, tab-separated, fields in dataclass order, no
    header, no colour, no alignment. Stable: adding a field appends a
    column; removing or renaming one is a breaking change.
  - Error records go to stderr in the same shape, prefixed so they can
    be filtered.
- Tests
  - Porcelain output against fixed records, string comparison.
  - A collecting fake `Renderer` used by subcommand tests to assert on
    records rather than on text.

### Scan

- `scout scan [target] [-r repo] [--no-hash] [--comment TEXT]`
  - `Manifest.open` (raises if not a manifest or version mismatch).
  - `--comment` updates `fs_meta.comment`; fs detail rows refreshed each
    run through `MetaRepo`.
  - Hashing is the default; `--no-hash` does a stat-only pass.
- Run
  - Open a `scan` row (`started`), then walk the scanner in DFS order.
  - Per dir: `DirRepo` upsert.
  - Per file: look up the row by `(dir_id, name)`, apply the rehash
    decision from the Scanner section, hash if needed (`hashed` updated
    when it is), `FileRepo.put()`, insert `(dir_id, name)` into a temp
    table of seen paths, hand the record to the renderer.
  - After the walk: set `gone` on live rows not in the temp table, using
    the previous scan's `finished`; close the `scan` row (`finished`,
    `files_seen`).
  - Commit every N files (N configurable, default in the hundreds) so an
    interrupted scan keeps its progress. How the next run treats an
    unfinished `scan` row is decided during implementation; a message
    and exit code may be enough.
- Output: one porcelain line per file as it is processed. That is the
  progress indicator for MVP.
- Errors: `Err.ScanError` subclasses for the failure modes met here;
  scanner error records are rendered, not raised.
- Tests
  - e2e: `scout init` then `scout scan` on a factory tree; assert `file`,
    `dir`, and `scan` rows through sqlite3. Rerun with one file deleted,
    one modified, one added; assert `gone`, `hashed`, and the new row.
  - Unit: the rehash decision, the `gone` query, batched commits.

## Unsequenced PRs before MVP

Optional work, roughly ordered by how likely it is to be pulled into MVP.
Nothing here is required for MVP. Items near the top may be promoted
into the sequence once `scan` is in real use; items near the bottom are
recorded so they are not forgotten and will move to a `ROADMAP.md` when
this section outgrows the document.

Every verb here is designed to pipe into the others. Each follows the
coreutils shape: reads records or hashes from stdin when given no other
source, writes one porcelain record per line to stdout, and uses the exit
code to mean something. The intended workflows are pipelines of scout
into scout, so a verb that cannot be a stage in one is not done.

### has

- `scout has [-r repo] [< hashes]`: read hashes from stdin (one per line,
  or porcelain records whose hash column is used), one query via a temp
  table join, print found/missing per hash with the path where found.
  Non-zero exit if any are missing, like `md5sum -c`.
- Stage role: the filter. Takes any hash-bearing stream and splits it by
  presence in a manifest. Answers "does this content exist anywhere on
  the NAS" in batch. The most likely promotion to MVP.

### ls

- `scout ls [-r repo] [prefix] [--gone]`: print live rows from the
  manifest, optionally under a path prefix (range scan on `dir.path`),
  optionally only gone rows.
- Stage role: the source. Produces the stream every other verb consumes.
  `scout ls old.db --gone | scout has nas.db` is the migration check;
  `scout ls old.db photos/ | scout has nas.db` is the per-directory one.

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

- `verify`: rehash rows whose stats match and report hash mismatches
  (bit rot). `md5sum -c` precedent.
- `diff`: path-exact tree comparison between two manifests, coreutils
  meaning. Low priority.
- `prune [--older-than]`: delete gone rows past a cutoff. The only thing
  that removes gone rows.
- JSON and Rich renderers behind the existing interface.
- Named filters (`--new`, `--missing`, `--changed`) once `status` exists.
- Configuration stack: args, env, config file, `fs_meta`, in precedence
  order, feeding `select_renderer` and policy values like
  `rehash_after`.
- Datasette adapter: metadata and canned queries over one or more
  attached manifests.
- Symlink ladder: model as records; resolve targets within the same
  manifest; full handling.
- `ctime` and `inode` columns for a stronger skip heuristic. Meaningful
  on ext4/btrfs/xfs, unreliable or absent on exfat, fat, ntfs, apfs.
- `note` column on `file` for human annotation of gone rows.
- `parent_id` on `dir` if a direct-children query needs it.
- DB abstraction one step past row mappers. Not an ORM. Two candidates:
  derive `SCHEMA`, `to_row`, `from_row`, and select/insert column lists
  from the dataclass fields so DDL and mappers cannot drift; and a small
  filter builder turning `get(**filters)` kwargs into a `WHERE` clause
  and params once for all repos. No sessions, identity maps, or lazy
  loading. Decide after the repos have settled.
- Prefix matching across hash widths (BLAKE3 XOF property) so wider
  digests still match 120-bit manifests.
- Extra hash columns (crc32, sha256), unindexed, for external
  cross-referencing.
- `dir.path` versus derived-from-hierarchy as source of truth. Currently
  path is the only truth after `dir_ancestor` is dropped; revisit if a
  hierarchy table returns.
- Explore hand-rolled abstractions past the above as they become
  interesting: lazy loading and caching first. Roadmap material.
