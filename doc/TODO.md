# Scout - TODO

## Before Working

### Crucial rules

- Strict lib/adapter split.
  - `lib/` emits typed records and never formats or prints.
  - Adapters (CLI only for now) handle input, output, and exit codes.
- A verb reads records or hashes from stdin when given no other source.
- A verb that cannot be a stage in a scout-to-scout pipeline is not done.
  - **EXCEPT** ones with clear use in other `coreutil`-like tools *(grep or jq)*
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
- Errors, repos, transactions, and timestamps:
  - follow the code conventions in `doc/CONTRIBUTE.md`;
    - new failure modes,
      - get a new `Err` subclass in the PR that introduces them.
- Row ids are local to one manifest.
  - They never appear in output or in cross-manifest comparison;
    - those speak in paths and hashes only.
- Timestamps are int64 nanoseconds;
  - `gone` and `hashed` are the `scan.started` of the scan that observed them.

### Required reading

- `doc/CONTRIBUTE.md` (mandatory)
- `doc/QA.md` (mandatory)
- `README.md` (optional)

## Sequenced PRs to MVP

### Init port

- Click
  - The argparse `cli/subcmd/init.py` was deleted on tst/test-foundation;
    read it with `git show main:cli/subcmd/init.py` when porting.
    Its tests are written fresh.
  - `init` is the first subcommand on the `scout` group, built with
    `scout_command`; shared options (`-r/--repo`) land here.
  - `target` defaults to cwd; `-r/--repo` is the full path to the DB
    file, defaulting to `target / ".scout.db"`. Built with `Path`.
  - New `--comment` for the human-readable disk description.
- Argument checks, as `Err.PathDomain` children raised before
  `Manifest.init`: the repo path's parent must exist and be a
  directory; the target must be a directory.
  - The boundary converter from `Path` to root-relative `PPP` lives
    here; the salvaged `TestPathHelpers` cases (absolute to relative,
    `..` rejected, `str` and path input alike) are its starting spec
- What init writes (through `MetaRepo`)
  - `schema_version`, `hash_algo`, `root`, `comment` come from
    `Manifest.init`.
  - fs detail rows if readable, null otherwise. Each reader is a thin
    function (`/proc/mounts`, `/dev/disk/by-*`, `lsblk`, hostname) so
    tests can stub it. Linux only for now.
- Tests
  - e2e: `scout init` via `CliRunner` against a factory tree; assert
    `fs_meta` contents through sqlite3.
  - fs detail readers stubbed; one unit test per reader against canned
    input.
- Salvage: `salvage/test/cli/subcmd/test_init.py` read and deleted;
  `test_fs.py` is Scan's

### Scan

Lib walker and hasher, the `scout scan` verb as its e2e, and the manual
acceptance run, in one PR.

- Blocking dependency: `b3c32` 0.0.3, specified with its maintainer:
  - `Hasher(bits)` with `update`, `digest`, `b32`; chunking-independent
  - `hash_stream(f, bits, chunk_size=1 << 20)` and
    `hash_path(path, bits)`; `OSError` propagates untouched
  - `CERTIFIED_BITS` public; Scout's `Hash` imports `_CERTIFIED_BITS`
    today and reds when it is renamed
  - Scout never calls blake3 directly
- `lib/scanner.py`: walk and stat only, DFS path order.
  - Yields one record per file: `PPP` path, `size`, `mtime` ns; hash
    computed only when the caller asks for that file
  - Unreadable files and dirs yield an error record (path, reason); the
    walk never aborts
  - Symlinks: not followed, not recorded
  - `lib/fs/dir_reader.py` is unreferenced; keep or delete here
- Rehash decision belongs to the caller, per file, in this order: no
  row, or size or mtime differ, or `rehash_after` is set and `hashed`
  is older than it.
  - `rehash_after` lives in `fs_meta`, default never
  - `scan --rehash` forces every file
- `scout scan [target] [-r repo] [--no-hash] [--comment TEXT]`
  - `Manifest.open`; `--comment` updates `fs_meta.comment`; fs detail
    rows refreshed each run through `MetaRepo`
  - `--no-hash` does a stat-only pass; `hash` and `hashed` stay null
- Run
  - `scans.start()`, then walk in DFS order
  - Per dir: `dirs.add`; per file: `files.get` by `(dir_id, name)`,
    rehash decision, `files.add` with `hashed = started` when hashed
  - Seen `(dir_id, name)` pairs go to a temp table; after the walk,
    `gone = started` on live rows not in it, dirs through
    `dirs.mark_gone` and files through `files.mark_gone`
  - `scans.finish(started, files_seen)`
  - `with manifest:` around every N files, N in the hundreds, so an
    interrupted scan keeps its progress; how the next run treats an
    unfinished `scan` row is decided during implementation
- Output: one porcelain line per file as it is processed; that is the
  MVP progress indicator.
- Errors: `Err.ScanDomain` and its children for the failures met here;
  scanner error records are rendered, not raised.
- Tests
  - Factory trees under `tmp_path`: ordering, stat fields, error record
    on an unreadable file, hash present only when requested
  - e2e: `scout init` then `scout scan` on a factory tree; assert
    `file`, `dir`, and `scan` rows through sqlite3; rerun with one file
    deleted, one modified, one added; assert `gone`, `hashed`, the new
    row
  - Unit: the rehash decision, the gone pass, batched commits
- Acceptance run per `doc/QA.md` before sign-off

### Stamp v0.1.0

- Pin `lib`'s public interface in `test/lib/test_import.py`;
  - the exported names, not just that modules import.
- Acceptance run on a real disk:
  - `init`
  - `scan`
  - checks from `doc/QA.md`
  - observations stated in the PR
- Install run:
  - `uv tool install` from the GitHub repo on a clean machine;
    - `scout --version` and `scout init` on a scratch directory
- Version:
  - `pyproject.toml` and `cli.VERSION` to `0.1.0`, one source
- `CHANGELOG.md` created with the `v0.1.0` entry;
  - CONTRIBUTE's "no CHANGELOG until MVP tags exist" line updated
- Tag `v0.1.0` on `main`;
  - MVP means `init`, `scan`, and raw `sqlite3` queries against the manifest
- Pin `b3c32` to the release that ships the streaming API

## Post-MVP Sequenced

Once this starts clearing up, this becomes the main task/PR sequencer.
Once we're out on MVP we're entering a more opportunistic cadence of development.
Trying workflows at first with raw SQL and basic commands.
Goal is to guage important workflows and find the best UX for this app.
Eventually we'll be doing two things:

- Using `PyO3` with rust modules that replace python ones
  - Eventually reaching a full rust port.
- Outlining a stable plan for `v1.0.0`.

Once we're along on the above two;
a cut in the plan should be made and freeze a path to `v1`.
But once we're in the post MVP cadence turn this into the main task list.
And before the `v1` plan emerges naturally.

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
- First `View`:
  - `PathView` joining `dir` and `file` into host-path rows;
    - The `Repo`/`View` convention is in [CONTRIBUTE](CONTRIBUTE.md).
    - Make sure we carefully exercise the convention here,
      - for learning opportunities.
- Stage role: the source. Produces the stream every other verb consumes.
  `scout ls old.db --gone | scout has nas.db` is the migration check;
  `scout ls old.db photos/ | scout has nas.db` is the per-directory one.
