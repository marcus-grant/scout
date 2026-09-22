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
- The manifest schema is versioned via `meta.schema_version`.
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
- Verbs emit typed events; renderers turn them into output.
  - One frozen `Event` dataclass per fact, `<Verb><Event>`, fields
    are lib types, never pre-formatted strings.
  - A renderer is one callable, event in, `Output` out: `out` and
    `err` line tuples the subcommand echoes.
  - Click callbacks are thin wiring over `run_<verb>` handlers
    taking an `emit` callable; tests collect events, not text.
  - Failures raise through `scout_command`, never events.

### Required reading

- `doc/CONTRIBUTE.md` (mandatory)
- `doc/QA.md` (mandatory)
- `README.md` (optional)

## Sequenced PRs to MVP

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
    Later the configuration stack (args, env, config file, `meta`, in
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
