# Scout - TODO

## Before Working

### Crucial rules

- Strict lib/adapter split.
  - `lib/` emits typed records/events and never formats or prints.
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
  - `just check` passes before a PR is opened.
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
- Verbs emit typed events; renderers turn them into output.
  - One frozen `CliEvent` dataclass per fact, fields lib types,
    never pre-formatted strings.
  - A renderer is one callable: event in, `Output` line tuples out.
  - Click callbacks are thin wiring over `run_<verb>` handlers
    taking an `emit` callable; tests collect events, not text.
  - Error placement follows CONTRIBUTE's error rules.
- Before building a new operation, read `src/scout/lib/scan/` for parts
  it would share.
  - Scan's likely cross-cutting parts live in its package provisionally.
  - A shared part moves out of scan's package;
    it is never imported across operations or copied.
  - Record the move against ROADMAP `### verb-architecture`.

### Restructure conventions

Staging: each restructure PR's doc commit moves its lines from here
into `doc/architecture.md`; this block empties as the sequence lands.

- Naming:
  - producer-prefixed product types (`WalkedDir`);
  - past tense for completed observations and happenings
    (`WalkedDir`, `FileScanned`, `AccessLost`);
  - `Err.*` names conditions (`Unreadable`), its own family style;
  - event grammars per layer: cli `<Verb><Fact>` (`ScanFile`),
    lib subject + past participle (`FileScanned`);
  - event bases: the lib's is unprefixed `Event`, adapters prefix
    theirs (`CliEvent`).
- The manifest is given by `-r`/`--repo`, default `"."`,
  a manifest file or the directory holding `.scout.db`;
  `Manifest.open` owns interpretation.
  - Positionals are paths under the root,
    so `scan` stays free to take subtree operands later.
  - Provisional: dogfooding judges it against a manifest positional;
    verbs whose subject is manifests (`comm`, `diff`) may keep them.
- Error placement and the repo / query / service taxonomy:
  moved to `doc/CONTRIBUTE.md` (2026-09-22), no longer staged here.
- Promotion on second independent consumer, for types and functions;
  - one taken exception: `RecordChange` moved to models with `check`
    merely named, to avoid verb-imports-verb.
- History stays `gone` and `hashed`; unreadable subtrees leave no db
  trace: report loudly (`AccessLost`), record nothing.

### Required reading

- [`doc/CONTRIBUTE.md`](CONTRIBUTE.md) *(mandatory)*
- [`doc/QA.md`](QA.md) *(mandatory)*
- [`README.md`](../README.md) *(optional)*
- [`doc/architecture.md`](architecture.md) *(optional)*

## Sequenced PRs to MVP

MVP means dogfood-ready: `init`, `scan`, and raw `sqlite3` queries,
where scan survives terabyte-scale runs and the code can be
introspected when dogfooding surfaces problems.
The restructure sections below come first, in order.

### import-convention

A small cleanup PR: every module imports lib and CLI modules by name,
as `event` already is, so each name shows where it comes from.

- `git checkout -b ref/import-convention`, from `main` after
  `ft/scan-cli` merges.
- `import scout.lib.error as Err` becomes `from scout.lib import error`,
  used as `error.<Name>`.
  - Before any `sed`, find every local, parameter and field named `error`;
    a colliding local becomes `err`.
  - `ReadFailed.error` and `ScanError.error` keep their field names;
    check how pyright reads a field annotated through the module.
  - One identifier per commit, previewed with a per-file count.
- The remaining `import scout.cli.event as events` imports become
  `from scout.cli import event`; colliding locals become `evt`.
- `test/lib/scan/test_scan.py` stops rebinding names from `mod_scan`.
- Doc commit: CONTRIBUTE's error rule and examples follow the new import.
- Pln commit: delete this section.

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
- Chore, its own commit before the version bump:
  `requires-python` becomes `>=3.13`, the minimum tracking Debian stable;
  `just check` must pass on 3.13.
- Chore: remove the unused dependencies,
  `attrs` from `dependencies` and `pyfakefs` and `pytest-mock` from the
  dev group, then `uv lock`; nothing in `src/` or `test/` uses them.
- Empty `### Restructure conventions` in `doc/TODO.md`,
  since the restructure sequence has landed:
  - delete the lines another doc already states
    (error placement, promotion, history and `AccessLost`);
  - first move what no doc states yet
    (the naming rules, the manifest operand convention)
    to `doc/architecture.md` or the crucial rules, in the stamp's `Doc:` commit.
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

- `scout has [manifest] [< hashes]`: read hashes from stdin (one per line,
  or porcelain records whose hash column is used), one query via a temp
  table join, print found/missing per hash with the path where found.
  Non-zero exit if any are missing, like `md5sum -c`.
- Stage role: the filter. Takes any hash-bearing stream and splits it by
  presence in a manifest. Answers "does this content exist anywhere on
  the NAS" in batch. The most likely promotion to MVP.

### ls

- `scout ls [manifest] [prefix] [--gone]`: print live rows from the
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
