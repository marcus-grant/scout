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
- The manifest is the primary operand of every verb: first
  positional, default `"."`; `Manifest.open` owns interpretation;
  `-r`/`--repo` as override while dogfooding judges both modes.
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

### scan-dir

>**NOTE**: Scan's parts that are likely shared by later operations are
>built in `src/scout/lib/scan/` grouped by subject, provisionally; see
>ROADMAP `### verb-architecture`.

- `git checkout -b ref/scan-dir`
- Mechanical moves first, each its own commit:
  - `src/scout/lib/scan.py` to `src/scout/lib/scan/__init__.py`,
    `test/lib/test_scan.py` to its mirror under `test/lib/scan/`;
  - lib `force` renamed `rehash` (the CLI flag is already `--rehash`);
  - `FileRepo.add` renamed `upsert`, then `DirRepo.add`;
    - the `sed` matches `files.add(`, `dirs.add(`, `def add(` only.
- Events in `src/scout/lib/event.py`, bare unprefixed `Event` base:
  - `FileScanned`, `RecordGone`, `ReadFailed` replace `Scanned`,
    `Gone` and the bare `Err.Unreadable` yields;
  - `run_scan`'s `match` follows.
- `src/scout/lib/scan/file_stats.py`:
  - `reconcile_file_stat` takes `dir_id`, the `FileStat`, the
    `FileRecord` or None, `hash`, `rehash`;
    - returns `FileStatReconciliation`: the `RecordChange` and the
      intended `FileRecord`, or None when there is nothing to write;
    - a None `hash` in the intended record means no hash is known;
  - the loop calls `hash_file` when hashing is on and the intended
    hash is None;
  - `apply_file_stat` writes the intended record via `files.upsert`.
- `src/scout/lib/scan/missing_files.py`:
  - `reconcile_missing_files` takes the `WalkedDir` and the dir's
    live records, returns `MissingFilesReconciliation`: the names
    missing from the filesystem; names that failed to stat count as
    present;
  - `apply_missing_files` takes the `dir_id` and those names, marks
    each via `files.mark_gone_one`.
- Per-directory loop: upsert the dir, read its live records,
  reconcile, hash, apply, report; stages called side by side, never
  nested.
- Testing rules, acceptance criteria for the structure:
  - a reconciliation is tested with no fixture, no filesystem and no
    monkeypatch, over every outcome, including rehash of a match;
  - an apply arranges at most one record via the `manifest` fixture;
  - needing more arrangement means the function is drawn wrong:
    redraw it before writing the body.
- `src/scout/lib/scan/__init__.py` docstring: its sibling modules are
  likely-shared parts living here for now; see `doc/architecture.md`.
- Doc commit: `doc/architecture.md` records the scan package's
  modules and provisional home, the stages observe, reconcile, apply
  and report, lib events unprefixed, and `upsert` where `add` is named.
- Pln commit: delete this section.

### scan-missing-dirs

- `git checkout -b ref/scan-missing-dirs`, from `main` after
  `ref/scan-dir` merges.
- `src/scout/lib/scan/missing_dirs.py`:
  - `WalkedPaths`: paths walked and paths unreadable (an unlistable
    dir's path, each failed entry's path), filled by the per-directory
    loop; replaces `scan`'s `walked` and `unreadable` locals;
  - `reconcile_missing_dirs` takes `WalkedPaths` and the recorded live
    dir paths, returns `MissingDirsReconciliation`: the topmost
    recorded dirs missing from the filesystem, excluding unreadable
    paths and anything under one;
  - `apply_missing_dirs` marks each via `manifest.gone_subtree.mark`,
    returning the paths in `mark`'s order.
- `AccessLost` in `src/scout/lib/event.py`:
  - for an unlistable dir, emitted beside `ReadFailed` when
    `manifest.claimed.counts(path)` shows claims under it;
  - nothing is written; where the "claims anything" check lives is
    decided at the stub.
- `GoneSubtree.mark` docstring: "deepest dirs last" becomes "every
  file first, then every dir, each in path order".
- Candidates still come from `dirs.descendants(PPP("."))` in memory;
  SQL path-prefix candidate selection is out of scope.
- Testing: the reconciliation with path sets only (a missing parent
  and child yield the parent; a dir under an unreadable one, and a
  failed entry, are excluded); the apply with one recorded subtree.
- Doc commit: `doc/architecture.md` records `missing_dirs.py`.
- Pln commit: delete this section.

### scan-session

- `git checkout -b ref/scan-session`, from `main` after
  `ref/scan-missing-dirs` merges.
- `ScanTally` and `Summary` in `src/scout/lib/scan/tally.py`:
  - `see(event)` before each yield; `summary(started, finished)`
    freezes it; one instance per run;
  - `scan(..., tally=None)`: an adapter injects it for live progress,
    consumed by `### scan-cli`.
- `ScanOptions` in `src/scout/lib/scan/options.py`, frozen:
  - `hash`, `rehash`, `bits`, `batch_size`, `on_progress`;
  - unpacked by `scan`; no reconciliation or apply ever sees it.
- `update_from_walk` takes the `Manifest`, an iterable of `WalkedDir`
  and the settings it needs; runs the per-directory loop, then the
  missing-dirs pass; calls `manifest.wrote()` after each file write.
- `scan` reduced to the session: exclude its own file,
  `scans.start`, `commit_every(batch_size)` around
  `update_from_walk(manifest, walk(root, exclude), ...)`,
  `scans.finish`, the `Summary` yield.
- `test/lib/scan/` tests:
  - the `TestScan` cases that monkeypatch `os.scandir` or `walk`
    pass constructed `WalkedDir` values to `update_from_walk`;
  - `test_rescan_after_rmtree_marks_subtree_gone` stays real-disk;
  - `listing` locals renamed `walked`.
- The fs-detail refresh and its `detail=` seam stay in `run_scan`.
- Doc commit: `doc/architecture.md` gains the operation anatomy and
  the provisional-home tension, pointing to ROADMAP.
- Pln commit: delete this section.

### scan-cli

CLI architecture settled 2026-09-22 (discussion round two):
>**NOTE**: This task is likely too big for one PR, plan a split if needed

- Module map, one role per module:
  - `cli/event.py` — the `CliEvent` family only;
  - `cli/render/` — a package now: `porcelain.py`, shared `Output`
    (later the `Renderer` protocol and `select_renderer` from the
    Renderer section below); future `rich.py`, `json.py` siblings;
  - `cli/emit.py` — the emitter object: a pure router between the
    renderer and the progress sinks (verbosity policy lives here;
    no counting, no terminal mechanics);
  - `cli/progress.py` — the status-line object, the only module
    that knows what a carriage return is;
  - `subcmd/*` — flag parsing and wiring only.
- Event naming: two grammars, no renames —
  - cli events are `<Verb><Fact>` (`ScanFile`, `ScanGone`);
  - lib events are subject + past participle (`FileScanned`);
  - the translate layer reads as a visible grammar shift.
- The manifest is THE primary operand:
  - first positional on every verb, default `"."`;
  - all interpretation in `Manifest.open` (resolve, dir →
    `.scout.db`); positional meaning never depends on disk content;
  - eliding it with later operands means typing `.`
    (`scout ls . doc/finance`) — accepted friction;
  - `-r`/`--repo` kept as an explicit override to dogfood both
    modes; flag wins when both are given (most intentional wins),
    disagreement gets a stderr note; loser removed on evidence;
  - verb operands follow: `init [manifest] [root]` (root defaults
    to the manifest's dir); finer operand patterns left to
    dogfooding.
- Flags: `--rehash` → `rehash`, `--no-hash` → `not hash`
  (identity mapping after the rename).
- `-v`/`--verbose` and `-p`/`--progress` are different concerns:
  - verbose = stdout content policy (emitter routing): whether
    per-file events become porcelain records downstream tools see;
    automation may want it;
  - progress = stderr human feedback (the progress sink): never
    data, gone when stderr is not a tty; automation never wants it;
  - independent and freely composed; humans often want both.
- Single-letter args pair with a long alias where no collision:
  `-v`/`--verbose`, `-p`/`--progress`.
- Translate layer updated to the lib `Event` family;
  - `outcome` to `change` at the `ScanFile` mapping;
  - `run_scan` shrinks to open manifest, wire emitter, iterate.
- Known bug: `_emitter`'s `--progress` count increments on every
  `ScanGone`, which includes swept dir paths, so "N files" overcounts;
  the status line reading `ScanTally` replaces that count.
- One status-line object owning stderr under `--progress`:
  - `log(line)` erases, writes, redraws; `status(...)` redraws at
    most every 500 ms;
  - spinner and byte count latency-gated: the first b3c32 callback
    (`interval_ms=1000`) is itself the evidence of a slow file;
    the file's completion event ends slow-file mode;
  - final erase, no redraw, before the summary renders: nothing
    carriage-returned survives into the log;
  - not a tty: plain interval-throttled lines, no CR, no spinner;
  - the CLI reads the injected `ScanTally` live for its numbers;
    `expected` totals (db estimate `~N`, or an exact pre-scan count
    behind a flag) are display-side and deferred until dogfooding
    asks;
  - porcelain rule stands: renderers produce lines; this object is
    the terminal handling around them; Rich later replaces it
    behind the same `log`/`status` seams.
- Doc commit: `doc/architecture.md` gains the adapter architecture,
  as settled by this PR's preceding discussion round.

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
