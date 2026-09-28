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
- Verbs emit typed events; renderers turn them into output.
  - One frozen `CliEvent` dataclass per fact, fields lib types,
    never pre-formatted strings.
  - A renderer is one callable: event in, `Output` line tuples out.
  - Click callbacks are thin wiring over `run_<verb>` handlers
    taking an `emit` callable; tests collect events, not text.
  - Error placement follows CONTRIBUTE's error rules.

### Restructure conventions

Staging: each restructure PR's doc commit moves its lines from here
into `doc/architecture.md`; this block empties as the sequence lands.

- Naming:
  - producer-prefixed product types (`WalkedDir`, `ScanEvent`);
  - past tense for completed observations and happenings
    (`WalkedDir`, `FileScanned`, `AccessLost`);
  - `Err.*` names conditions (`Unreadable`), its own family style;
  - event grammars per layer: cli `<Verb><Fact>` (`ScanFile`),
    lib subject + past participle (`FileScanned`).
- The manifest is the primary operand of every verb: first
  positional, default `"."`; `Manifest.open` owns interpretation;
  `-r`/`--repo` as override while dogfooding judges both modes.
- Error placement and the repo / `View` / service taxonomy:
  moved to `doc/CONTRIBUTE.md` (2026-09-22), no longer staged here.
- Promotion on second independent consumer, for types and functions;
  - one taken exception: `RecordChange` moved to models with `check`
    merely named, to avoid verb-imports-verb.
- History stays `gone` and `hashed`; unreadable subtrees leave no db
  trace — report loudly (`AccessLost`), record nothing.

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

### manifest-package

- `git checkout -b ref/manifest-package` from `main`.
- `Fix:` `DirRepo._UPDATE_GONE` gains `gone IS NULL`.
  - Marking a parent gone overwrote an already-gone child's `gone`.
  - Pin: `test_keeps_earlier_gone` in `test/lib/repo/test_dir_repo.py`.
- `Fix:` `FileRepo.mark_gone` chunks `dir_ids`.
  - One `?` per id overflows SQLite's bound-variable limit.
  - Chunk at a class constant below 32766, the upstream default.
  - Pin: a test in `test/lib/repo/test_file_repo.py`,
    - with the constant patched small.
- `Ref:` `lib/manifest.py` becomes `lib/manifest/__init__.py`.
  - `test/lib/test_manifest.py` becomes
    `test/lib/manifest/test_init.py`.
- `Ref:` `lib/repo/` becomes `lib/manifest/repo/`.
  - `test/lib/repo/` becomes `test/lib/manifest/repo/`.
  - `scout.lib.repo` becomes `scout.lib.manifest.repo`,
    - by whole-word `sed`, previewed per file.
- `Doc:` Layers list in `doc/architecture.md`.
- `Pln:` delete this section.

### manifest-services

- `git checkout -b ref/manifest-services` from `main`,
  - after `ref/manifest-package` merges.
- Kinds under `lib/manifest/`:
  - repos in `repo/`, services in `service.py`, queries in `query.py`;
  - a flat module becomes a package once populated enough;
  - query replaces the `View` term.
- Ownership: queries join across tables; repos alone write.
  - Ids pass as Python values;
    - leaves room for a read-through id cache repos invalidate.
- `Ref:` `GoneSubtree(dirs, files)` as `manifest.gone_subtree`:
  - `mark(path, started) -> list[PPP]`, the body of `_mark_dir_gone`;
  - completes its writes when called, no generator;
  - order as produced: files, then dirs in path order;
  - `scan` wraps each returned path in `Gone`;
  - `TestMarkDirGone` becomes `TestGoneSubtree`,
    - in `test/lib/manifest/test_service.py`.
- `Ft:` `PresentSubtree(db)` as `manifest.present_subtree`:
  - `counts(path) -> PresentCounts(dirs, files)`, backing `AccessLost`;
  - two `COUNT(*)` statements over `DirRepo`'s two where fragments,
    - which become public in this commit;
  - dirs strictly under path; files in path and in those dirs;
  - bounds provisional until seen running.
- `Ft:` `Manifest.open` resolves `path` first.
  - `run_scan` drops its own `path.resolve()`.
- `Ft:` `manifest.commit_every(n)` returns `CommitBatcher`:
  - enter begins; `tick()` commits and begins again at `n`;
  - exit commits when clean, rolls back on exception,
    - including `GeneratorExit` from an abandoned `scan`;
  - no resume feature: rerunning scan is cheap (`MATCHED` skips hash).
- `Ref:` `scan` uses `manifest.commit_every(batch_size)`.
  - Ticks on `ADDED` and `UPDATED` only, never `MATCHED`.
  - `_Batch` deleted.
- Deferred to a later PR:
  - `PresentSubtree.files(path)`, one join,
    - replacing the per-dir `files.in_dir` N+1 in `GoneSubtree.mark`;
  - repo docstrings say present instead of live.
- `Doc:` `doc/architecture.md` gains the repo / service / query trio,
  - the composition rule and the ownership rule;
  - CONTRIBUTE: `View` becomes query,
    - its `Subtree` example becomes `GoneSubtree`;
  - `### scan-restructure` lines naming `manifest.subtree` updated.
- `Pln:` delete this section.

### scan-restructure

>**NOTE**: This task is likely too big for one PR, plan a split if needed

- `ScanEvent` base family, mirroring `cli.event`:
  - `FileScanned(path, record, change)`, `RecordGone(path)`,
    `AccessLost(path, dirs, files)`, `ReadFailed(path, error)`;
  - each subclass docstring states meaning and expected handling;
  - `Summary` stays outside as the terminal yield;
  - `AccessLost` is derived reporting: emitted beside `ReadFailed`
    when prior live records exist under an unreadable dir;
    the db is never written for unreachability.
- `ScanOptions`, frozen, flat:
  - `hash`, `rehash`, `bits`, `batch_size`, `on_progress`;
    - `rehash` renames lib's `force`: two tiers of one policy
      (`hash` = hash new/changed; `rehash` = hash even MATCHED);
      same word-family as the future `rehash_after` budget;
      dogfooding may revisit the name;
  - field defaults are the program-defaults layer of the future
    config fold (`dataclasses.replace` per source, partial layers);
  - context options (which manifest, streams) are adapter-side and
    never reach the verb.
- `_should_hash(change, record, opts)`:
  - the one expensive decision (whole-file read on slow media),
    isolated, exhaustively unit-testable.
- `_scan_file`/`_scan_dir` → `_reconcile_file`/`_reconcile_dir`:
  - assemblies stay private to scan; verbs share atoms, never
    assemblies;
  - the hash-and-write step inside `_reconcile_file` stays a
    separable function *(future home: a service component, when a
    hash-fill verb arrives)*.
- `_reconcile_tree`: the routing loop:
  - coverage sets `covered` and `unreadable`; dispatch to
    `_reconcile_dir` / `_handle_unreadable`; sweep last;
  - `_handle_unreadable` yields `ReadFailed`, derives `AccessLost`
    via `manifest.subtree.live_counts`, writes nothing;
  - `_sweep_unwalked`: candidates via repo SQL (path-prefix, not
    Python `parents` loops), marks via `manifest.subtree.mark_gone`,
    yields `RecordGone`.
- `ScanTally`, public, injectable: `scan(manifest, opts, tally=None)`:
  - `see(event)` before each yield, so the live instance is exactly
    current through the last yielded event;
  - `summary(started, finished)` freezes it into `Summary`;
  - one instance per run, shared with the adapter by injection.
- `scan()` recomposed: session bracket
  (`scans.start`/`finish`), `with manifest.commit_every(...)`,
  tally + tick + relay loop, terminal `Summary`; no other logic.
- The fs-detail refresh (`fs_meta.read_all` +
  `meta.write_fs_detail`) moves from `run_scan` into `scan()`'s
  session opening — it is part of the verb, not adapter work;
  `run_scan`'s `detail=` test param dies with it.
- `test/lib/test_scan.py` locals still say `listing` for a `WalkedDir`.
  - Rename them to `walked` when the module is reassessed.
- `test/lib/fs/test_walk.py` holds `_fail_scandir` and `_fail_stat`.
  - If the reassessed `test/lib/test_scan.py` needs them,
    - hoist them into `test/factory.py`;
    - replacing its own inline `os.scandir` fake.
- Doc commit: `doc/architecture.md` gains verb anatomy
  (atoms / policy / assembly), the event-family pattern, and the
  options shape with its config-fold constraint.

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
- Translate layer updated to the `ScanEvent` family;
  `outcome` → `change` at the `ScanFile` mapping;
  `run_scan` shrinks to open manifest → wire emitter → iterate.
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
