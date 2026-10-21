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

### scan-cli

Split from the old `scan-cli` on 2026-10-16: output and terminal
handling only; the lib boundary is `### scan-interface`.

- `git checkout -b ft/scan-cli`, from `main` after
  `ref/scan-interface` merges.
- Module map, one role per module:
  - `cli/render/`, a package now: `porcelain.py`, shared `Output`
    (later the `Renderer` protocol and `select_renderer` from the
    Renderer section below); future `rich.py`, `json.py` siblings;
  - `cli/emit.py`, the emitter object: a pure router between the
    renderer and the progress sinks (verbosity policy lives here;
    no counting, no terminal mechanics);
  - `cli/progress.py`, the status-line object, the only module
    that knows what a carriage return is.
- The manifest is THE primary operand:
  - first positional on every verb, default `"."`;
  - all interpretation in `Manifest.open` (resolve, dir to
    `.scout.db`); positional meaning never depends on disk content;
  - eliding it with later operands means typing `.`
    (`scout ls . doc/finance`), accepted friction;
  - `-r`/`--repo` kept as an explicit override to dogfood both
    modes; flag wins when both are given (most intentional wins),
    disagreement gets a stderr note; loser removed on evidence;
  - verb operands follow: `init [manifest] [root]` (root defaults
    to the manifest's dir); finer operand patterns left to
    dogfooding.
- `-v`/`--verbose` and `-p`/`--progress` are different concerns:
  - verbose = stdout content policy (emitter routing): whether
    per-file events become porcelain records downstream tools see;
    automation may want it;
  - progress = stderr human feedback (the progress sink): never
    data, gone when stderr is not a tty; automation never wants it;
  - independent and freely composed; humans often want both.
- Single-letter args pair with a long alias where no collision:
  `-v`/`--verbose`, `-p`/`--progress`.
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
- Gaps from `ref/scan-interface`:
  - `run_scan(path, opt, make_emit)`: the tally `make_emit` receives
    has no reader yet; the `cli/emit.py` constructor becomes `make_emit`
    and hands that tally to the status line.
  - `-v` and `--progress` reach the emitter inside the callback's closure;
    pass a frozen display choice instead if that wiring is tested as data.
  - `TestScanCommand._invoke`, `test_default_prints_summary_only`,
    `test_verbose_prints_file_lines` and `test_progress_writes_file_counts`
    pin `_emitter`; they move to `cli/emit.py`'s tests when it replaces it.
  - Module-name imports: `event` for one module, `lib_event` and
    `cli_event` where both meet; CLI modules still import `events`,
    `test/lib/scan/test_scan.py` rebinds names from `mod_scan`,
    and `import scout.lib.error as Err` gets the same convention.
- Doc commit: `doc/architecture.md` gains the adapter architecture,
  as settled by the 2026-09-22 discussion round.
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
