# Contributing to Scout

This document is the canonical statement of how work is done in this
repository: how changes are planned, tested, committed, and documented.
Other documents point here rather than restating these rules.

How a change is verified before it is submitted lives in `doc/QA.md`.
This document does not restate it.

Scout produces `.scout.db` files: SQLite manifests of a file tree.
It consumes nothing from other projects at runtime except the `b3c32`
package, which provides the hash every manifest is keyed on.
`depo` and `normpic` use the same hash, and Datasette can open a
manifest directly.
The manifest schema will be documented as a contract once live manifests
exist; until MVP it is a moving target and lives only in code and
`doc/TODO.md`.

## Security

Scout reads file metadata and hashes file contents.
It never prints, logs, or stores file contents.
Only the hasher reads them and only emits hashes.
A path found in a manifest is never opened outside `scan` and `verify`.

Do not read or enumerate shell environment variables.
When a configuration stack exists, it reads environment variables only
through one declared white list of `SCOUT_` prefixed environment var names.

## Ways of working

Every change moves through the same path: plan, review, implement,
verify, submit.
Three roles participate:

- Author: writes the plan, implements it, and reports a short summary
  after each commit.
- Reviewer: signs off the plan before any code is written, and runs
  quality assurance before the change is submitted.
- Maintainer: sets direction, approves, and merges.

Plan-first is the rule.
No implementation begins before the plan is signed off.
The plan is the baseline that review grades against.

The collaborator never has repository access.
All shell output moves by clipboard relay in either workflow below.

In the review workflow, an author and a reviewer are separate parties.

In the stub workflow, the maintainer holds author and maintainer, and
the collaborator holds reviewer.
The collaborator supplies stubs, edit guidance, and diagnosis; the
maintainer writes every test body and implementation.
A stub is a signature and a docstring, nothing else.
One atomic step per exchange, confirmed before the next begins.
This is the default workflow for this repository.

## Planning

`doc/TODO.md` is the tracker.
A change begins as a written plan there: a branch-named section with an
ordered task list precise enough that following it top to bottom
produces the change.

A well-formed plan:

- Opens with the branch task: `git checkout -b <prefix>/<slug>`.
- Runs as spec-then-test-then-implement cycles grouped by behavior.
  Each cycle names the test that pins it.
- Names the test that closes every known boundary or trap.
- States scope concretely: which files change and roughly how much.
- Closes with a `Doc:` commit for any documentation the work changed or
  invalidated, then a `Pln:` commit deleting the completed tasks from
  `doc/TODO.md`.
  Either is skipped only deliberately.

A plan whose cycles all land in one function has the wrong units;
decompose first.
A parameter that exists only to branch behavior is a missing type.

### Salvage

`salvage/test/` holds the pre-MVP test suite, uncollected.
Every PR states, for the modules it touches, what it moved back, what it
rewrote and deleted, and what it left.
This rule ends at MVP, when `salvage/` is empty and deleted.

## Test-driven development

Test-driven development is the default for any change that alters
behavior.

The rhythm for a PR is e2e first:

1. Write the e2e test for the PR's feature, marked skip.
   It touches only the CLI (`CliRunner`, in-process) and the DB file
   (`sqlite3`), never `scout.lib` internals.
2. Identify the unit tests it depends on.
   Green them red-green-refactor, one behavior at a time.
3. Unskip the e2e test.
   It going green is the PR's acceptance step.

Red-green-refactor is a working rhythm, not a commit boundary.
Every commit leaves the suite green.
e2e tests carry `@pytest.mark.e2e`; `just test -m "not e2e"` is the
fast loop.

Before writing tests, read the `conftest.py` files covering the area
and the neighboring test modules.
Fixtures are the zero-arg calls of factories with override kwargs; a
test that rebuilds a factory's work inline is a duplicate.
No pyfakefs; real files under `tmp_path`, and factory trees stay tiny.

### Renderers

Porcelain output is pinned by string comparison against fixed records.
Subcommand tests use a collecting fake renderer and assert on records,
not text.
Rich output, when it exists, is pinned structurally, never visually.

## Commits and branches

### The pre-commit gate

`just check` runs, in this order, and all must pass:

1. `uv run ruff check`
2. `uv run pyright`
3. `uv run pytest` (the full suite)

The justfile follows this list; a change here changes the recipe.
Use `uv run`, never a bare interpreter.

### Commit sizing

A commit is a self-contained unit of related work that leaves the tree
green.
Around 300 lines is a soft ceiling; two to eight commits per change is
typical.
A coherent, slightly larger commit beats fragmenting one behavior.

### Commit message format

The `commit-msg` hook in `script/hook/` enforces:

- Title at most 50 characters, `<Prefix>: <Title>`, capital after the
  prefix.
- Body lines at most 72 characters, `-` bullets with nested detail,
  even indentation, no wrapped continuation lines.
- No signature block: no emoji, links, or co-authored-by lines.

Prefixes: `Pln:` planning, `Ft:` feature, `Fix:` bug fix, `Ref:` pure
refactor, `Doc:` documentation, `Chr:` chore, `Tst:` test-only.

### Branch names

Lowercase prefix, slash, kebab-case slug: `ft/scanner`,
`ref/drop-dir-ancestor`.
Refer to other work by branch name, never by position.

## Coordinating work across roles

- Shell output moves by clipboard relay; requests are targeted greps
  and line ranges, not whole files.
- One command per exchange.
  Multi-command blocks are brace-grouped before the pipe.
  Never `cd`; never inline comments in shell commands; never heredocs.
- A commit is delivered as the `git add` command and the commit message
  in separate fences.
  The coordinator writes commit messages.
- Every proposed change is a fenced block with its destination stated
  next to it: the file path and enough context to place it.
- A fence containing a fence uses four or five outer backticks.
- Terse and direct; one topic at a time; questions front-loaded, never
  batched; one concrete recommendation, not a menu.
- State a correction once and move on.
- Escalate scope changes; never fold them in silently.
- Read the code before making a claim about it.

### Relay hygiene

Shell output reaches the collaborator through `cc`, a local clipboard
utility that copies stdin to the system clipboard.
*(`OSC 52`, falling back to `wl-copy` under Wayland)*.
Every command the collaborator provides ends in `| cc`, with stderr
merged (`2>&1 | cc`) when failure output matters.

- Bound all output before it runs: pipe through `head`, `tail`,
  `wc -l`, or a count.
  One unbounded command can flood the channel.
- When the answer is "did this differ", relay the count, not the
  difference.
- Never `cd`; run everything from the repository root.
- Brace-group multiple commands before the pipe so an intermediate
  failure halts loudly.

## Style and formatting

Applies to all text: code, comments, docstrings, commit messages, PR
descriptions.

- ASCII only.
  No em dashes, arrows, emoji, or decorative Unicode.
- Line length: commit body 72, prose 80, Python 88.
- In prose, sentence-ending punctuation is followed by a newline.
- Singular directory names: `doc/`, `test/`, `script/`.
- Nested bullet lists for related detail.

Code conventions:

- Functions carry a docstring.
  Code is type annotated; dataclasses for basic data structures.
- Stored paths are `PurePosixPath`, imported as
  `from pathlib import PurePosixPath as PPP`.
  `Path` exists only at the I/O boundary (scanner, `DBConnector`, CLI).
- Errors: `import scout.lib.error as Err`.
  - One hierarchy rooted at `Err.ScoutDomain`;
    - every class is constructible as `Cls("message")`.
  - Names ending in `Domain` group errors and...
    - may carry optional members their children inherit;
    - concrete errors carry no `Error` suffix.
  - `Err.ScoutUnknown.wrap` rewraps what Scout did not predict.
  - The lib raises;
    - `scout_command` catches at the subcommand and maps to message and exit code.
- Tables:
  - a `Repo`:
    - owns one table,
    - reads and writes,
    - and holds its `SCHEMA` and every statement as class constants;
    - `WHERE` fragments are literals in the class and values are always bound.
  - A `View` reads across tables, never writes, and is named for what it returns.
- Transactions:
  - repos never commit.
  - `DBConnector` holds one auto-commit connection;
  - `with manifest:`
    - is the only transaction boundary.
- Timestamps in manifests are int64 nanoseconds since the epoch, UTC.
  - One conversion on the way in;
    - every comparison is an integer compare.
  - `file.hashed` and every `gone` hold a `scan.started`.
- Match neighboring files.
  - Verify a library is available before using it.
- PEP 8 and ruff defaults.
- Every module opens with its repo-relative path as comment,
  - then a docstring:
    - one-sentence purpose,
    - `Author:`, `Created:` (ISO date),
    - `License: AGPL-3.0-or-later`, there's only one for whole repo
    - `Revision:` as a list of ISO dates when the module has been majorly revised.

## Documentation discipline

A fact lives in exactly one place.
Facts Scout owns live in one document each under `doc/`, and other
documents link there.

Every document is reachable from the root README through a chain of
links.
`README.md` links only to `doc/README.md`.
Each directory's README indexes its peers and links one level down to
subdirectory READMEs, never deeper.

Documentation is written for a developer reading it cold.
Never reference assistant sessions or conversation structure.

`doc/TODO.md` is append-and-prune.
Find a section with `grep -n`, view a few lines, edit surgically.
The final commit of a change deletes its completed task lines.
There is no CHANGELOG until MVP tags exist.
