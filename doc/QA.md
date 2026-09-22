# Quality assurance in Scout

This document is the source of truth for how a change is verified
before it is submitted.
`doc/CONTRIBUTE.md` holds the conventions a change must satisfy.
This document holds how those claims are checked; read that first.

Quality assurance is a gated, progressive hunt.
The green suite already proves mechanical correctness, so review never
re-derives that the code works.
Review spends its effort only on what tests cannot catch: a misread
plan, a missing but required thing, a false claim, and a latent trap.

Review grades commits against the approved plan in `doc/TODO.md`, not
against intent reconstructed during review.
It starts from the author's summary, before any repository read.

## The author's summary

After each commit the author reports four things: the commit subject,
one sentence of what changed, pass or fail, and a `git diff --stat`.

No diff, no plan restatement, and no test output unless asked.

The stat is required, because scope drift is invisible in prose.

The author escalates to full detail unprompted on any of: a test
failure, scope expanding beyond the plan, documentation found to
contradict the code, or an ambiguous schema or hash question.

## The ladder

Checks run cheapest first.
Each rung runs only when its failure is possible for this change,
which is read from the plan and the summary.
A rung that cannot fail here is skipped.
When the cheap rungs come back clean and no expensive rung applies,
the hunt short-circuits and the change is signed off.

- Summary against plan: does it describe doing what was approved?
  Watch for overclaims ("complete", "all", a specific count) and for
  scope beyond the plan.
- Scope, from the stat: only the expected files, and a size
  proportionate to the plan?
  A disproportionate diff is stopped and flagged before any content is
  read.
- Signatures, by targeted diff or grep: is the substantive change the
  one that was specified?
- Claims against ground truth, only when the summary makes a
  falsifiable claim: verify it against the repository.
- Completeness, only when the change contributes to a defined set
  discovered dynamically (a repo's `SCHEMA`, a renderer, a subcommand
  registered on the Click group): check the set against its inventory.
- Trap reasoning, only when the change touches correctness-bearing
  logic: vacuous conditions, boundary inputs (empty tree, empty file,
  unreadable file, a path that reappears), one defect producing many
  errors, and misattributed errors.

## The probe kit

A few load-bearing reads, around fifteen lines total, confirm that
tests assert what was intended.
Derive the targets from the plan:

- Grep `assert` in the changed test files to read the assertions, not
  just the test names.
- Grep the source for the removed or old token (`md5`, `dir_ancestor`,
  `os.path`) to confirm the migration is complete.
  Empty output is the proof.
- Grep `test/` for the newly introduced symbol to confirm a test
  exercises it.
- Grep the changed test files for hardcoded literals in assertions.
  A literal the test does not care about is often a configuration line
  not yet written.

Use `grep -I --include='*.py'`.
Exclude `__pycache__/` & `.venv/`.

These greps confirm the tests assert the right thing for the cases
they name.
Extending confidence past those cases requires independent ground
truth: a real disk, or a hash computed by `b3c32` outside the code
under test.

## Deleting a rung

When a failure mode recurs, push it into a suite assertion so it goes
red on its own.
A recurring manual check is a missing test.

## Per-commit sign-off on fragile changes

Most changes are signed off as a unit.

A dependency-ordered change whose commits are preconditions for one
another is signed off per commit: plan the one commit, sign off,
implement, summarize, sign off, commit, then the next.
The schema and repo refactor PRs have this shape.
Where such an ordering exists, encode it in the tracker section with
the reason each step precedes the next.

## The manual acceptance run

The green suite proves mechanical correctness on factory trees.
It does not prove `scan` behaves on a real disk.

For changes touching `scan`, the scanner, or the schema, the maintainer
runs `init` and `scan` on a real directory before final sign-off.
Small first, then a larger one.
Check what factories cannot:

- Row count in `file` against `find <root> -type f | wc -l`.
- One hash spot-checked against `b3c32` run by hand on the same file.
- A rerun with no changes writes nothing and reports nothing changed.
- A rerun after deleting one file sets `gone` on exactly that row.
- The manifest opens cleanly in `sqlite3` and, if installed, Datasette.

State in the submission that this run happened and what was observed.
"Worked" is not an observation; name what was checked.

## The reviewer's own analysis is not exempt

A hazard the reviewer infers is a claim, not a finding, until checked
against the code.
If the code already answers a question, read it; do not ask the
maintainer.
When the reviewer is wrong, the correction is stated plainly once and
the analysis moves on.

## Sign-off

A change is signed off when all of the following hold:

- Its commits match the plan.
- Its scope is contained and proportionate.
- Every falsifiable claim checks against ground truth.
- The applicable trap and completeness rungs found nothing.
- The PR's e2e test is unskipped and green.
- The manual acceptance run has happened where it applies, and what it
  observed has been stated.

Open items go back as specific, surgical requests, not as a direction
to start over.
