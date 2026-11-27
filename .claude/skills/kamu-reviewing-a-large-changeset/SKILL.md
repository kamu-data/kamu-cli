---
name: kamu-reviewing-a-large-changeset
description: Step-by-step cleanup review of a large changeset in Kamu CLI — an epic or stage branch, or any branch too big for one audit pass. Each step asks one question across the whole diff (dead code, duplication, Rust rules, comments and docs, changelog, integration bugs, coverage gaps, redundant tests, design docs), and each ends with a user review and its own commit. Use at an intermediate review point of a long branch, before finalizing a stage or epic PR, or when the user asks for a branch-wide cleanup or a systematic audit.
---

# Reviewing A Large Changeset

A branch that grew through many reviewed pieces still needs a review of the whole: code the
pieces left unused, duplication between them, rule drift, prose that no longer matches, bugs at
the seams. One audit pass that asks every question at once misses most of them. This skill runs a
fixed sequence of passes. Each pass asks **one question** across the **whole changeset**, the user
reviews the outcome, and the pass becomes one commit.

For a single commit or a small branch, a plain self-audit is enough; this procedure is for
changesets whose diff does not fit in one careful read.

## Setup

1. **The changeset.** Find the branch the work will merge into (the PR's base:
   `gh pr view --json baseRefName`, or ask). The changeset is
   `git diff origin/<base>...HEAD`: the three-dot form starts at the merge base, so commits merged
   in from the base are left out. List its files once (`--stat`) and group them by area. Leave
   out code copied from another project (AGENTS.md, "Code style and tests"): no step changes it.
2. **The intent.** Read the PR description and the tickets it closes. A pass judges the code
   against what the branch set out to do, not only against itself.
3. **The working branch.** Work on a cleanup branch made from the reviewed branch, unless the user
   names another. Never push, merge or fast-forward the reviewed branch: the user chooses how the
   cleanup lands (AGENTS.md, "Hard rules").
4. **The plan.** Show the steps below with any the user wants to skip, reorder or add, then start
   with the first one. The user may also start at any step.

## The steps

| # | Step | The one question | Owner of the rules | Mode |
|---|---|---|---|---|
| 1 | Dead code | What did the branch leave unused? Functions, types, variants, errors, mock and harness helpers, config fields, leftovers of renames | `kamu-rust-style` | Report, then fix |
| 2 | Duplication | Which code is duplicated or nearly so, and could be generalized, parameterized or decomposed? Production code, test harnesses and e2e scenarios alike, never at the cost of a method's visible transaction blocks | `kamu-rust-style`, `kamu-test-harness`, [database-transactions.md §3.2](../../../docs/internal/database-transactions.md#32-inside-a-level) | Report, then fix |
| 3 | Rust rules | Does the branch's Rust follow the written rules? Exhaustive matches, imports, conversions, module layout, visibility, DI grouping, error mapping, test structure | `kamu-rust-style`, `kamu-dill-di`, `kamu-domain-design`, `kamu-test-harness` | Fix, then report |
| 4 | Integration audit | Where do pieces reviewed one at a time break together? Functional regressions, new bugs, authorization and input bounds, performance (transactions held during long work, repeated queries), parallel paths that drifted apart (CLI and task, telemetry) | AGENTS.md, the area's design doc | Report, then fix |
| 5 | Coverage gaps | Which behavior the branch changed has no test that fails when the change is reverted? At every level, unit to e2e | `kamu-test-harness`, `kamu-cli-e2e-tests`, `kamu-repository-tests` | Report, then fix |
| 6 | Redundant tests | Which tests add no coverage, which look-alikes should be merged or table-driven, which e2e tests repeat a lower level, which Postgres wirings the repository cannot affect? | `kamu-test-harness`, `kamu-cli-e2e-tests` | Report, then fix |
| 7 | Dependencies | Does each crate the branch touched depend on what it uses, at the narrowest level? [Checklist](#the-dependency-step) | `kamu-release-dependency-workflows`, `kamu-test-harness` | Report, then fix |
| 8 | Comments and docs | Does every comment and doc line the branch added or invalidated meet the prose rules? Include text made stale by merges from the base | `kamu-prose-and-comments` | Fix, then report |
| 9 | Changelog | Is there one consolidated entry per user-visible change, written for users, with "breaking" checked against release tags? | `kamu-release-dependency-workflows` | Fix, then report |
| 10 | Design docs | Do the `docs/internal/` sections the branch touched or invalidated match the code, read well, and stay routed in AGENTS.md? | `kamu-prose-and-comments`, AGENTS.md "Documentation classes" | Fix, then report |
| 11 | Wrap-up | Is the whole branch verified and described? | `kamu-github-operations` | See below |

**Why this order.** Removing code first shrinks what every later pass reads. Duplication and the
Rust rules reshape the code before the integration audit reads it, so the audit sees settled code.
Coverage gaps come before the trim, so trimming knows which coverage must stay. Every step up to
the trim can add or drop a dependency, so the dependency step follows it. The text steps come
last, once code and tests stop moving: comments, then the changelog written from them, then the
design docs, which every earlier step can invalidate.

### The dependency step

Every dependency is code to update, audit and build. For each `Cargo.toml` the branch touched, or
whose crate's code it changed, check:

| Check | Fix |
|---|---|
| A dependency nothing in the crate uses any more | Remove it. `cargo shear` and `make lint-udeps` find candidates; `git grep` over the crate decides, as both miss a crate reached only through another dependency's API |
| A `[dependencies]` entry used only by `tests/`, benches or `#[cfg(test)]` code | Move it to `[dev-dependencies]` |
| A runtime dependency used only by the crate's `testing` module | Make it optional and enable it from the `testing` feature ([`kamu-test-harness`](../kamu-test-harness/SKILL.md#scope-2--crate-level-reusable-harness)) |
| A feature of a dependency, or its default features, that no code needs | Drop the feature; take `default-features = false` and list only the features used |
| A feature of the crate itself that no dependent enables, or one that enables crates it does not need | Remove the feature, or narrow what it enables |
| A dependency that a crate already in the tree, or the standard library, can replace | Replace it |
| A crate that depends on another layer's crate only for one type or helper | Report it: the type may belong lower, the call may belong in a caller |

After the fixes, `make clippy` and the tests of the touched crates show that nothing needed was
dropped. A feature the crate needs but no longer names can still build here, enabled by another
crate of the workspace build (AGENTS.md, "Build scope"); check that the crate's code reaches only
the features it lists.

## Running one step

1. **Load the owning skills** in this session before reading for the step; a subagent loads its
   own.
2. **Read the whole changeset for the one question.** Do not report findings that belong to
   another step: note them and carry them into that step. For a large changeset, split the file
   groups across subagents (`Agent`), give each the question and the owning skills, and
   **verify every finding yourself** before reporting it. A subagent's claim is a lead, not a
   finding.
3. **Act by the step's mode.**
   - *Fix, then report:* fix what is clearly wrong under the written rules; report each decision
     that has more than one reasonable answer, with a recommendation.
   - *Report, then fix:* change nothing yet. Report the findings and wait for the user's
     decisions, then apply them.
4. **Report** with an ID per finding (`D1`, `R3`, `G2` …) so the user can answer in one line
   ("fix D1–D4, D5 as you recommend, skip D6"). For each: where, what is wrong, the fix or the
   options, and its [value against effort](#value-against-effort). Every finding is reported.
   Skipping or deferring one is the user's decision, never yours.
5. **Validate** what the step touched: `make clippy` after Rust changes, `make lint-harness` after
   docs or skills, and the tests of the crates a fix changed, through nextest filtersets
   (`package(...)`, not `rdeps(...)`). The wide run, every dependent and the full suite, happens
   once in the wrap-up, unless the user asks earlier; the user often runs it in parallel.
6. **Commit, once approved.** One commit per step, titled after the step and the scope, e.g.
   "Stage 0 Rust rules audit". If a step also applied decisions the user made after the report,
   a second commit may separate them ("… audit decisions"). The commit titles record which
   steps are done, so a resumed session reads them from `git log`.

The user's reply to a step's report often approves its commit and starts the next step at once.
Within an approved step, do not ask again about each fix.

### Value against effort

Read as wide as the question needs, code older than the branch included: a pattern that spans
areas is one finding, and limiting the reading to the diff misses it. What a step recommends doing
is decided per finding, by what the work gains against what it costs. Rate each finding high,
medium or low on both, with a one-line reason. Recommend the work only where the value clearly
exceeds the effort. Report the rest in one line each as "not worth it", so the user can still
take one. A pattern worth the work but too large for the branch becomes a follow-up ticket.

The rating never removes a floor: a bug is always reported, and a written rule or a wrong
statement is always fixed. It decides how much to do beyond that.

| Step | Value is higher when | Effort is higher when | What the rating decides |
|---|---|---|---|
| Duplication | The copies changed more than once already, are large, or would drift into a bug | The fix spans many crates, changes a public API, or needs regeneration or wide test runs | Which copies get one home |
| Rust rules | Readers and editors hit the code often | Restructuring: splitting a module, regrouping DI, moving a type across crates | The judgment calls only; the written rules apply as they are |
| Integration audit | The failure is severe and likely: data loss and authorization before a slow path nobody runs | The fix is large, or changes behaviour that needs a changelog or a migration | Whether a fix lands in the branch or in a ticket; performance findings entirely |
| Coverage gaps | A regression would do damage, the code is likely to change, other code relies on the behaviour | The level the test needs, cheapest first: unit, repository suite, service harness, e2e; a Postgres wiring adds a database | The lowest level that catches the regression; e2e only for a contract no lower level reaches |
| Redundant tests | Removing it saves run time or upkeep: an e2e test or a database wiring saves most, a unit test almost nothing | Showing that another test still catches the same regression | Which e2e tests and wirings go; unit look-alikes merge only when that reads better |
| Dependencies | The dependency is heavy, rarely updated, or widens the attack surface | Code must be rewritten to drop it | Feature trimming and replacements; an unused dependency always goes |
| Design docs | The section is read often | Restructuring rather than fixing a sentence | Rewrites of sections that are correct but read badly |

## Wrap-up

- Run the full verification, or confirm that the user ran it, on the final state.
- Refresh the PR description for the cleanup commits: a section or table row per step that changed
  behavior or structure, and updated test and behavior-change notes. Edit through the REST API
  (`kamu-github-operations`).
- Land the cleanup branch the way the user chooses: a fast-forward of the reviewed branch, or a
  PR into it. Pushing either needs approval for that push.
- Delete the cleanup branch once landed; list any other branch the review made unused and ask
  before deleting it.

## Rejected approaches

Do not re-propose these without new evidence.

| Approach | Why it was rejected |
|---|---|
| One audit pass asking every question at once | Earlier branch-wide self-audits missed style, conventions and prose; the user had to ask whether they were covered. One question per pass is what found them |
| All cleanup steps in one commit | The user reviews each step's diff on its own; a combined commit hides a risky refactor among mechanical fixes |
| Deferring or dropping a finding without asking | Scope is the user's decision; a finding left out of the report never gets one |
| Pushing to or fast-forwarding the reviewed branch when the cleanup is done | The user lands branches themselves; an unrequested push to a stage branch had to be reverted with a force-push |

## What lives elsewhere

- The rules each step checks: the skills named in [the steps](#the-steps).
- Commit and push approval: AGENTS.md, "Hard rules".
- Changelog policy outside finalization: AGENTS.md, "Changelog review context".
