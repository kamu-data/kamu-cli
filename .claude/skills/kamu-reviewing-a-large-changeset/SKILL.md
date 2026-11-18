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
| 1 | Dead code | What did the branch leave unused? Functions, types, variants, errors, mock and harness helpers, config fields, dependencies, feature flags, leftovers of renames | `kamu-rust-style` | Report, then fix |
| 2 | Duplication | Which code is duplicated or nearly so, and could be generalized, parameterized or decomposed? Production code, test harnesses and e2e scenarios alike, never at the cost of a method's visible transaction blocks | `kamu-rust-style`, `kamu-test-harness`, [database-transactions.md §3.2](../../../docs/internal/database-transactions.md#32-inside-a-level) | Report, then fix |
| 3 | Rust rules | Does the branch's Rust follow the written rules? Exhaustive matches, imports, conversions, module layout, visibility, DI grouping, error mapping, test structure | `kamu-rust-style`, `kamu-dill-di`, `kamu-domain-design`, `kamu-test-harness` | Fix, then report |
| 4 | Comments and docs | Does every comment and doc line the branch added or invalidated meet the prose rules? Include text made stale by merges from the base | `kamu-prose-and-comments` | Fix, then report |
| 5 | Changelog | Is there one consolidated entry per user-visible change, written for users, with "breaking" checked against release tags? | `kamu-release-dependency-workflows` | Fix, then report |
| 6 | Integration audit | Where do pieces reviewed one at a time break together? Functional regressions, new bugs, authorization and input bounds, performance (transactions held during long work, repeated queries), parallel paths that drifted apart (CLI and task, telemetry) | AGENTS.md, the area's design doc | Report, then fix |
| 7 | Coverage gaps | Which behavior the branch changed has no test that fails when the change is reverted? At every level, unit to e2e | `kamu-test-harness`, `kamu-cli-e2e-tests`, `kamu-repository-tests` | Report, then fix |
| 8 | Redundant tests | Which tests add no coverage, which look-alikes should be merged or table-driven, which e2e tests repeat a lower level, which Postgres wirings the repository cannot affect? | `kamu-test-harness`, `kamu-cli-e2e-tests` | Report, then fix |
| 9 | Design docs | Do the `docs/internal/` sections the branch touched or invalidated match the code, read well, and stay routed in AGENTS.md? | `kamu-prose-and-comments`, AGENTS.md "Documentation classes" | Fix, then report |
| 10 | Wrap-up | Is the whole branch verified and described? | `kamu-github-operations` | See below |

**Why this order.** Removing code first shrinks what every later pass reads. Rules and prose come
after the code stops moving, and the changelog after the prose. The integration audit reads
settled code. Coverage gaps come before the trim, so trimming knows which coverage must stay.
Design docs come last, because every earlier step can invalidate them.

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
   options. Every finding is reported. Skipping or deferring one is the user's decision, never
   yours.
5. **Validate** what the step touched: `make clippy` after Rust changes, `make lint-harness` after
   docs or skills, the tests of the touched areas through nextest filtersets. The full suite runs
   once, in the wrap-up, unless the user asks earlier; the user often runs it in parallel.
6. **Commit, once approved.** One commit per step, titled after the step and the scope, e.g.
   "Stage 0 Rust rules audit". If a step also applied decisions the user made after the report,
   a second commit may separate them ("… audit decisions"). The commit titles record which
   steps are done, so a resumed session reads them from `git log`.

The user's reply to a step's report often approves its commit and starts the next step at once.
Within an approved step, do not ask again about each fix.

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
