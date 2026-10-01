---
name: kamu-prose-and-comments
description: Rules for code comments, doc comments, and prose in Kamu CLI — Rust `//` and `///` comments, SQL comments, docs/internal design docs, AGENTS.md, CLAUDE.md, skills, and CHANGELOG wording. Use before writing or editing any comment or documentation text, and for a re-read pass before handing work back.
---

# Prose And Comments

## Code comments

A comment earns its place by telling the reader something the code cannot: a constraint, a
non-obvious invariant, a reason a choice was made, a trap. Keep it to one or two lines.

**A comment must never be:**

| Kind | Example to avoid | Why |
|---|---|---|
| A restatement | `// increment the counter` | The code already says it |
| A change narration | `// was previously a Vec`, `// no longer returns None`, `// renamed from X`, `// after removing Y`, `// this change adds…` | History belongs in commit messages; in the source it is stale the day it merges |
| A plan or ticket citation | `// see plan 7 item 3`, `// JIRA-123`, `// per issue #1588`, `// slice 4` | The referent is not in the repository and rots |
| A tally | `// the three repositories below` | Goes wrong the moment a fourth is added |
| A positional reference | `// see line 120`, `// the function above` | Moves with every edit; name the item instead |
| A walkthrough | a paragraph per step of an obvious algorithm | Split into well-named helpers instead |

**Allowed exception:** stable test-slice identifiers defined by a checked-in map, such as `RF-*`
indexed by
[`COVERAGE.md`](../../../src/domain/resources/facade-tests/tests/contract/COVERAGE.md). The map
lives beside the tests, so it cannot rot without the map rotting too. Keep it in sync when adding
or renumbering.

**Doc comments (`///`)** describe the contract — what the item guarantees, its errors, its
invariants — not its implementation.

**Dividing lines** (a comment line of only `/`) are exactly 120 characters, matching the
surrounding files. A repo lint checks it.

The post-edit hook rejects added Rust comments that cite plans or tickets or narrate change. It
catches the common phrasings only, so the rules above still apply in full.

## Documentation prose

These apply to `docs/internal/*.md`, `AGENTS.md`, `CLAUDE.md` and skills.

- **One owner per claim.** Each fact lives in exactly one place. Elsewhere, link to it rather
  than restating it — a restated rule drifts from its owner. AGENTS.md "Documentation classes"
  says who owns what.
- **Link by path and heading anchor, never by line number.** `#L55` links go stale with the next
  edit; the drift lint refuses them.
- **Describe the code as it is,** not how it came to be. A design doc that says "we used to…" is
  narrating change, the same as a comment would.
- **Amend in the same change.** If your change invalidates a sentence in a design doc, fixing
  that sentence is part of the change, not a follow-up.
- **Route new documents.** A new `docs/internal/*.md` needs a row in AGENTS.md "What to load for
  which task". A new skill needs a row there too, plus its `.agents/skills` symlink.
- **Prefer tables and checklists** for decisions and procedures. Prose is for the reasoning
  behind them.
- **Record rejected approaches with the reason.** A "Rejected approaches" table stops the next
  session from re-proposing a dead end. Give the evidence that killed each one.

## CHANGELOG wording

- Write for end users: what changed for them. Leave out how it was implemented.
- One entry per user-visible change, under `### Added` / `### Changed` / `### Fixed`.
- Written once, at finalization (AGENTS.md, "Changelog review context").

## Re-read pass before handing back

Read every comment and doc line you added, as a newcomer would, and check:

1. Does it say something the code or a linked doc does not already say? If not, delete it.
2. Does it narrate history, cite a plan or ticket, count things, or point at line numbers?
   Rewrite it.
3. Will it still be true after the next unrelated change? If not, make it name the invariant
   instead of the current state.
4. For docs: does every link resolve? Is every new file routed? `make lint-harness` checks both.

## What lives elsewhere

- Code style rules other than comments: `kamu-rust-style`.
- Changelog structure and release flow: `kamu-release-dependency-workflows`.
