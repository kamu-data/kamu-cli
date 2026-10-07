# AGENTS.md

Project guidance for coding agents working in this repository. This file is canonical: every
rule about how this codebase is written, built, tested and documented lives here or in a skill
it routes to. Claude-specific notes are in [`CLAUDE.md`](CLAUDE.md); the human guide is
[`DEVELOPER.md`](DEVELOPER.md).

## Hard rules

These hold without exception unless the user lifts one for a named case. Hooks in
`scripts/agents/` check common violations when enabled and trusted; agents must follow the rules
on every tool path. A blocked command or refused edit is the rule speaking — fix the command or
load the skill, never route around it through another tool. Hook setup and limitations are in
[`DEVELOPER.md`](DEVELOPER.md#agent-harness).

- **Never commit without explicit approval.** Do not run `git commit`, `git push`, `git merge`,
  `git rebase`, or any other history-altering command unless the user asks for it in that specific
  instance. Approval to commit once is not standing approval; commit steps in a plan are
  checkpoints, not permission. Leave finished work in the working tree, report, and stop.
- **Never discard uncommitted work.** `git checkout <path>`, `git restore <path>`,
  `git reset --hard`, `git stash` and `git clean` destroy working-tree changes irreversibly. Do not
  run them on a file with uncommitted changes unless the user explicitly asked to throw those
  changes away — and say which changes will be lost before you do. The guard cannot tell a branch
  from a path, so `git checkout <path>` may pass it and is still forbidden.
  - A bad edit is fixed forward (`Edit`, or rewrite with `Write`), never by resetting the file: a
    reset also reverts every unrelated edit already made to it.
  - To compare against the committed version, read it without touching the working tree:
    `git show HEAD:<path> > <scratch>/orig`.
  - In scripted multi-edit passes, anchor on exact strings and assert every anchor matches before
    writing. Line numbers go stale as soon as an earlier edit lands — re-read the file to
    recompute them.
- **Never set `SQLX_OFFLINE` on the command line.** SQLx query-checking mode comes from `.env`
  files: the root [`.env`](.env) sets `SQLX_OFFLINE=true` so CI (which has no database) compiles
  from the committed `.sqlx` cache, and `make sqlx-local-setup` writes per-crate `.env` files with
  a live `DATABASE_URL` that take precedence locally. Forcing it from the shell silently checks
  queries against the stale cache and hides schema drift. See
  [`DEVELOPER.md`](DEVELOPER.md#build-with-databases).
- **Generated files are never edited by hand.** See [Documentation classes](#documentation-classes)
  for each file and the command that regenerates it.
- **Two instructions that cannot both be followed are a bug in one of them.** Name both and ask;
  do not silently pick one. When you break a rule, say which one, what it says, and what it cost.

## Build scope (`-p`)

Build, check, lint and test **the whole workspace**; narrow tests with nextest filtersets, which
select tests without changing what gets built:

```bash
cargo build
cargo nextest run -E 'test(test_name_here)'
cargo nextest run -E 'package(kamu-adapter-graphql) and test(dataset)'
cargo nextest run -E 'binary(name)'
make clippy
```

Why: the workspace pulls in about 1300 crates and builds with workspace-wide feature unification
(`.cargo/config.toml`, `[resolver] feature-unification = "workspace"`). Scoping
`cargo build`/`check`/`clippy`/`test`/`nextest run` to one crate with `-p <crate>` (`--package`)
does not reuse the intermediate artifacts of full-workspace builds, so heavy dependencies —
DataFusion, Arrow, SQLx — get recompiled and the "narrow" build ends up slower. `-p` works; the
problem is cost.

- Exception: `-p` can be cheap for a crate with a small, reusable dependency tree (e.g. the
  Makefile's `cargo nextest run -p kamu-repo-tools`). Use it only when the user asks for it or a
  Makefile target already does it. The command hook asks for approval on `-p` builds.
- Unrelated: `-p` on `cargo update`, `cargo upgrade`, `cargo tree` is a package spec, not build
  scoping.

## Validation

| You touched | Run before handing back |
|---|---|
| Any `.rs` file | `cargo fmt` (a hook also runs `rustfmt` per edited file), then `make clippy` |
| Any `Cargo.toml` | `make fmt` (`cargo fmt`, `cargo sort`, `taplo fmt`; a hook also sorts and formats each edited manifest) |
| SQLx queries or `migrations/` | `make sqlx-prepare` and keep the regenerated `.sqlx/` files |
| GraphQL types or resolvers | `make resources-graphql-schema`; review the `resources/schema.gql` diff |
| CLI args, config, HTTP API | `make resources`; review the `resources/` diff |
| `scripts/agents/`, `.claude/`, `.codex/`, skills, `AGENTS.md`, `CLAUDE.md`, `docs/internal/` | `make lint-harness` |

- Treat Clippy warnings as errors to fix. Do not silence them with `#[allow]` / `#[expect]`: a
  pragma hides the problem instead of resolving it. If a lint seems genuinely wrong for a case,
  ask before suppressing it; an approved suppression is an `#[expect]` with a `reason` (see the
  `kamu-rust-style` skill).
- Keep build, test and lint output available in full. Do not pipe it into `head` or `tail`;
  save long output to a file and read the relevant sections after checking the exit status.
- `make lint` runs every lint CI runs, plus `lint-sqlx` and `lint-harness`.

### Migration review context

Do not assume edited migrations are already applied externally just because they exist in git
history.

- Treat uncommitted migration edits as branch-local and not yet released, shared, or applied
  externally. Review their contents for correctness only.
- Do not flag checksum drift or "already migrated database" risk based only on the migration
  timestamp or prior commits.
- Reconsider external-application risk only when the user says the migration was released, shared,
  or applied, or when something in the workspace shows it was. If that status is genuinely unclear
  and would change your answer, ask — do not turn the uncertainty into a review finding.
- Migrations are forward-only single files; do not ask for down migrations.

### Changelog review context

Do not require or suggest `CHANGELOG.md` updates while reviewing uncommitted work on an
in-progress feature branch, regardless of branch size. The changelog is consolidated into one
compact entry when the feature is finalized.

- Check for a changelog entry only when the user explicitly asks for PR finalization, release
  preparation, or changelog work.
- This applies to breaking API changes too. A schema or interface change is only "breaking" for
  consumers if it was previously released — verify against the release tags
  (`git show <tag>:path`) before treating it as one.

### In-memory repositories review context

`*-inmem` repository crates exist for tests only and intentionally do not model transactions (no
rollback on a failed transaction). Do not raise missing rollback or "in-memory used in production"
as findings. Do fix genuine in-memory bugs that break test fidelity (e.g. index corruption on a
rejected save).

## Code style and tests

Rust style lives in the `kamu-rust-style` skill and test conventions in `kamu-test-harness`; the
edit hook requires them before the first edit they govern (see the table below). Targeted test
runs use nextest filtersets — see [Build scope](#build-scope--p).

## What to load for which task

### Skills

Skills live in [`.claude/skills/`](.claude/skills); `.agents/skills/` holds relative symlinks to
the same directories for Codex. Load a skill (Claude: the `Skill` tool; Codex: read its
`SKILL.md`) before the first edit its task needs. Edits under a *guarded path* are refused until
the session has loaded the skill — the column below mirrors
[`.claude/hooks/governed_paths.json`](.claude/hooks/governed_paths.json) and a repo lint keeps the
two equal. Rows are matched top to bottom and the first match wins; a *baseline* row applies in
addition to that match (a GraphQL resolver needs both `kamu-graphql-api` and `kamu-rust-style`).

| Task | Skill | Guarded paths |
|---|---|---|
| CLI black-box e2e tests: shared `repo-tests` scenarios, `execute_command` vs `run_api_server`, per-DB macros, `KamuCliPuppet` | `kamu-cli-e2e-tests` | `src/e2e/**` |
| Storage-backed repository trait suites, `repo-tests` crates, `database_transactional_test!` | `kamu-repository-tests` | `src/infra/*/repo-tests/**` |
| Postgres/SQLite repositories, SQLx macros, migrations, SQLx offline data | `kamu-sqlx-database-work` | `migrations/**`, `src/infra/*/postgres/**`, `src/infra/*/sqlite/**`, `src/infra/*/cache-postgres/**`, `src/infra/*/cache-sqlite/**` |
| GraphQL queries, mutations, roots, resolvers, enum mappings, schema regeneration | `kamu-graphql-api` | `src/adapter/graphql/**`, `src/adapter/resources-facade-graphql/**` |
| Test harness structs, per-account catalog wiring, in-memory test doubles | `kamu-test-harness` | `src/**/tests/**/*.rs` |
| Changelog, release, general Cargo dependency updates | `kamu-release-dependency-workflows` | `CHANGELOG.md` |
| Comments, doc comments, and any prose in docs or skills | `kamu-prose-and-comments` | `AGENTS.md`, `CLAUDE.md`, `docs/**/*.md`, `.claude/skills/**` |
| dill components, interfaces, scopes, catalog building and chaining | `kamu-dill-di` | |
| Outbox, repositories, domain/view construction, event modeling, operation-specific errors | `kamu-domain-design` | |
| Renaming a type, field, message, flag or any other concept across layers | `kamu-renaming-a-concept` | |
| Adding a new bounded context (domain + services + repositories + wiring) | `kamu-adding-a-bounded-context` | |
| DataFusion, Arrow, Object Store, Parquet and related query-engine upgrades | `kamu-datafusion-upgrade-workflows` | |
| Jupyter demo, rustfs, multi-platform demo image releases | `kamu-jupyter-demo-release-workflows` | |
| Writing any Rust: imports, numeric conversions, strum mappings, module layout, visibility (baseline) | `kamu-rust-style` | `**/*.rs` |

### Documents

Read the relevant document before changing that area, and amend it in the same change when the
change invalidates what it says.

| Task | Document |
|---|---|
| Posting messages, new message types or consumers, consumption modes, the outbox agent, delivery ordering | [`docs/internal/outbox.md`](docs/internal/outbox.md) |
| How background agents wake up, Postgres `LISTEN`/`NOTIFY`, SQLite polling, deadline-driven waits | [`docs/internal/wakeup-listeners.md`](docs/internal/wakeup-listeners.md) |
| Exported Prometheus metrics, recommended alerts, adding metrics | [`docs/internal/metrics.md`](docs/internal/metrics.md) |
| Task scheduling and execution, the task agent, planners and runners, new task types | [`docs/internal/task-system.md`](docs/internal/task-system.md) |
| Redesigning tasks as RFC-019 resources: open questions and decisions | [`docs/internal/task-system-redesign.md`](docs/internal/task-system-redesign.md) |
| Flows, triggers, configurations, scheduling, sensors, flow process state, new flow types | [`docs/internal/flow-system.md`](docs/internal/flow-system.md) |
| Root dataset ingest: polling and push sources, fetch steps, savepoints, the DataFusion data writer, merge strategies | [`docs/internal/root-dataset-ingest.md`](docs/internal/root-dataset-ingest.md) |
| Dataset pull: `kamu pull` flags, pull planning and depth ordering, iteration execution, authorization, how the update task reuses the planner | [`docs/internal/dataset-pull.md`](docs/internal/dataset-pull.md) |
| Dataset sync: `SyncService`, Simple and Smart Transfer Protocols (client and server), `kamu push`, remote repositories and aliases, IPFS | [`docs/internal/dataset-sync.md`](docs/internal/dataset-sync.md) |
| Derived dataset transform: transform planning and elaboration, engine provisioning and containers, diverged inputs, transform replay in verification | [`docs/internal/derived-dataset-transform.md`](docs/internal/derived-dataset-transform.md) |
| Hard compaction: `kamu system compact`, the compaction planner and executor, merged slices, what is (not) deleted | [`docs/internal/dataset-hard-compaction.md`](docs/internal/dataset-hard-compaction.md) |
| Dataset reset: `kamu reset`, reset to a block, reset to metadata, how block indexes, statistics, search and the dependency graph react to any history rewrite | [`docs/internal/dataset-reset.md`](docs/internal/dataset-reset.md) |
| Webhooks: subscriptions and their statuses, secrets, delivery signing and payload, the receiver contract, how subscriptions drive flow triggers | [`docs/internal/webhooks.md`](docs/internal/webhooks.md) |
| The declarative resources subsystem | [`docs/internal/resources-framework.md`](docs/internal/resources-framework.md) |
| Authored vs generated fields of a resource | [`docs/internal/resources-anatomy.md`](docs/internal/resources-anatomy.md) |
| Resource label selectors and filtering | [`docs/internal/resources-label-filtering.md`](docs/internal/resources-label-filtering.md) |
| `RF-*` test-slice identifiers of the resources contract tests | [`COVERAGE.md`](src/domain/resources/facade-tests/tests/contract/COVERAGE.md) |
| Local DB setup, migrations, Elasticsearch, test groups, build-speed tips, release procedures | [`DEVELOPER.md`](DEVELOPER.md) |
| How the agent harness works: hooks, skills, drift lints | [`DEVELOPER.md`](DEVELOPER.md#agent-harness) |

## Documentation classes

| Class | Contract |
|---|---|
| `AGENTS.md` | Canonical agent rules. Always loaded. One owner per rule: other files link here instead of restating. |
| `CLAUDE.md` | Claude Code specifics only. |
| `.claude/skills/` | Canonical task procedures. Each skill's `name` equals its directory and has a row in the table above. |
| `.agents/skills/` | Relative symlinks to `.claude/skills/` only — never real files. |
| `docs/internal/` | Living design docs. Changing behaviour a document describes means amending it in the same change. |
| `DEVELOPER.md` | The human developer guide and owner of human procedures. Skills link to its sections rather than copying them. |
| `CHANGELOG.md` | Written at finalization only (see [Changelog review context](#changelog-review-context)). |
| `resources/schema.gql` | Generated — `make resources-graphql-schema`. |
| `resources/openapi*.json`, `resources/config-*`, `resources/cli-reference.md`, `resources/di.puml` | Generated — `make resources`. |
| `**/.sqlx/` | Generated — `make sqlx-prepare`. |
| `.spec/` | Gitignored local working material; nothing else tracked links to or cites it. |

## Memory

Agent memory (e.g. Claude's per-project memory directory) is for external context only: facts
about environments, people, and systems outside this repository. A rule about how this codebase is
written, tested, documented or verified belongs in this file or a skill, committed to git — a rule
in private memory is invisible to other agents and to the team. Writes to the memory directory
prompt for approval.

## Sub-agents

Reusable role prompts for Rust build/test delegation live in
[`.claude/agents/rust-builder.md`](.claude/agents/rust-builder.md) and
[`.claude/agents/rust-tester.md`](.claude/agents/rust-tester.md); treat them as canonical instead
of duplicating them.

## Scope

- Keep this file repo-specific. Task procedures go into skills; human procedures into
  `DEVELOPER.md`.
- Do not move agent guidance into `DEVELOPER.md`. It may describe the agent harness itself (what
  lives where, how to add a skill or change a hook) because humans maintain it.
