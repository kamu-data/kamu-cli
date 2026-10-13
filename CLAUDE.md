# CLAUDE.md

Read [`AGENTS.md`](AGENTS.md) — it is the canonical source for every rule: git safety, build scope,
validation, tests, style, and which skill or document to load for a task. This file only adds what
is specific to Claude Code.

## Environment

The usual local setup here runs against live databases: `make sqlx-local-setup` writes per-crate
`.env` files (gitignored) with a `DATABASE_URL`, and the DB containers run under Podman. If those
`.env` files exist, SQLx queries are checked against the real schema — never override
`SQLX_OFFLINE` on the command line ([why](AGENTS.md#hard-rules)). If they do not, builds use the
committed `.sqlx` cache and database-backed tests need the setup first
([`DEVELOPER.md`](DEVELOPER.md#build-with-databases)).

## Hooks

[`.claude/settings.json`](.claude/settings.json) wires the hooks in `scripts/agents/`:

- a command guard (destructive git forms denied; commit/push/merge/rebase and `-p` builds ask);
- an edit guard (generated files denied; guarded paths refused until their skill is loaded);
- a post-edit pass on `.rs` files, whether an edit or a shell command wrote them (`rustfmt`, then
  checks on the added text), and on `Cargo.toml` files (`cargo sort`, then `taplo fmt`);
- a session-start contract and a stop-time reminder when `.rs` files changed since the last green
  `make clippy`.

A refusal names the rule and the fix. Load skills with the `Skill` tool; a subagent loads its own.
How the harness is built and tested: [`DEVELOPER.md`](DEVELOPER.md#agent-harness).

## Memory

Memory is for external context only (see [AGENTS.md](AGENTS.md#memory)). Before saving a memory,
check whether it is really a rule about this codebase — if so, propose an `AGENTS.md` or skill
change instead.
