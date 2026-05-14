# CLAUDE.md

This file provides Claude Code-specific guidance. For the full agent guide, read [`AGENTS.md`](AGENTS.md) — it is the canonical source for validation, testing, style, and skill-loading rules.

## Environment

Always set `SQLX_OFFLINE=true` when running any `cargo` command. The postgres crates fail to compile without it because the SQLx offline cache is used instead of a live database connection during compilation.

```bash
SQLX_OFFLINE=true cargo nextest run -E 'test(test_name_here)'
SQLX_OFFLINE=true cargo build
SQLX_OFFLINE=true make clippy
```

## Skills

Claude Code can load skills via the `Skill` tool. The same skill list from `AGENTS.md` applies — trigger them by topic, not by default.
