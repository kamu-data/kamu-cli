# Agent Harness

Coding agents (Claude Code, Codex) working in this repository are guided by documents and held to them by hooks. This page is for whoever maintains that machinery; the rules themselves live in [`AGENTS.md`](../../AGENTS.md), and how to set agents up is in [`DEVELOPER.md`](../../DEVELOPER.md#coding-agents).

| What | Where |
|---|---|
| Canonical agent rules, skill and document routing | [`AGENTS.md`](../../AGENTS.md) |
| Claude Code specifics | [`CLAUDE.md`](../../CLAUDE.md) |
| Task procedures (skills) | `.claude/skills/<name>/SKILL.md` |
| The same skills for Codex | `.agents/skills/<name>` — relative symlinks into `.claude/skills/` |
| Which paths need which skill; which files are generated | [`.claude/hooks/governed_paths.json`](../../.claude/hooks/governed_paths.json) |
| Hook wiring | [`.claude/settings.json`](../../.claude/settings.json), [`.codex/hooks.json`](../../.codex/hooks.json) |
| Hook code and its tests | this directory (Python standard library only), `tests/` |
| Consistency lints | `src/utils/repo-tools/tests/lints/agent_harness.rs` |
| Runtime state (gitignored) | `.claude/state/`, `.codex/state/` |

Policy and agent instructions are owned by [`AGENTS.md`](../../AGENTS.md) and the routed skills.

What the hooks do:
* **Command guard** — denies commands that discard uncommitted work (`git reset --hard`, `git checkout -- <path>`, `git restore`, `git stash`, `git clean -f`), `SQLX_OFFLINE=...` on the command line, and build/test output piped into `head`/`tail`; asks for approval on `git commit`/`push`/`merge`/`rebase`/`tag` and on `-p`-scoped cargo builds.
* **Edit guard** — refuses hand edits to generated files (naming the regeneration command) and edits under a guarded path until the session has loaded the governing skill; asks before writes to agent memory.
* **Post-edit** — runs `rustfmt` on an edited `.rs` file, then checks only the added lines (`assert!(matches!(..))`, lint suppressions, `dbg!`, plan/ticket citations and change narration in comments, license header, dividing lines); runs `cargo sort` and `taplo fmt` on an edited `Cargo.toml`, or asks for `make fmt` when either tool is missing; reminds about `make sqlx-prepare`, schema regeneration and mirroring or marking e2e wiring for one database.
* **Post-shell** — the same pass over every file a shell command changed (`sed -i`, heredocs, scripts), judged against its committed version. The changed files are found by fingerprinting the working tree before and after the command; files brought in by git commands that rewrite the tree (`switch`, `pull`, `stash`, `rebase`, ...) are left alone. A guarded path changed without its skill gets a reminder.
* **Session start** — injects a short contract generated from `governed_paths.json`; after compaction or `/clear` skills must be loaded again.
* **Stop** (Claude only) — if the session edited Rust and the tree changed since the last green foreground `make clippy`, reminds the agent once to run it. Otherwise it lists the lines the uncommitted Rust change adds three or more times, leaving out idioms the tree already uses often, once per line (`repeated_lines.py`). The same check runs over a branch with `python3 -m scripts.agents.repeated_lines --base <ref>`.

Limitations — the rules in `AGENTS.md` still apply where the hooks cannot see:
* Files written through the shell are only checked after the command, and the edit guard cannot refuse them: a guarded path written that way gets a reminder instead of a refusal, and a hand-edited generated file is not told apart from a regenerated one. A command run in the background is compared when it is handed off, not when it finishes.
* `git checkout <path>` without `--` is indistinguishable from a branch switch and passes the guard.
* Commands assembled at runtime (variables, generated scripts) are not inspected.
* Codex hooks cannot ask: its adapters deny `-p` builds outright and leave commit approval to `AGENTS.md`.

Typical changes:
* **Add a skill:** create `.claude/skills/<name>/SKILL.md` with `name` and `description` front matter, `ln -s ../../.claude/skills/<name> .agents/skills/<name>`, and add a row to the skills table in `AGENTS.md`.
* **Guard a path with a skill:** add the glob to the skill's rule in `governed_paths.json` **and** to the "Guarded paths" column in `AGENTS.md` — a lint keeps the two identical, including order (the first matching rule wins). A rule with `"baseline": true` applies in addition to the first match — `kamu-rust-style` on every `.rs` file is one.
* **Add a design doc:** put it in `docs/internal/` and add a row to the documents table in `AGENTS.md`.
* **Change a hook rule:** edit the policy module here and add a case to its test.

Before pushing changes to any of the above, run:
```sh
make lint-harness
```
It runs the hook tests, including Codex handlers invoked from a nested directory with JSON on
stdin, and the repository lints (skills declared, routed and symlinked; guarded paths in sync;
documentation links and anchors resolve; every design doc routed). The handler tests exercise
policy and payload handling without a model request; they do not verify Codex trust decisions or
runtime event dispatch. CI runs the same target on pull requests that touch any of the files above,
including documentation-only ones; code changes get the repository lints from the build workflow.
