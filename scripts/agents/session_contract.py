"""The harness contract injected at session start, generated from the policy table."""

from __future__ import annotations

from scripts.agents.common import policy


def contract(load_with: str = "the Skill tool", command_approval: str = "ask every time") -> str:
    guarded = "\n".join(
        f"  - {', '.join(r['paths'])} → `{r['skill']}`" + (" (in addition to any match above)" if r.get("baseline") else "")
        for r in policy()["skills"]
    )
    generated = "\n".join(f"  - {', '.join(r['paths'])} → `{r['command']}`" for r in policy()["generated"])
    return f"""Harness contract for this session (hooks in scripts/agents/, rules in AGENTS.md):
- Edits under a guarded path are refused until this session (or this subagent) has loaded its skill with {load_with}:
{guarded}
- Generated files are refused outright; regenerate them instead:
{generated}
- Commands: destructive git forms (reset --hard, checkout --/., restore, stash, clean -f) are denied; commit/push/merge/rebase/tag and `cargo build/check/clippy/test/nextest -p` {command_approval} (whole-workspace builds are the default — narrow tests with nextest filtersets); SQLX_OFFLINE on the command line is denied; build/test output piped into head/tail is denied.
- After a .rs edit the hook runs rustfmt and checks the added lines; a failure means the edit landed and must be fixed forward.
- Before handing back: `make clippy` after Rust changes; `make lint-harness` after changes to hooks, skills, AGENTS.md/CLAUDE.md or docs/internal.
- A refusal is the rule speaking: fix the command or load the skill — never route around it through another tool."""
