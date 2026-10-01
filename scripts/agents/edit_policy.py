"""Judge a file write before an agent makes it: generated files, memory, governed paths."""

from __future__ import annotations

import re
from pathlib import Path

from scripts.agents.common import Decision, matches, policy, repo_relative
from scripts.agents.skill_ledger import missing_skills, refusal

MEMORY_PATH = re.compile(r"\.claude/projects/[^/\s'\"]+/memory(?:/|$|\s)")
MEMORY_REASON = (
    "This writes to agent memory. Memory is for external context only (AGENTS.md, 'Memory'); a "
    "rule about this codebase belongs in AGENTS.md or a skill. Approve only for an external fact."
)


def generated_decision(paths: list[str]) -> Decision | None:
    hits = []
    for path in paths:
        rel = repo_relative(path)
        if not rel:
            continue
        for rule in policy()["generated"]:
            if matches(rel, rule["paths"]):
                hits.append(f"- {rel} is generated — regenerate with `{rule['command']}`")
                break
    if not hits:
        return None
    return ("deny", "Generated files are never edited by hand (AGENTS.md, 'Documentation classes'):\n" + "\n".join(hits))


def memory_decision(text: str) -> Decision | None:
    return ("ask", MEMORY_REASON) if MEMORY_PATH.search(text) else None


def decide(paths: list[str], key: str, state_file: Path, load_with: str) -> Decision | None:
    """Generated-file and skill refusals win over the memory prompt."""
    if decision := generated_decision(paths):
        return decision
    if missing := missing_skills(paths, key, state_file):
        return ("deny", refusal(missing, load_with))
    for path in paths:
        if decision := memory_decision(str(path)):
            return decision
    return None
