"""Codex payload helpers. Codex hooks cannot ask, so its adapters only deny.

Mapping of shared verdicts: denials stay denials; a `-p` build becomes a denial with the same
reason (the user can still run it); approval prompts for commit/push are left to AGENTS.md,
which Codex reads before acting.
"""

from __future__ import annotations

import re

from scripts.agents.command_policy import Verdict

PATCH_FILE = re.compile(r"^\*\*\* (Add File|Update File|Delete File|Move to): (.+)$")


def codex_decision(verdict: Verdict | None) -> tuple[str, str] | None:
    if verdict is None:
        return None
    if verdict.decision[0] == "deny":
        return verdict.decision
    if verdict.kind == "build-scope":
        return ("deny", verdict.decision[1])
    return None


def patch_text(payload: dict) -> str:
    tool_input = payload.get("tool_input") or {}
    text = tool_input.get("command") if isinstance(tool_input, dict) else None
    return text if isinstance(text, str) else ""


def patch_files(patch: str) -> dict[str, list[str]]:
    """Map each path an apply_patch envelope touches to the lines it adds, in patch order."""
    files: dict[str, list[str]] = {}
    current: str | None = None
    for line in patch.splitlines():
        if m := PATCH_FILE.match(line):
            current = m.group(2).strip()
            files.setdefault(current, [])
        elif line.startswith("*** "):
            current = None if line.startswith("*** End Patch") else current
        elif current is not None and line.startswith("+"):
            files[current].append(line[1:])
    return files
