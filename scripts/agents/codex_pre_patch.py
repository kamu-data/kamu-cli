"""Codex PreToolUse(apply_patch): deny generated files and guarded paths without their skill."""

from __future__ import annotations

from scripts.agents.codex_common import patch_files, patch_text
from scripts.agents.common import emit, pre_tool_decision, read_payload, run_safely
from scripts.agents.edit_policy import generated_decision
from scripts.agents.skill_ledger import CODEX_LOAD_WITH, CODEX_STATE, missing_skills, reader_key, refusal


def main() -> int:
    payload = read_payload()
    paths = list(patch_files(patch_text(payload)))
    decision = generated_decision(paths)
    if decision is None and (missing := missing_skills(paths, reader_key(payload), CODEX_STATE)):
        decision = ("deny", refusal(missing, CODEX_LOAD_WITH))
    if decision:
        emit(pre_tool_decision(decision))
    return 0


if __name__ == "__main__":
    run_safely(main)
