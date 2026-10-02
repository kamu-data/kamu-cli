"""Codex PostToolUse(Bash): record skill loads, then check the files the command changed.

Codex loads a skill by reading its SKILL.md, so the load is recorded from the command text.
"""

from __future__ import annotations

import sys

from scripts.agents.bash_edits import call_key, changed_paths, check_changed, moves_working_tree, snapshot, take_pending
from scripts.agents.codex_pre_bash import STATE_DIR
from scripts.agents.common import additional_context, emit, read_payload, run_safely, tool_failed
from scripts.agents.post_edit import report
from scripts.agents.skill_ledger import CODEX_LOAD_WITH, CODEX_STATE, reader_key, record, skills_read_by


def main() -> int:
    payload = read_payload()
    before = take_pending(STATE_DIR, call_key(payload))
    command = (payload.get("tool_input") or {}).get("command")
    if not isinstance(command, str):
        return 0
    if not tool_failed(payload):
        record(skills_read_by(command), reader_key(payload), CODEX_STATE)
    if before is None or moves_working_tree(command):
        return 0
    changed = changed_paths(before, snapshot())
    if not changed:
        return 0
    problems, notes = check_changed(changed, reader_key(payload), CODEX_STATE, CODEX_LOAD_WITH)
    if problems:
        sys.stderr.write(report(problems))
        return 2
    if notes:
        emit(additional_context("PostToolUse", "\n".join(notes)))
    return 0


if __name__ == "__main__":
    run_safely(main)
