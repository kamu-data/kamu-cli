"""Claude PostToolUse(Bash): rustfmt, added-text checks and reminders for files the command changed."""

from __future__ import annotations

import sys

from scripts.agents.bash_edits import call_key, changed_paths, check_changed, moves_working_tree, snapshot, take_pending
from scripts.agents.claude_pre_bash import STATE_DIR
from scripts.agents.clippy_ledger import note_rust_edit
from scripts.agents.common import additional_context, emit, read_payload, run_safely
from scripts.agents.post_edit import report
from scripts.agents.skill_ledger import CLAUDE_LOAD_WITH, CLAUDE_STATE, reader_key


def main() -> int:
    payload = read_payload()
    before = take_pending(STATE_DIR, call_key(payload))
    command = (payload.get("tool_input") or {}).get("command")
    if before is None or not isinstance(command, str) or moves_working_tree(command):
        return 0
    changed = changed_paths(before, snapshot())
    if not changed:
        return 0
    if any(rel.endswith(".rs") for rel in changed):
        note_rust_edit(payload.get("session_id") or "unknown")
    problems, notes = check_changed(changed, reader_key(payload), CLAUDE_STATE, CLAUDE_LOAD_WITH)
    if problems:
        sys.stderr.write(report(problems))
        return 2
    if notes:
        emit(additional_context("PostToolUse", "\n".join(notes)))
    return 0


if __name__ == "__main__":
    run_safely(main)
