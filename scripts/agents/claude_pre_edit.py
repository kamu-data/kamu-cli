"""Claude PreToolUse(Write|Edit|MultiEdit|NotebookEdit): generated files, memory, skill gate."""

from __future__ import annotations

from scripts.agents.common import emit, pre_tool_decision, read_payload, run_safely
from scripts.agents.edit_policy import decide
from scripts.agents.skill_ledger import CLAUDE_LOAD_WITH, CLAUDE_STATE, reader_key


def main() -> int:
    payload = read_payload()
    tool_input = payload.get("tool_input") or {}
    path = tool_input.get("file_path") or tool_input.get("notebook_path")
    if not isinstance(path, str):
        return 0
    if decision := decide([path], reader_key(payload), CLAUDE_STATE, CLAUDE_LOAD_WITH):
        emit(pre_tool_decision(decision))
    return 0


if __name__ == "__main__":
    run_safely(main)
