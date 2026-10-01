"""Claude PreToolUse(Bash): judge the command line."""

from __future__ import annotations

from scripts.agents.command_policy import Verdict, combine, verdicts
from scripts.agents.common import emit, pre_tool_decision, read_payload, run_safely
from scripts.agents.edit_policy import memory_decision


def main() -> int:
    command = (read_payload().get("tool_input") or {}).get("command")
    if not isinstance(command, str):
        return 0
    found = verdicts(command)
    if memory := memory_decision(command):
        found.append(Verdict(memory, "memory"))
    if verdict := combine(found):
        emit(pre_tool_decision(verdict.decision))
    return 0


if __name__ == "__main__":
    run_safely(main)
