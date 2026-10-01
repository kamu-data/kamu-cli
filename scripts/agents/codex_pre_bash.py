"""Codex PreToolUse(Bash): deny-only command guard."""

from __future__ import annotations

from scripts.agents.codex_common import codex_decision
from scripts.agents.command_policy import decide
from scripts.agents.common import emit, pre_tool_decision, read_payload, run_safely


def main() -> int:
    command = (read_payload().get("tool_input") or {}).get("command")
    if isinstance(command, str) and (decision := codex_decision(decide(command))):
        emit(pre_tool_decision(decision))
    return 0


if __name__ == "__main__":
    run_safely(main)
