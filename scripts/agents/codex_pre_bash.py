"""Codex PreToolUse(Bash): deny-only command guard, then fingerprint the working tree."""

from __future__ import annotations

from scripts.agents.bash_edits import call_key, save_pending, snapshot
from scripts.agents.codex_common import codex_decision
from scripts.agents.command_policy import decide
from scripts.agents.common import ROOT, emit, pre_tool_decision, read_payload, run_safely

STATE_DIR = ROOT / ".codex" / "state"


def main() -> int:
    payload = read_payload()
    command = (payload.get("tool_input") or {}).get("command")
    if not isinstance(command, str):
        return 0
    if decision := codex_decision(decide(command)):
        emit(pre_tool_decision(decision))
        return 0
    save_pending(STATE_DIR, call_key(payload), snapshot())
    return 0


if __name__ == "__main__":
    run_safely(main)
