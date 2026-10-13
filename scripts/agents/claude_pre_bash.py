"""Claude PreToolUse(Bash): judge the command line, then fingerprint the working tree."""

from __future__ import annotations

from scripts.agents.bash_edits import call_key, save_pending, snapshot
from scripts.agents.command_policy import Verdict, combine, verdicts
from scripts.agents.common import ROOT, emit, pre_tool_decision, read_payload, run_safely
from scripts.agents.edit_policy import memory_decision

STATE_DIR = ROOT / ".claude" / "state"


def main() -> int:
    payload = read_payload()
    command = (payload.get("tool_input") or {}).get("command")
    if not isinstance(command, str):
        return 0
    found = verdicts(command)
    if memory := memory_decision(command):
        found.append(Verdict(memory, "memory"))
    if verdict := combine(found):
        emit(pre_tool_decision(verdict.decision))
        if verdict.decision[0] == "deny":
            return 0
    save_pending(STATE_DIR, call_key(payload), snapshot())
    return 0


if __name__ == "__main__":
    run_safely(main)
