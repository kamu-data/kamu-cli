"""Codex PostToolUse(Bash): Codex loads a skill by reading its SKILL.md, so record it here."""

from __future__ import annotations

from scripts.agents.common import read_payload, run_safely, tool_failed
from scripts.agents.skill_ledger import CODEX_STATE, reader_key, record, skills_read_by


def main() -> int:
    payload = read_payload()
    if tool_failed(payload):
        return 0
    command = (payload.get("tool_input") or {}).get("command")
    if isinstance(command, str):
        record(skills_read_by(command), reader_key(payload), CODEX_STATE)
    return 0


if __name__ == "__main__":
    run_safely(main)
