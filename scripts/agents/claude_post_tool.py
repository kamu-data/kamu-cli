"""Claude PostToolUse(Skill|Read|Bash): record skill loads and green clippy runs."""

from __future__ import annotations

from scripts.agents.clippy_ledger import is_whole_clippy_run, record_green
from scripts.agents.common import read_payload, run_safely, tool_failed
from scripts.agents.skill_ledger import CLAUDE_STATE, reader_key, record, skill_from_path, skills_read_by


def main() -> int:
    payload = read_payload()
    if tool_failed(payload):
        return 0
    tool, tool_input = payload.get("tool_name"), payload.get("tool_input") or {}
    skills: set[str] = set()
    if tool == "Skill" and isinstance(tool_input.get("skill"), str):
        skills.add(tool_input["skill"].rsplit(":", 1)[-1])
    elif tool == "Read" and isinstance(tool_input.get("file_path"), str):
        if skill := skill_from_path(tool_input["file_path"]):
            skills.add(skill)
    elif tool == "Bash" and isinstance(tool_input.get("command"), str):
        command = tool_input["command"]
        skills |= skills_read_by(command)
        if is_whole_clippy_run(command) and not tool_input.get("run_in_background"):
            record_green()
    record(skills, reader_key(payload), CLAUDE_STATE)
    return 0


if __name__ == "__main__":
    run_safely(main)
