"""Claude PostToolUse(Write|Edit|MultiEdit): rustfmt, added-text checks, reminders."""

from __future__ import annotations

import sys

from scripts.agents.clippy_ledger import note_rust_edit
from scripts.agents.common import additional_context, emit, read_payload, run_safely
from scripts.agents.post_edit import check_edit, report


def edits(tool_name: str, tool_input: dict) -> list[tuple[str | None, str]]:
    """(old, new) text pairs; old=None means the whole file was written."""
    if tool_name == "Write":
        return [(None, tool_input.get("content") or "")]
    if tool_name == "MultiEdit":
        return [(e.get("old_string") or "", e.get("new_string") or "") for e in tool_input.get("edits") or []]
    return [(tool_input.get("old_string") or "", tool_input.get("new_string") or "")]


def main() -> int:
    payload = read_payload()
    tool_input = payload.get("tool_input") or {}
    path = tool_input.get("file_path")
    if not isinstance(path, str):
        return 0
    problems, notes = [], []
    for old, new in edits(payload.get("tool_name", ""), tool_input):
        p, n = check_edit(path, old, new)
        problems += p
        notes += n
    if path.endswith(".rs"):
        note_rust_edit(payload.get("session_id") or "unknown")
    if problems:
        sys.stderr.write(report(list(dict.fromkeys(problems))))
        return 2
    if notes:
        emit(additional_context("PostToolUse", "\n".join(dict.fromkeys(notes))))
    return 0


if __name__ == "__main__":
    run_safely(main)
