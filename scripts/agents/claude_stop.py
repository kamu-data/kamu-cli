"""Claude Stop: remind once when this session changed Rust after the last green clippy, then once
per line its uncommitted change repeats."""

from __future__ import annotations

from scripts.agents.clippy_ledger import stop_reminder
from scripts.agents.common import emit, read_payload, run_safely
from scripts.agents import repeated_lines


def main() -> int:
    payload = read_payload()
    if payload.get("stop_hook_active"):
        return 0
    session_id = payload.get("session_id") or "unknown"
    if reason := stop_reminder(session_id) or repeated_lines.stop_reminder(session_id):
        emit({"decision": "block", "reason": reason})
    return 0


if __name__ == "__main__":
    run_safely(main)
