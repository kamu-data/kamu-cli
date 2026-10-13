"""Claude Stop: remind once when this session changed Rust after the last green clippy."""

from __future__ import annotations

from scripts.agents.clippy_ledger import stop_reminder
from scripts.agents.common import emit, read_payload, run_safely


def main() -> int:
    payload = read_payload()
    if payload.get("stop_hook_active"):
        return 0
    if reason := stop_reminder(payload.get("session_id") or "unknown"):
        emit({"decision": "block", "reason": reason})
    return 0


if __name__ == "__main__":
    run_safely(main)
