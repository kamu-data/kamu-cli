"""Claude SessionStart: inject the harness contract; a compacted or cleared session reloads skills."""

from __future__ import annotations

from scripts.agents.common import additional_context, emit, read_payload, run_safely
from scripts.agents.session_contract import contract
from scripts.agents.skill_ledger import CLAUDE_STATE, forget_session


def main() -> int:
    payload = read_payload()
    if payload.get("source") in {"clear", "compact"} and payload.get("session_id"):
        forget_session(payload["session_id"], CLAUDE_STATE)
    emit(additional_context("SessionStart", contract()))
    return 0


if __name__ == "__main__":
    run_safely(main)
