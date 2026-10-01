"""Codex SessionStart: refresh skill requirements and inject the repository contract."""

from scripts.agents.common import additional_context, emit, read_payload, run_safely
from scripts.agents.session_contract import contract
from scripts.agents.skill_ledger import CODEX_STATE, forget_session


def main() -> int:
    payload = read_payload()
    if payload.get("source") in {"clear", "compact"} and payload.get("session_id"):
        forget_session(payload["session_id"], CODEX_STATE)
    emit(additional_context("SessionStart", contract(
        "a shell read of its SKILL.md (for example `cat .agents/skills/<skill>/SKILL.md`)",
        "require explicit user authorization in AGENTS.md; scoped builds are denied by the hook",
    )))
    return 0


if __name__ == "__main__":
    run_safely(main)
