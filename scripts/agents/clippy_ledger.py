"""Remember the last green `make clippy`, and whether Rust changed since.

The fingerprint covers HEAD plus every changed or untracked `.rs` and `Cargo.toml`, so it
moves exactly when clippy's verdict could.
"""

from __future__ import annotations

import hashlib
import subprocess

from scripts.agents.common import ROOT, locked_state, now
from scripts.agents.command_policy import lex, segments

STATE = ROOT / ".claude" / "state" / "clippy.json"
PATHSPEC = ["*.rs", "*Cargo.toml", "Cargo.lock"]


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def fingerprint() -> str | None:
    """None when no Rust input differs from HEAD."""
    diff = git("diff", "HEAD", "--", *PATHSPEC)
    untracked = git("ls-files", "--others", "--exclude-standard", "--", *PATHSPEC).split()
    if not diff and not untracked:
        return None
    h = hashlib.sha256(git("rev-parse", "HEAD").encode() + diff.encode())
    for rel in sorted(untracked):
        try:
            h.update(rel.encode() + (ROOT / rel).read_bytes())
        except OSError:
            pass
    return h.hexdigest()


def is_whole_clippy_run(command: str) -> bool:
    """`make clippy` or `make lint` alone on the line, so a success really is clippy's."""
    try:
        parts = segments(lex(command))
    except ValueError:
        return False
    if len(parts) != 1 or parts[0][1]:
        return False
    argv = parts[0][0]
    return argv in (["make", "clippy"], ["make", "lint"])


def record_green() -> None:
    with locked_state(STATE) as state:
        state["green"] = fingerprint() or "clean"
        state["at"] = now()


def note_rust_edit(session_id: str) -> None:
    with locked_state(STATE) as state:
        sessions = state.setdefault("edited_sessions", {})
        sessions[session_id] = now()
        cutoff = now() - 14 * 24 * 3600
        state["edited_sessions"] = {k: v for k, v in sessions.items() if v >= cutoff}


def stop_reminder(session_id: str) -> str | None:
    """A reminder once per Rust state, only for sessions that edited Rust themselves."""
    with locked_state(STATE) as state:
        if session_id not in state.get("edited_sessions", {}):
            return None
        current = fingerprint()
        if current is None or current == state.get("green"):
            return None
        reminded = state.setdefault("reminded", {})
        if reminded.get(session_id) == current:
            return None
        reminded[session_id] = current
    return (
        "Rust files changed since the last green `make clippy` (AGENTS.md, 'Validation'). Run "
        "`make clippy` (or delegate to the rust-builder subagent) and fix what it reports before "
        "handing back — or say explicitly why it is not needed for this change."
    )
