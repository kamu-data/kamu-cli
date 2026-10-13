"""Shared plumbing for agent hooks: repository paths, policy table, globs, state files.

Every hook must fail open on its own errors: a broken hook may lose a check, it may never
block work. Policy decisions are the only reason a hook refuses anything.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time
from contextlib import contextmanager
from functools import cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
POLICY_FILE = ROOT / ".claude" / "hooks" / "governed_paths.json"

Decision = tuple[str, str]
"""A verdict (`deny` or `ask`) and the reason shown to the agent."""


@cache
def policy() -> dict:
    return json.loads(POLICY_FILE.read_text())


@cache
def glob_regex(pattern: str) -> re.Pattern[str]:
    """Translate a repository glob: `**` spans directories, `*` and `?` stay within one."""
    out, i = [], 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("".join(out) + r"\Z")


def matches(path: str, patterns: list[str]) -> bool:
    return any(glob_regex(p).match(path) for p in patterns)


def repo_relative(path: str | os.PathLike, root: Path = ROOT) -> str | None:
    """Return a POSIX path relative to the repository, or None for paths outside it."""
    p = Path(path)
    if not p.is_absolute():
        p = root / p
    try:
        return p.resolve().relative_to(root.resolve()).as_posix()
    except (ValueError, OSError):
        return None


def read_payload() -> dict:
    try:
        payload = json.load(sys.stdin)
    except (json.JSONDecodeError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


def emit(output: dict) -> None:
    print(json.dumps(output))


def pre_tool_decision(decision: Decision) -> dict:
    verdict, reason = decision
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": verdict,
            "permissionDecisionReason": reason,
        }
    }


def additional_context(event: str, text: str) -> dict:
    return {"hookSpecificOutput": {"hookEventName": event, "additionalContext": text}}


def load_state(path: Path) -> dict:
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def save_state(path: Path, data: dict) -> None:
    """Write atomically, so parallel hooks never read a half-written file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=1, sort_keys=True)
    os.replace(tmp, path)


@contextmanager
def locked_state(path: Path):
    """Serialize the complete read-modify-write cycle across hook processes."""
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = load_state(path)
        yield state
        save_state(path, state)


def tool_failed(payload: dict) -> bool:
    response = payload.get("tool_response")
    if isinstance(response, dict):
        if response.get("is_error") or response.get("isError"):
            return True
        code = response.get("exit_code", response.get("exitCode"))
        return code is not None and code != 0
    if isinstance(response, str):
        match = re.search(r"^Process exited with code (-?\d+)\s*$", response, re.M)
        return match is not None and int(match[1]) != 0
    return False


def now() -> float:
    return time.time()


def run_safely(main) -> None:
    """Run a hook entry point; any unexpected error exits 0 so the tool call proceeds."""
    try:
        code = main()
    except Exception as e:  # noqa: BLE001 - a hook must never break the session
        sys.stderr.write(f"agent hook error (ignored): {e!r}\n")
        code = 0
    sys.exit(code or 0)
