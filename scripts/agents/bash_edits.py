"""Run the post-edit checks over the files a shell command changed.

A shell write declares nothing: `sed -i`, a heredoc, `mv`, a Python one-liner and a script are
all writes, and the ways to write a file are open-ended. So the working tree is asked instead:
every tracked and untracked file is fingerprinted (modification time and size) before the
command and compared after, and a path whose fingerprint moved was written, whatever wrote it.

The two halves run as separate hook processes, so the reading waits on disk under the tool
call's id. Neither half ever blocks the command itself: a reading that cannot be taken or
found costs the checks one command, not the agent its work.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path

from scripts.agents.command_policy import git_subcommand, lex, segments, unwrap
from scripts.agents.common import ROOT, load_state, matches, now, policy, save_state
from scripts.agents.post_edit import check_edit
from scripts.agents.skill_ledger import missing_skills

Snapshot = dict[str, list[int]]

PENDING_DIR = "bash_edits"
PENDING_LIFETIME_SECONDS = 3600

# Git subcommands that rewrite the working tree with content the agent did not write
MOVES_WORKING_TREE = {"switch", "checkout", "pull", "merge", "rebase", "stash", "cherry-pick", "revert", "am", "reset"}


def snapshot(root: Path = ROOT) -> Snapshot:
    """Fingerprint every tracked and untracked file; ignored build output is left out."""
    try:
        listed = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=root, capture_output=True, text=True, check=False,
        )
    except OSError:
        return {}
    if listed.returncode:
        return {}
    taken: Snapshot = {}
    for rel in listed.stdout.split("\0"):
        if not rel:
            continue
        try:
            status = os.stat(root / rel)
        except OSError:
            continue
        taken[rel] = [status.st_mtime_ns, status.st_size]
    return taken


def call_key(payload: dict) -> str:
    """Pair one command's two hook halves: parallel calls and subagents share a session id."""
    return payload.get("tool_use_id") or payload.get("session_id") or "unknown"


def pending_file(state_dir: Path, key: str) -> Path:
    return state_dir / PENDING_DIR / f"{hashlib.sha256(key.encode()).hexdigest()[:32]}.json"


def save_pending(state_dir: Path, key: str, taken: Snapshot) -> None:
    """Hold the reading for the post-command half; readings nobody collected expire."""
    directory = state_dir / PENDING_DIR
    directory.mkdir(parents=True, exist_ok=True)
    for held in directory.glob("*.json"):
        if load_state(held).get("at", 0) < now() - PENDING_LIFETIME_SECONDS:
            held.unlink(missing_ok=True)
    save_state(pending_file(state_dir, key), {"at": now(), "snapshot": taken})


def take_pending(state_dir: Path, key: str) -> Snapshot | None:
    """Return and remove the reading saved for this call, or None when there is none."""
    path = pending_file(state_dir, key)
    entry = load_state(path)
    if not isinstance(entry.get("snapshot"), dict):
        return None
    path.unlink(missing_ok=True)
    return entry["snapshot"]


def changed_paths(before: Snapshot, after: Snapshot) -> list[str]:
    """Paths written or created by the command; a deleted file has nothing left to check."""
    return sorted(rel for rel, mark in after.items() if before.get(rel) != mark)


def moves_working_tree(command: str) -> bool:
    """A git command whose changes are someone else's work, not this command's edits."""
    try:
        tokens = lex(command)
    except ValueError:
        return False
    for raw, _ in segments(tokens):
        argv = unwrap(raw, {})
        if argv and argv[0].rsplit("/", 1)[-1] == "git":
            invocation = git_subcommand(argv[1:])
            if invocation is not None and invocation[0] in MOVES_WORKING_TREE:
                return True
    return False


def is_generated(rel: str) -> bool:
    """Generated files change through their regeneration command, which runs in the shell."""
    return any(matches(rel, rule["paths"]) for rule in policy()["generated"])


def check_changed(paths: list[str], reader: str, skills_state: Path, load_with: str) -> tuple[list[str], list[str]]:
    """Return (problems that block, context notes) for the files a command changed.

    Each file is judged against its committed version, as a whole-file write is. Generated files
    are skipped: their text is the generator's. Only the edit tools refuse hand edits to them; a
    shell command that edits one by hand is not caught here.
    """
    problems, notes = [], []
    for rel in paths:
        if is_generated(rel):
            continue
        try:
            content = (ROOT / rel).read_text()
        except (OSError, UnicodeDecodeError):
            continue
        p, n = check_edit(rel, None, content)
        problems += p
        notes += n
    missing = missing_skills([rel for rel in paths if not is_generated(rel)], reader, skills_state)
    if missing:
        lines = ["Changed through the shell without the governing skill loaded (AGENTS.md, 'What to load for which task'):"]
        for rel, skills in missing.items():
            how = "; ".join(load_with.format(skill=s) for s in skills)
            lines.append(f"- {rel} needs {', '.join(f'`{s}`' for s in skills)} — load with {how}, then re-check the change")
        notes.append("\n".join(lines))
    return list(dict.fromkeys(problems)), list(dict.fromkeys(notes))
