"""Record which skills a session has loaded, and refuse guarded edits until it has.

A load is recorded from what the agent actually did — a Skill tool call, a Read of a
`SKILL.md`, or a shell command reading one — never from a claim. Records are keyed by session
*and* agent: a subagent shares its parent's session id but not its context, so a skill it
loaded must not satisfy the guard for the parent, nor the other way round.
"""

from __future__ import annotations

import re
from pathlib import Path

from scripts.agents.common import ROOT, load_state, locked_state, matches, now, policy, repo_relative
from scripts.agents.command_policy import lex, segments

EXPIRY_SECONDS = 14 * 24 * 3600
SKILL_FILE = re.compile(r"(?:^|/)\.(?:claude|agents)/skills/([^/]+)/SKILL\.md$")
READERS = {"cat", "head", "tail", "less", "more", "bat", "sed", "awk", "grep", "rg"}

CLAUDE_STATE = ROOT / ".claude" / "state" / "skills.json"
CODEX_STATE = ROOT / ".codex" / "state" / "skills.json"
CLAUDE_LOAD_WITH = "the Skill tool, skill: {skill}"
CODEX_LOAD_WITH = "`cat .agents/skills/{skill}/SKILL.md`"


def reader_key(payload: dict) -> str:
    return f"{payload.get('session_id') or 'unknown'}/{payload.get('agent_id') or 'main'}"


def governing_skills(path: str) -> list[str]:
    """The first matching ordinary rule, plus every matching baseline rule.

    Mirrors the AGENTS.md table: ordinary rows are matched top to bottom and the first match
    wins; baseline rows (e.g. Rust style for every `.rs` file) apply in addition.
    """
    rules = policy()["skills"]
    first = next((r["skill"] for r in rules if not r.get("baseline") and matches(path, r["paths"])), None)
    baseline = [r["skill"] for r in rules if r.get("baseline") and matches(path, r["paths"])]
    return ([first] if first else []) + [s for s in baseline if s != first]


def skill_from_path(path: str) -> str | None:
    m = SKILL_FILE.search(path.replace("\\", "/"))
    return m.group(1) if m else None


def drop_output_redirects(argv: list[str]) -> list[str]:
    """Remove output redirections with their targets, but keep input ones' targets: `cat > SKILL.md`
    authors a skill, while `cat < SKILL.md` reads it."""
    out: list[str] = []
    skip = False
    for tok in argv:
        if skip:
            skip = False
        elif set(tok) <= set("<>&") and ">" in tok:
            if out and out[-1].isdigit():
                out.pop()
            skip = True
        elif set(tok) <= set("<&") and "<" in tok:
            if out and out[-1].isdigit():
                out.pop()
        else:
            out.append(tok)
    return out


def skills_read_by(command: str) -> set[str]:
    """Skills whose SKILL.md a shell command reads (Codex's way of loading one)."""
    try:
        tokens = lex(command)
    except ValueError:
        return set()
    found = set()
    for raw, _ in segments(tokens):
        argv = drop_output_redirects(raw)
        if argv and argv[0].rsplit("/", 1)[-1] in READERS and not (argv[0] == "sed" and "-i" in argv):
            found |= {s for a in argv[1:] if (s := skill_from_path(a))}
    return found


def loaded(key: str, state_file: Path) -> set[str]:
    entry = load_state(state_file).get(key) or {}
    if entry.get("updated", 0) < now() - EXPIRY_SECONDS:
        return set()
    return set(entry.get("skills", []))


def record(skills: set[str], key: str, state_file: Path) -> None:
    if not skills:
        return
    with locked_state(state_file) as state:
        cutoff = now() - EXPIRY_SECONDS
        expired = [k for k, v in state.items() if not isinstance(v, dict) or v.get("updated", 0) < cutoff]
        for k in expired:
            del state[k]
        entry = state.setdefault(key, {"skills": []})
        entry["skills"] = sorted(set(entry["skills"]) | skills)
        entry["updated"] = now()


def forget_session(session_id: str, state_file: Path) -> None:
    """After compaction or /clear the skill text is gone from context: require a reload."""
    with locked_state(state_file) as state:
        for key in list(state):
            if key.startswith(f"{session_id}/"):
                del state[key]


def missing_skills(paths: list[str], key: str, state_file: Path) -> dict[str, list[str]]:
    """Map each guarded repository path to the skills it still needs."""
    have = loaded(key, state_file)
    missing = {}
    for path in paths:
        rel = repo_relative(path)
        needed = [s for s in governing_skills(rel) if s not in have] if rel else []
        if needed:
            missing[rel] = needed
    return missing


def refusal(missing: dict[str, list[str]], load_with: str) -> str:
    lines = ["Refused until this session loads the governing skills (AGENTS.md, 'What to load for which task'):"]
    for path, skills in missing.items():
        how = "; ".join(load_with.format(skill=s) for s in skills)
        lines.append(f"- {path} needs {', '.join(f'`{s}`' for s in skills)} — load with {how}")
    lines.append("Then retry the edit. A subagent must load the skill itself.")
    return "\n".join(lines)
