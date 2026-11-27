"""Find Rust lines a change adds again and again: a field threaded through every copy of a shape.

A line counts when the change adds it at least `MIN_COUNT` times and the base tree does not
already use it as an idiom. Punctuation, imports, attributes, comments, declarations and macro
openers are skipped, as they repeat in any change.

Run by hand over a branch: `python3 -m scripts.agents.repeated_lines --base <ref>`.
"""

from __future__ import annotations

import argparse
import re
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass

from scripts.agents.common import ROOT, locked_state
from scripts.agents.clippy_ledger import STATE as CLIPPY_STATE

STATE = ROOT / ".claude" / "state" / "repeated_lines.json"
MIN_COUNT = 3
MIN_LENGTH = 10
# A line the base tree has this many times more often than the change adds it is an idiom
IDIOM_RATIO = 4
REPORT_LIMIT = 10

_NOISE = re.compile(
    r"""^(
      [\s{}()\[\];,.?]*                          # punctuation only
    | //.* | /\*.* | \*.*                        # comments
    | use\s.* | pub(\(crate\))?\s+(use|mod)\s.* | mod\s.* | \#!?\[.*
    | [})\]]*\.\w+(\(\))?(\.\w+(\(\))?)*[?;,]*   # bare chain tails: .await?; }).unwrap();
    | (Ok|Err|Some)\(\(\)\)[;,]? | None[;,]? | Ok\(\w+\)[;,]?
    | (let\s+)?\w+[;,]? | return\s*(\w+|Ok\(\(\)\))?;? | \}\s*else\s*\{ | (async\s+)?move\s*\{
    | \w+,\s*\w+(,\s*\w+)*,?                     # bare argument lists
    | let\s+(mut\s+)?\w+\s*=\s*[\w:]+::new\(\);   # a test's own harness or fixture
    )$""",
    re.X,
)
# Declarations repeat by nature: parameters and fields typed `&T`, `impl T`, `dyn T` or a bare
# type, macro arguments `key = value`, and lines opening a call, block or list
_DECLARATION = re.compile(
    r"^\w+:\s*(&|impl\s|dyn\s|(?!None\b)[A-Z]\w*(<.*>)?,?$)|^\w+\s*=\s*[\w:]+,?$|[({\[]$"
)


@dataclass(frozen=True)
class Repeat:
    text: str
    paths: tuple[str, ...]

    @property
    def count(self) -> int:
        return len(self.paths)


def added_lines(diff: str) -> dict[str, list[str]]:
    """Each candidate line the diff adds to a `.rs` file, stripped, with the file of each copy."""
    found: dict[str, list[str]] = defaultdict(list)
    path = None
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = line[6:] if line.startswith("+++ b/") else None
            continue
        if path is None or not path.endswith(".rs") or not line.startswith("+"):
            continue
        text = line[1:].strip()
        if len(text) < MIN_LENGTH or _NOISE.match(text) or _DECLARATION.search(text):
            continue
        found[text].append(path)
    return found


def repeats(diff: str, base_counts) -> list[Repeat]:
    """Lines added `MIN_COUNT` times or more that are not idioms of the base, most copies first.

    `base_counts(texts)` returns how often each text occurs, stripped, in the base tree.
    """
    candidates = {t: p for t, p in added_lines(diff).items() if len(p) >= MIN_COUNT}
    base = base_counts(list(candidates)) if candidates else Counter()
    found = [
        Repeat(text, tuple(paths))
        for text, paths in candidates.items()
        if base.get(text, 0) < IDIOM_RATIO * len(paths)
    ]
    return sorted(found, key=lambda r: (-r.count, r.text))


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def git_base_counts(rev: str):
    def counts(texts: list[str]) -> Counter:
        result = subprocess.run(
            ["git", "grep", "-h", "-F", "-f", "-", rev, "--", "*.rs"],
            cwd=ROOT, input="\n".join(texts), capture_output=True, text=True,
        )
        wanted = set(texts)
        return Counter(s for s in (line.strip() for line in result.stdout.splitlines()) if s in wanted)

    return counts


def working_tree_diff(base: str) -> str:
    """The change from `base` to the working tree, untracked Rust files included as added."""
    diff = git("diff", base, "--", "*.rs")
    for rel in git("ls-files", "--others", "--exclude-standard", "--", "*.rs").split():
        try:
            body = (ROOT / rel).read_text()
        except (OSError, UnicodeDecodeError):
            continue
        diff += f"\n+++ b/{rel}\n" + "".join(f"+{line}\n" for line in body.splitlines())
    return diff


def describe(found: list[Repeat]) -> str:
    lines = []
    for r in found[:REPORT_LIMIT]:
        files = sorted(set(r.paths))
        where = ", ".join(files[:3]) + (f" and {len(files) - 3} more" if len(files) > 3 else "")
        lines.append(f"- {r.count}x `{r.text}` ({where})")
    if len(found) > REPORT_LIMIT:
        lines.append(f"- … and {len(found) - REPORT_LIMIT} more lines")
    return "\n".join(lines)


def stop_reminder(session_id: str) -> str | None:
    """Once per newly repeated line, for sessions that edited Rust, over the uncommitted change."""
    with locked_state(CLIPPY_STATE) as clippy_state:
        if session_id not in clippy_state.get("edited_sessions", {}):
            return None
    found = repeats(working_tree_diff("HEAD"), git_base_counts("HEAD"))
    with locked_state(STATE) as state:
        reported = state.setdefault("reported", {})
        known = set(reported.get(session_id, []))
        new = [r for r in found if r.text not in known]
        if not new:
            return None
        reported[session_id] = sorted(known | {r.text for r in found})
    return (
        "The uncommitted change adds these lines several times each (kamu-rust-style, 'Leave no "
        "dead or duplicate code'). Give each repeated shape one home — a constructor with "
        "defaults, a helper, a harness method, a shared query macro — then add the line once; or "
        "say why the copies are right:\n" + describe(new)
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base", default="HEAD", help="compare the working tree with this ref")
    args = parser.parse_args()
    base = git("merge-base", args.base, "HEAD").strip() if args.base != "HEAD" else "HEAD"
    found = repeats(working_tree_diff(base), git_base_counts(base))
    print(describe(found) if found else "No repeated lines.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
