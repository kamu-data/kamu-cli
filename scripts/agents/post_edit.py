"""Checks run after an agent edits files: format Rust and Cargo.toml, then judge only the text it added.

Only added lines are judged, so existing code that predates a rule never blocks an
unrelated edit. A failed check exits 2: the edit already landed, and the message tells the
agent which rule it broke and how to fix it forward.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

from scripts.agents.common import ROOT, matches, repo_relative

LICENSE_HEADER = (ROOT / "docs" / "license_header.txt").read_text().strip()
LICENSE_EXEMPT = ["src/utils/datafusion-cli/**"]
RUST_EDITION = "2024"

CITATION = "comments never cite plans, tickets or issues — the referent rots (kamu-prose-and-comments)"
COMMENT = re.compile(r"(?:^|[^:\"'/])//+!?\s?(.*)$")
RULES: list[tuple[re.Pattern[str], str, bool]] = [
    # (pattern, message, applies to comment text only)
    (re.compile(r"\bassert!\s*\(\s*matches!"), "use `assert_matches!(expr, pattern)` directly, never `assert!(matches!(...))` (kamu-test-harness)", False),
    (re.compile(r"#!?\[\s*(allow\s*\(\s*clippy::|expect\s*\()"), "do not silence lints with `#[allow(clippy::...)]` / `#[expect(...)]` — fix the cause, or ask the user first (AGENTS.md, 'Validation')", False),
    (re.compile(r"\bdbg!\s*\("), "`dbg!` is disallowed (clippy.toml)", False),
    (re.compile(r"\b(plan|slice)\s+#?\d+\b|\b(jira|ticket|issue|pr)\s*#\s*\d+|\bjira\b", re.I), CITATION, True),
    (re.compile(r"\b(?!RF-)[A-Z][A-Z0-9]+-\d{2,}\b"), CITATION, True),
    (re.compile(r"\b(was|were) previously\b|\bused to (be|return|have|hold|take)\b|\bno longer (exists?|returns?|used|supported|called|present)\b|\b(has|have) been (renamed|replaced|removed|moved)\b|\brenamed from\b|\bthis (change|patch|commit|PR)\b", re.I), "comments describe the code as it is, never its history (kamu-prose-and-comments)", True),
]
DIVIDER = re.compile(r"^/{4,119}$|^/{121,}$")
IGNORED_UPPER_TOKENS = re.compile(r"\b(UTF|SHA|ISO|RFC|HTTP|TLS|AES|RSA|ECDSA|ED|CRC|X|BLAKE|MD|S|UUID|IPV|HMAC|PKCS)-\d+\b", re.I)


def added_lines(old: str, new: str) -> list[str]:
    """Lines present in `new` but not in `old` — a cheap, order-insensitive diff."""
    before = set(old.splitlines())
    return [line for line in new.splitlines() if line not in before]


def head_version(rel: str) -> str | None:
    try:
        return subprocess.run(["git", "show", f"HEAD:{rel}"], cwd=ROOT, capture_output=True, text=True, check=True).stdout
    except (subprocess.CalledProcessError, OSError):
        return None


def check_rust_lines(rel: str, lines: list[str]) -> list[str]:
    problems = []
    for line in lines:
        if DIVIDER.match(line):
            problems.append(f"{rel}: dividing lines are exactly 120 `/` characters (repo lint `dividing_lines`)")
        comment = COMMENT.search(line)
        comment_text = IGNORED_UPPER_TOKENS.sub("", comment.group(1)) if comment else ""
        for pattern, message, comment_only in RULES:
            subject = comment_text if comment_only else line
            if subject and pattern.search(subject):
                problems.append(f"{rel}: {message}\n    > {line.strip()}")
    return problems


def check_license(rel: str, path: Path) -> list[str]:
    if matches(rel, LICENSE_EXEMPT) or head_version(rel) is not None:
        return []
    try:
        content = path.read_text()
    except OSError:
        return []
    if content.startswith(LICENSE_HEADER):
        return []
    return [f"{rel}: new Rust files start with the license header from docs/license_header.txt (repo lint `license_header`)"]


def rustfmt(path: Path) -> str | None:
    """Format one file; return an error description, or None when it formatted cleanly."""
    try:
        result = subprocess.run(["rustfmt", "--edition", RUST_EDITION, str(path)], cwd=ROOT, capture_output=True, text=True, timeout=60)
    except (OSError, subprocess.TimeoutExpired) as e:
        return str(e)
    return None if result.returncode == 0 else result.stderr.strip()[:600]


def format_manifest(path: Path) -> str | None:
    """Sort and format one Cargo.toml as `make fmt` does; return why it could not, or None."""
    missing = [tool for tool in ("cargo-sort", "taplo") if not shutil.which(tool)]
    if missing:
        return f"{', '.join(missing)} not installed"
    for command in (["cargo", "sort", "-g", "-n", str(path.parent)], ["taplo", "fmt", str(path)]):
        try:
            result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.TimeoutExpired) as e:
            return str(e)
        if result.returncode != 0:
            return result.stderr.strip()[:600]
    return None


def nudges(rel: str, added: list[str]) -> list[str]:
    out = []
    text = "\n".join(added)
    crate = re.match(r"(.*?/(?:postgres|sqlite|cache-postgres|cache-sqlite))/", rel)
    if rel.startswith("migrations/"):
        out.append("SQL changed: run `make sqlx-prepare` and keep the regenerated `.sqlx/` files.")
    elif crate and re.search(r"query(_as|_scalar)?!", text):
        out.append(f"SQL changed: run `(cd {crate.group(1)} && cargo sqlx prepare)` and keep the regenerated `.sqlx/` files.")
    if rel.startswith("src/adapter/graphql/src/"):
        out.append("GraphQL changed: run `make resources-graphql-schema` and review the `resources/schema.gql` diff.")
    m = re.match(r"src/e2e/app/cli/(postgres|sqlite)/", rel)
    if m:
        twin = "sqlite" if m.group(1) == "postgres" else "postgres"
        out.append(f"E2E wiring changed: mirror it in `src/e2e/app/cli/{twin}/` (lockstep rule, kamu-cli-e2e-tests).")
    return out


def check_edit(path_str: str, old: str | None, new: str) -> tuple[list[str], list[str]]:
    """Return (problems that block, context notes) for one edited file.

    `old` is the replaced text for an in-place edit; None means the whole file was written,
    so its added lines are judged against the committed version.
    """
    rel = repo_relative(path_str)
    if not rel:
        return [], []
    path = ROOT / rel
    if old is None:
        old = head_version(rel) or ""
    added = added_lines(old, new)
    problems, notes = [], nudges(rel, added)
    if rel.endswith(".rs") and path.exists():
        if err := rustfmt(path):
            notes.append(f"rustfmt could not format {rel}: {err}")
        problems += check_rust_lines(rel, added)
        problems += check_license(rel, path)
    if path.name == "Cargo.toml" and path.exists():
        if err := format_manifest(path):
            notes.append(f"could not format {rel} ({err}): run `make fmt`")
    return problems, notes


def report(problems: list[str]) -> str:
    return "Edit landed but breaks repository rules — fix it forward:\n" + "\n".join(f"- {p}" for p in problems) + "\n"
