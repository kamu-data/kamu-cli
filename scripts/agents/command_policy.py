"""Judge a shell command line before an agent runs it.

The line is lexed the way a POSIX shell would split it, every simple command is unwrapped
from launchers (`env`, `timeout`, `xargs`, `bash -c '...'`, ...) and judged on its own.
A deny anywhere on the line wins over an ask, so an approved first command can never carry
a destructive second one through.

What this cannot see: commands built at runtime (`$(...)`, variables holding commands),
scripts the command runs, and `git checkout <path>` written without `--` (indistinguishable
from a branch switch). AGENTS.md still forbids those.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass

from scripts.agents.common import Decision

MAX_DEPTH = 16
SEPARATORS = {";", "&&", "||", "|", "&", "(", ")", ";;", "|&"}
KEYWORDS = {"{", "}", "!", "if", "then", "else", "elif", "fi", "do", "done", "while", "until", "time"}
ASSIGNMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*=")
HEREDOC = re.compile(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1")

GIT_ASK = {"commit", "push", "merge", "rebase", "cherry-pick", "revert", "am", "tag"}
CARGO_BUILDING = {
    "build", "b", "check", "c", "clippy", "test", "t", "nextest", "run", "r",
    "doc", "d", "bench", "rustc", "fix",
}
OUTPUT_HEAVY_MAKE = re.compile(r"^(test.*|clippy|lint.*)$")

BUILD_SCOPE_REASON = (
    "`-p`/`--package` scopes the build to one crate (AGENTS.md, 'Build scope'). With workspace "
    "feature unification such a build does not reuse full-workspace artifacts and recompiles "
    "heavy dependencies (DataFusion, Arrow, SQLx), so it is usually slower. Build the whole "
    "workspace and narrow tests with nextest filtersets instead: "
    "`cargo nextest run -E 'package(<crate>) and test(<name>)'`. Approve only for a crate with a "
    "small dependency tree."
)
SQLX_REASON = (
    "Never set SQLX_OFFLINE on the command line (AGENTS.md, 'Hard rules'): `.env` files configure "
    "it, and forcing it checks queries against the stale `.sqlx` cache. Run the plain command."
)
TRUNCATE_REASON = (
    "Build/test/lint output piped into head/tail loses the failures (AGENTS.md, 'Validation'). "
    "Run it whole, or delegate to the rust-builder / rust-tester subagent to summarize it."
)
DISCARD_REASON = (
    "`{cmd}` discards uncommitted work irreversibly (AGENTS.md, 'Hard rules: never discard "
    "uncommitted work'). Fix forward with Edit/Write; compare with `git show HEAD:<path>`. If the "
    "user explicitly asked to throw changes away, ask them to run it."
)
APPROVAL_REASON = (
    "`git {sub}` alters history or publishes and needs explicit user approval for this specific "
    "instance (AGENTS.md, 'Hard rules')."
)


@dataclass(frozen=True)
class Verdict:
    decision: Decision
    kind: str
    """`discard`, `approval`, `build-scope`, `sqlx`, `truncate` — adapters map kinds differently."""


def strip_heredocs(command: str) -> str:
    """Drop heredoc bodies: they are data fed to a program, not commands."""
    lines, out, i = command.split("\n"), [], 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        delims = [m.group(2) for m in HEREDOC.finditer(line)]
        i += 1
        for delim in delims:
            while i < len(lines) and lines[i].strip() != delim:
                i += 1
            i += 1
    return "\n".join(out)


def lex(command: str) -> list[str]:
    command = strip_heredocs(command).replace("\\\n", " ")
    # Newlines separate commands; shlex would otherwise treat them as plain whitespace.
    lexer = shlex.shlex(command.replace("\n", " ; "), posix=True, punctuation_chars=";&|()<>")
    lexer.whitespace_split = True
    lexer.commenters = "#"
    return list(lexer)


def segments(tokens: list[str]) -> list[tuple[list[str], str]]:
    """Split into simple commands, each paired with the separator that follows it."""
    result, current = [], []
    for tok in tokens:
        if tok in SEPARATORS:
            result.append((current, tok))
            current = []
        else:
            current.append(tok)
    result.append((current, ""))
    return [(argv, sep) for argv, sep in result if argv]


def drop_redirects(argv: list[str]) -> list[str]:
    """Remove redirections. shlex splits `2>&1` into `2`, `>&`, `1`: drop the operator, the fd
    before it, and the target after it."""
    out: list[str] = []
    skip = False
    for tok in argv:
        if skip:
            skip = False
        elif set(tok) <= set("<>&") and ("<" in tok or ">" in tok):
            if out and out[-1].isdigit():
                out.pop()
            skip = True
        else:
            out.append(tok)
    return out


def unwrap(argv: list[str], env: dict[str, str]) -> list[str]:
    """Peel launchers off a simple command; return the argv of the program that really runs."""
    argv = drop_redirects(argv)
    while argv:
        head = argv[0].rsplit("/", 1)[-1]
        if head in KEYWORDS:
            argv = argv[1:]
        elif ASSIGNMENT.match(argv[0]):
            key, _, value = argv[0].partition("=")
            env[key] = value
            argv = argv[1:]
        elif head == "export":
            for tok in argv[1:]:
                if ASSIGNMENT.match(tok):
                    key, _, value = tok.partition("=")
                    env[key] = value
            return []
        elif head == "env":
            argv = argv[1:]
            while argv and (argv[0].startswith("-") or ASSIGNMENT.match(argv[0])):
                tok = argv.pop(0)
                if ASSIGNMENT.match(tok):
                    key, _, value = tok.partition("=")
                    env[key] = value
                elif tok in {"-u", "--unset", "-C", "--chdir"} and argv:
                    argv.pop(0)
                elif tok in {"-S", "--split-string"} and argv:
                    argv = shlex.split(argv.pop(0)) + argv
                elif tok == "--":
                    break
        elif head in {"sudo", "doas", "nohup", "exec", "command", "builtin", "setsid", "stdbuf", "ionice", "nice", "time"}:
            argv = argv[1:]
            while argv and argv[0].startswith("-"):
                tok = argv.pop(0)
                if head == "nice" and tok == "-n" and argv:
                    argv.pop(0)
        elif head == "timeout":
            argv = argv[1:]
            while argv and argv[0].startswith("-"):
                tok = argv.pop(0)
                if tok in {"-s", "--signal", "-k", "--kill-after"} and argv:
                    argv.pop(0)
            argv = argv[1:]  # the duration
        elif head == "xargs":
            argv = argv[1:]
            while argv and argv[0].startswith("-"):
                tok = argv.pop(0)
                if tok in {"-I", "-n", "-P", "-L", "-d", "-E", "-s", "-a"} and argv:
                    argv.pop(0)
        else:
            return argv
    return argv


def git_subcommand(args: list[str]) -> tuple[str, list[str]] | None:
    """Split `git`'s arguments into the subcommand and its own arguments, past global options."""
    i = 0
    while i < len(args) and args[i].startswith("-"):
        i += 2 if args[i] in {"-C", "-c", "--git-dir", "--work-tree", "--namespace"} else 1
    if i >= len(args):
        return None
    return args[i], args[i + 1 :]


def judge_git(args: list[str]) -> Verdict | None:
    if (invocation := git_subcommand(args)) is None:
        return None
    sub, rest = invocation
    flags = set(rest)

    def short(letter: str) -> bool:
        return any(a.startswith("-") and not a.startswith("--") and letter in a[1:] for a in rest)

    def discard() -> Verdict:
        return Verdict(("deny", DISCARD_REASON.format(cmd=" ".join(["git", sub, *rest]))), "discard")

    if sub == "reset" and "--hard" in flags:
        return discard()
    if sub == "clean" and (short("f") or "--force" in flags) and not (short("n") or "--dry-run" in flags):
        return discard()
    if sub == "checkout":
        if flags & {"-b", "-B", "--orphan"}:
            return None
        if "--" in flags or "." in flags or flags & {"-f", "--force"}:
            return discard()
    if sub == "switch" and flags & {"-f", "--force", "--discard-changes"}:
        return discard()
    if sub == "restore":
        staged = bool(flags & {"--staged", "-S"})
        worktree = bool(flags & {"--worktree", "-W"})
        if not staged or worktree:
            return discard()
    if sub == "stash":
        action = rest[0] if rest else "push"
        if action not in {"list", "show", "apply", "pop", "branch"}:
            return discard()
    if sub == "read-tree" and "--reset" in flags and (short("u") or "-u" in flags):
        return discard()
    if sub == "checkout-index" and (flags & {"-f", "--force"} or short("f")):
        return discard()
    moves_head = sub == "reset" and (
        flags & {"--soft", "--mixed", "--keep", "--merge"} or any(a.startswith(("HEAD~", "HEAD^", "origin/")) for a in rest)
    )
    if sub in GIT_ASK or moves_head or (sub == "branch" and flags & {"-D", "-d", "--delete", "-f", "--force", "-m", "-M"}):
        if sub == "tag" and (not rest or flags & {"-l", "--list"}):
            return None
        return Verdict(("ask", APPROVAL_REASON.format(sub=sub)), "approval")
    return None


def judge_cargo(args: list[str]) -> Verdict | None:
    i = 0
    if i < len(args) and args[i].startswith("+"):
        i += 1
    while i < len(args) and args[i].startswith("-"):
        i += 2 if args[i] in {"-Z", "--config", "-C", "--color"} else 1
    if i >= len(args) or args[i] not in CARGO_BUILDING:
        return None
    for tok in args[i + 1 :]:
        if tok == "--":
            break
        if tok in {"-p", "--package"} or tok.startswith("--package=") or (tok.startswith("-p") and not tok.startswith("--") and len(tok) > 2):
            return Verdict(("ask", BUILD_SCOPE_REASON), "build-scope")
    return None


def is_output_heavy(argv: list[str]) -> bool:
    if not argv:
        return False
    head = argv[0].rsplit("/", 1)[-1]
    if head == "cargo":
        return any(a in CARGO_BUILDING for a in argv[1:3])
    if head == "make":
        return any(OUTPUT_HEAVY_MAKE.match(a) for a in argv[1:] if not a.startswith("-"))
    return False


def verdicts(command: str, depth: int = 0) -> list[Verdict]:
    if depth > MAX_DEPTH:
        return [Verdict(("deny", "Command nests launchers too deeply to judge; simplify it."), "discard")]
    try:
        tokens = lex(command)
    except ValueError:
        return fallback(command)

    found: list[Verdict] = []
    previous: list[str] = []
    previous_sep = ""
    for raw, sep in segments(tokens):
        env: dict[str, str] = {}
        argv = unwrap(raw, env)
        if "SQLX_OFFLINE" in env:
            found.append(Verdict(("deny", SQLX_REASON), "sqlx"))
        if argv:
            head = argv[0].rsplit("/", 1)[-1]
            if head in {"bash", "sh", "zsh", "dash"} and "-c" in argv[1:-1]:
                found += verdicts(argv[argv.index("-c") + 1], depth + 1)
            elif head == "eval":
                found += verdicts(" ".join(argv[1:]), depth + 1)
            elif head == "git":
                v = judge_git(argv[1:])
                found += [v] if v else []
            elif head == "cargo":
                v = judge_cargo(argv[1:])
                found += [v] if v else []
            elif head in {"head", "tail"} and previous_sep in {"|", "|&"} and is_output_heavy(previous):
                found.append(Verdict(("deny", TRUNCATE_REASON), "truncate"))
        if argv:
            previous = argv
        previous_sep = sep
    return found


def fallback(command: str) -> list[Verdict]:
    """Unlexable input (e.g. unbalanced quotes): scan the raw text, erring towards refusal."""
    found = []
    if re.search(r"\bgit\s+(reset\s+--hard|clean\s+-\w*f|checkout\s+(--|\.)|stash(\s+(push|save|drop|clear))?\s*($|[;&|]))", command):
        found.append(Verdict(("deny", DISCARD_REASON.format(cmd="git ...")), "discard"))
    if re.search(r"\bSQLX_OFFLINE=", command):
        found.append(Verdict(("deny", SQLX_REASON), "sqlx"))
    if re.search(r"\bgit\s+(commit|push|merge|rebase)\b", command):
        found.append(Verdict(("ask", APPROVAL_REASON.format(sub="...")), "approval"))
    return found


def combine(found: list[Verdict]) -> Verdict | None:
    """Deny beats ask; reasons of the winning verdict are joined, each once."""
    for verdict in ("deny", "ask"):
        hits = [v for v in found if v.decision[0] == verdict]
        if hits:
            reasons = list(dict.fromkeys(v.decision[1] for v in hits))
            return Verdict((verdict, "\n".join(reasons)), hits[0].kind)
    return None


def decide(command: str) -> Verdict | None:
    return combine(verdicts(command))
