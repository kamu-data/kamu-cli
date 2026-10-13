"""Codex PostToolUse(apply_patch): rustfmt and added-text checks on every patched file."""

from __future__ import annotations

import sys

from scripts.agents.codex_common import patch_files, patch_text
from scripts.agents.common import additional_context, emit, read_payload, run_safely
from scripts.agents.post_edit import check_edit, report


def main() -> int:
    problems, notes = [], []
    for path, added in patch_files(patch_text(read_payload())).items():
        p, n = check_edit(path, "", "\n".join(added))
        problems += p
        notes += n
    if problems:
        sys.stderr.write(report(list(dict.fromkeys(problems))))
        return 2
    if notes:
        emit(additional_context("PostToolUse", "\n".join(dict.fromkeys(notes))))
    return 0


if __name__ == "__main__":
    run_safely(main)
