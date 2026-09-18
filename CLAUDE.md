# CLAUDE.md

This file provides Claude Code-specific guidance. For the full agent guide, read [`AGENTS.md`](AGENTS.md) — it is the canonical source for validation, testing, style, and skill-loading rules.

## Environment

**Do not pass `SQLX_OFFLINE=true` on the command line.** This checkout runs against a live database: `make sqlx-local-setup` has written per-crate `.env` files (gitignored) with a `DATABASE_URL` and `SQLX_OFFLINE=false`, and the DB containers run under Podman. Setting the variable on the command line overrides that and silently forces query checking against the stale `.sqlx` cache instead of the real schema — which defeats the point of the local setup and hides schema drift.

Just run the plain commands:

```bash
cargo build
cargo nextest run -E 'test(test_name_here)'
make clippy
```

Background: the root [`.env`](.env) sets `SQLX_OFFLINE=true` as the repo-wide default so CI — which has no database service — can compile the postgres crates from the committed `.sqlx` cache. The per-crate `.env` files from `sqlx-local-setup` take precedence over it locally. After changing any SQL, run `make sqlx-prepare` and commit the regenerated `.sqlx`, or CI will fail to build. See [`DEVELOPER.md`](DEVELOPER.md#build-with-databases).

**NEVER use `-p <crate>` (or `--package`) to scope `cargo build`/`check`/`clippy`/`nextest run` to a single crate.** Always build/check/lint the full workspace. This has been requested repeatedly — do not reintroduce `-p` scoping. Narrow *tests* with `-E 'test(...)'`, not with `-p`.

## Never discard uncommitted work

`git checkout <path>`, `git restore <path>`, `git reset --hard`, `git stash` and `git clean` destroy uncommitted changes irreversibly — there is no undo, and edits made earlier in the session are not recoverable from the transcript. **Do not run them on a file that has uncommitted changes** unless the user explicitly asked to throw those changes away.

This has caused real loss more than once. The usual trigger is using a checkout to "reset" a file after a scripted edit went wrong — which also reverts every unrelated edit already made to that file.

Instead:
- **A bad edit?** Fix it forward with `Edit`, or rewrite the file with `Write`. Both preserve everything else.
- **Need a pristine copy to compare against?** `git show HEAD:<path> > /tmp/.../orig.md` — read it without touching the working tree.
- **Recomputing line offsets after an edit?** Re-read the file; never reset it to make stale offsets valid again.
- **Genuinely need to discard?** Ask first, and say exactly which changes will be lost.

When a scripted multi-edit is involved, prefer anchored string replacement over line numbers, and assert each anchor matches before writing — line indices go stale the moment an earlier edit lands.

## Skills

Claude Code can load skills via the `Skill` tool. The same skill list from `AGENTS.md` applies — trigger them by topic, not by default.
