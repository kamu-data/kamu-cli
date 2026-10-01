---
name: kamu-datafusion-upgrade-workflows
description: DataFusion stack upgrade workflow for Kamu CLI. Use when upgrading DataFusion, Arrow, object_store, parquet, arrow-digest, datafusion-odata, datafusion-ethers, datafusion-functions-json, or updating the DataFusion SQL shell.
---

# Kamu DataFusion Upgrade Workflows

DataFusion-family crates — `datafusion`, `arrow`, `object_store`, `parquet`, `arrow-digest`,
`datafusion-odata`, `datafusion-ethers`, `datafusion-functions-json` — move together, so keep these
upgrades separate from routine Cargo dependency updates.

The procedure is owned by
[`DEVELOPER.md` — Upgrading Datafusion stack](../../../DEVELOPER.md#upgrading-datafusion-stack),
including the `cargo -Z unstable-options update --breaking ... --dry-run` command. Follow it step
by step; if it and this skill disagree, `DEVELOPER.md` wins and this skill gets fixed.

## Agent checklist

1. **Versions first.** Read the target DataFusion tag's `Cargo.toml` for its `arrow` and
   `object_store` versions. If a required dependency has no compatible release, stop and report —
   do not pin around it.
2. **Our upstream crates.** `arrow-digest` (versioned in lockstep with Arrow), `datafusion-odata`
   and `datafusion-ethers` live in other repositories and must be published before this repo can
   consume them. Check crates.io; if a version is missing, report it to the user instead of
   patching with git dependencies.
3. **Dry run before writing.** Run the breaking update with `--dry-run`, show the user the planned
   version moves, then run it for real. `-p` here is a package spec, not build scoping.
4. **No duplicate majors.** `cargo deny check --hide-inclusion-graph` must report no second
   version of Arrow, DataFusion, Object Store or Parquet.
5. **SQL shell.** Follow `src/utils/datafusion-cli/README.md`. The crate is copied from upstream
   under Apache 2.0, so it is exempt from the BSL license-header lint.
6. **Fix forward.** Fix compilation errors without `#[allow]`/`#[expect]`, then run targeted
   tests for query, transform and SQL-shell paths, then `cargo fmt` and `make clippy`.

## What lives elsewhere

- General dependency updates and releases: `kamu-release-dependency-workflows`.
