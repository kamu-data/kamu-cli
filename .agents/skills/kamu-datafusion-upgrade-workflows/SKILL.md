---
name: kamu-datafusion-upgrade-workflows
description: DataFusion stack upgrade workflow for Kamu CLI. Use when upgrading DataFusion, Arrow, object_store, parquet, arrow-digest, datafusion-odata, datafusion-ethers, datafusion-functions-json, or updating the DataFusion SQL shell.
---

# Kamu DataFusion Upgrade Workflows

Use this skill for DataFusion-family dependency upgrades, which include: `datafusion`, `arrow`, `object_store`, `parquet`, `arrow-digest`, `datafusion-odata`, `datafusion-ethers`, and `datafusion-functions-json`. Keep these upgrades separate from routine Cargo dependency updates because compatible crate versions and downstream Kamu crates must move together.

## Upgrade Flow

### 1. Verify Compatible Versions

- Check the upstream DataFusion repository for compatible `arrow` and `object_store` versions in `Cargo.toml`.
- If no compatible versions are available for required dependencies, document the incompatibility, wait for upstream releases, or consider opening an issue with the DataFusion maintainers.

### 2. Update Downstream Kamu Crates

- Upgrade `arrow-digest` to the new Arrow version if needed, then publish it before consuming it here.
- Upgrade and publish `datafusion-odata` and `datafusion-ethers` for the new DataFusion version.
- Check that `datafusion-functions-json` has a compatible release.

### 3. Perform Breaking Update

- Use a dry-run breaking update command before committing lockfile changes (see Typical Dry Run section below).
- Ensure `Cargo.lock` does not contain duplicate major versions of Arrow, DataFusion, Object Store, or Parquet crates.

### 4. Update SQL Shell

- Follow `src/utils/datafusion-cli/README.md` to update the DataFusion SQL shell.

### 5. Validate and Fix

- Fix compilation errors and warnings.
- Run the repo validation commands required by `AGENTS.md` (`cargo fmt` and `make clippy`).

## Typical Dry Run

Adjust the package list and versions to match the target DataFusion release:

```sh
cargo -Z unstable-options update --breaking \
  -p arrow \
  -p arrow-digest \
  -p arrow-flight \
  -p arrow-json \
  -p arrow-schema \
  -p object_store \
  -p datafusion \
  -p datafusion-common \
  -p datafusion-functions-json \
  -p datafusion-odata \
  -p datafusion-ethers \
  -p parquet \
  --dry-run
```

## Validation

- Run `cargo deny check --hide-inclusion-graph` when checking for duplicate major versions.
- Run targeted tests for affected query, transform, and DataFusion CLI paths when practical.
- Finish with `cargo fmt` and `make clippy`.
