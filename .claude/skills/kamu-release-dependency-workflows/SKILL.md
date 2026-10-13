---
name: kamu-release-dependency-workflows
description: Release, changelog, and Cargo dependency update workflow for Kamu CLI. Use when preparing releases, updating general Cargo dependencies, choosing TLS features, editing CHANGELOG entries, tagging versions, or coordinating release PR steps. Do not use for DataFusion stack upgrades or Jupyter demo image releases; use the dedicated Kamu skills for those workflows.
---

# Kamu Release And Dependency Workflows

Use this skill for release-scale work. Do not apply release steps to incremental local edits unless
the user explicitly asks for PR or release preparation.

The procedures themselves are owned by [`DEVELOPER.md`](../../../DEVELOPER.md) — follow the section
there step by step; this skill only adds what an agent must watch for. If the two disagree,
`DEVELOPER.md` wins and this skill gets fixed.

| Task | Procedure |
|---|---|
| Branch naming, merge policy | [Feature Branches](../../../DEVELOPER.md#feature-branches) |
| Cutting a release | [Release Procedure](../../../DEVELOPER.md#release-procedure) |
| `cargo update` | [Minor Dependencies Update](../../../DEVELOPER.md#minor-dependencies-update) |
| `cargo upgrade --incompatible` | [Major Dependencies Update](../../../DEVELOPER.md#major-dependencies-update) |

## Changelog

`CHANGELOG.md` follows Keep a Changelog. Entries go under `## [Unreleased]`:

- `### Added` for new features, `### Changed` for behaviour changes, `### Fixed` for bug fixes.
- Write for end users: what changed for them, not how it was implemented.
- One consolidated entry per feature, written at finalization — never per slice. Do not add or
  request entries for in-progress work (see AGENTS.md, "Changelog review context").
- A change is "breaking" only if the previous form was released; check with `git show <tag>:path`.
- The release workflow builds GitHub release notes from the version's section, so a release
  without its dated section publishes empty notes.

## Agent checklist

- Every commit, tag and push in these procedures needs explicit user approval for that step
  (AGENTS.md, "Hard rules"). Prepare the change, report, and stop.
- `cargo update -p <crate>` / `cargo upgrade -p <crate>@<ver>` are package specs — the build-scope
  rule about `-p` does not apply to them.
- After any dependency change run `cargo deny check`; duplicate major versions of Arrow,
  DataFusion, SQLx, Tokio or dill are denied by `deny.toml`. CI also runs `cargo udeps`, so a
  dependency left unused fails it.
- When deferring a hard major upgrade, leave a `# TODO:` comment in `Cargo.toml` and tell the user
  so they can ticket it.
- `make release-*` rewrites version strings in generated `resources/` files itself; do not edit
  them by hand.

## TLS

- TLS is configured only by top-level applications — the "Top-level TLS configuration" block in
  [`src/app/cli/Cargo.toml`](../../../src/app/cli/Cargo.toml). Library crates never enable a TLS
  feature (`rustls-tls*`, `native-tls`, `*-rustls-*`, root-certificate selection); they take the
  dependency with `default-features = false` and only the non-TLS features they need (e.g. alloy
  `reqwest`, not `reqwest-rustls-tls`).
- The stack is `rustls` with the `aws-lc-rs` crypto provider and webpki roots. Do not add
  `native-tls`/OpenSSL or the `ring` provider.
- Tests that need TLS rely on workspace feature unification with the application crate, so they
  work in workspace builds, not when a library is built alone with `-p`.

## What lives elsewhere

- DataFusion / Arrow / Parquet family upgrades: `kamu-datafusion-upgrade-workflows`.
- Jupyter demo images: `kamu-jupyter-demo-release-workflows`.
- Changelog prose style: `kamu-prose-and-comments`.
