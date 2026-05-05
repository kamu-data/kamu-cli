---
name: kamu-release-dependency-workflows
description: Release, changelog, and Cargo dependency update workflow for Kamu CLI. Use when preparing releases, updating general Cargo dependencies, editing CHANGELOG entries, tagging versions, or coordinating release PR steps. Do not use for DataFusion stack upgrades or Jupyter demo image releases; use the dedicated Kamu skills for those workflows.
---

# Kamu Release And Dependency Workflows

Use this skill for release-scale work. Do not apply release steps to incremental local edits unless the user explicitly asks for PR or release preparation.

## Changelog

For a full PR, update `CHANGELOG.md` under `[Unreleased]`:

- `### Added` for new features.
- `### Changed` for behavior changes.
- `### Fixed` for bug fixes.
- Write an end-user-facing summary.

Do not require changelog updates for incremental IDE/local work.

## Branches

- Keep `master` stable and releasable.
- Use conventional branch prefixes such as `bug/`, `feature/`, `refactor/`, `ci/`, `docs/`, `chore/`, and `release/`.
- Maintainers merge via `git merge --ff-only`; do not rebase signed commits.

## Release Procedure

1. Start from a release branch or existing feature branch.
2. Run `cargo update` for minor releases.
3. Run `cargo upgrade --dry-run --incompatible` and decide whether to perform or ticket major upgrades.
4. Run `cargo deny check`.
5. Bump with `make release-patch`, `make release-minor`, or `make release-major`.
6. Create a dated changelog entry.
7. Create PR, wait for checks, merge normally.
8. Tag `master` with `git tag vX.Y.Z`.
9. Push with `git push origin tag vX.Y.Z`.

GitHub Actions creates the GitHub release from the tag.

## Dependency Updates

Minor updates:

```sh
cargo update
cargo deny check
```

Major updates:

- Optionally update local cargo tools first with `cargo install-update -a`.
- Run minor updates first.
- Use `cargo upgrade --dry-run --incompatible`.
- Prefer upgrading crate families coherently.
- Ticket difficult upgrades and leave a `# TODO:` comment in `Cargo.toml` when deferring.
- Run `cargo update` and `cargo deny check` after major changes.
