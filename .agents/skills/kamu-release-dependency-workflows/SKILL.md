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

Do not require changelog updates for incremental IDE/local work (small exploratory edits, refactoring in progress, or work-in-progress changes not yet ready for PR submission).

## Branches

- Keep `master` stable and releasable.
- Use conventional branch prefixes such as `bug/`, `feature/`, `refactor/`, `ci/`, `docs/`, `chore/`, and `release/`.
- Maintainers merge via `git merge --ff-only`; do not rebase signed commits.

## Release Procedure

### Preparation Phase

1. Start from a release branch or existing feature branch.
2. Update dependencies:
    - For **minor/patch releases**: Run `cargo update` to update within compatible version ranges.
    - For **major releases**: Run `cargo upgrade --dry-run --incompatible` and decide whether to perform or ticket major upgrades.
3. Run `cargo deny check` to verify license and security compliance.

### Version Bump Phase

4. Bump version using the appropriate command:
    - `make release-patch` for bug fixes (0.0.X)
    - `make release-minor` for new features (0.X.0)
    - `make release-major` for breaking changes (X.0.0)
5. Create a dated changelog entry documenting all changes.

### Publication Phase

6. Create PR, wait for checks, merge normally.
7. After merge, tag `master` with `git tag vX.Y.Z`.
8. Push the tag with `git push origin tag vX.Y.Z`.

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
