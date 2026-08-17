# AGENTS.md

Small project-specific guidance for coding agents working in this repository.

## Git

- **Never commit without explicit approval.** Do not run `git commit`, `git push`,
  `git merge`, `git rebase`, or any other history-altering command unless the user
  asks for it in that specific instance. Leave finished work staged or unstaged in
  the working tree and let the user review and commit it themselves.
- Approval to commit once is not standing approval for later commits; ask again.

## Validation

- Run `cargo fmt` after edits.
- Run `make clippy` before considering the task finished.
- Treat Clippy warnings as errors to fix, not to ignore.
- Prefer full workspace incremental commands over narrowing by package with `-p`;
  this workspace is usually precompiled and package narrowing is often slower for
  builds, tests, and validation commands.

### Migration Review Context

Do not assume edited migrations are already applied externally just because they exist in git history.

Default assumption for uncommitted migration edits:
- Treat them as branch-local and not yet released, shared, or applied externally. Review their
  contents for correctness only.
- Do not flag checksum drift or “already migrated database” risk based only on the migration timestamp or prior commits.
- Reconsider external-application risk only when the user says the migration was released, shared,
  or applied, or when something in the workspace shows it was. If that status is genuinely unclear
  and would change your answer, ask — do not turn the uncertainty into a review finding.

### Changelog Review Context

Do not require or suggest `CHANGELOG.md` updates while reviewing uncommitted work on an
in-progress feature branch, regardless of branch size. The changelog is intentionally
consolidated into one compact entry when the feature is finalized, so per-slice entries would
have to be merged back together.

- Check for a changelog entry only when the user explicitly asks for PR finalization, release
  preparation, or changelog work.
- This applies to breaking API changes too. A schema or interface change is only "breaking" for
  consumers if it was previously released — verify against the release tags (`git show
  <tag>:path`) before treating it as one.

## Tests

- Prefer `cargo nextest run` over `cargo test` for targeted test execution.
- Prefer workspace-level incremental runs with test filters instead of narrowing by
  package.
- Typical pattern:

```bash
cargo nextest run -E 'test(test_name_here)'
```
- Use `assert_matches!(expr, pattern)` directly — never wrap it as `assert!(matches!(expr, pattern))`. Either `std::assert_matches` or `pretty_assertions::assert_matches` is acceptable; use whichever is already imported in the file.

## Style

- Follow existing Rust style and naming in surrounding code.
- Prefer inline formatting like `format!("value={value}")`.
- Prefer checked numeric conversions like `usize::try_from(x).unwrap()` when narrowing types.
- Respect exact long separator comment style where surrounding files use it.
- Keep comments concise — one or two lines, never prose poems.
- Never explain what the code plainly says. If a reader can see it, do not restate it.
- Comment the WHY: a constraint, a non-obvious invariant, a reason a choice was made. Not the how, and not the history of how the code got here ("used to be X", "after removing Y") — that belongs in commit messages, not in the source.
- Never cite ephemeral plan/spec references or external issue identifiers (e.g. "plan 7 item 3", "see JIRA-123") in code comments — they are not part of the codebase and the reference rots.
- Stable test-slice identifiers defined by a version-controlled coverage map in the same repo (e.g. `RF-*`, indexed by `contract/COVERAGE.md`) are allowed in test code and comments: the referent is checked in beside the test, so it cannot rot without the map rotting too. Keep the map in sync when adding or renumbering.
- Keep macros declarative. Put algorithmic logic into ordinary helper functions or services.
- When logic becomes conceptually distinct, split it into its own module early.
- Organize model files top-down: highest-level result/union type first, then referenced structs/enums, with impl blocks immediately after the type.
- Group repeated logical sections inside functions into named helpers when they represent a coherent concept.
- Keep visibility tight by default. Use `pub(crate)` or private helpers unless a real boundary requires wider visibility.
- Do not publicly re-export internal helper modules unless external consumers or macro expansion truly require it.

## Specialized Skills

Repo-local skills live in `.agents/skills/`. Load them only when the task matches their trigger:

- `.agents/skills/kamu-dill-di`: defining dill components, interfaces, scopes, catalog building and chaining.
- `.agents/skills/kamu-test-harness`: test harness structs, per-account catalog wiring, in-memory test doubles, singleton scope.
- `.agents/skills/kamu-cli-e2e-tests`: CLI black-box e2e tests under `src/e2e/app/cli` — shared `repo-tests` scenario bodies, `execute_command` vs `run_api_server` harness modes, per-DB wiring macros, local↔remote context pattern, `KamuCliPuppet`.
- `.agents/skills/kamu-repository-tests`: storage-backed repository trait test suites, `repo-tests` crates, storage harnesses, `database_transactional_test!`.
- `.agents/skills/kamu-sqlx-database-work`: Postgres/SQLite repositories, SQLx macros, migrations, SQLx offline data, local DB validation.
- `.agents/skills/kamu-graphql-api`: GraphQL queries, mutations, roots, resolvers, models, enum mappings, schema regeneration.
- `.agents/skills/kamu-domain-design`: outbox, repositories, domain/view construction, event modeling, operation-specific errors.
- `.agents/skills/kamu-release-dependency-workflows`: changelog, release, and general Cargo dependency update workflows.
- `.agents/skills/kamu-datafusion-upgrade-workflows`: DataFusion, Arrow, Object Store, Parquet, and related query-engine dependency upgrades.
- `.agents/skills/kamu-jupyter-demo-release-workflows`: Jupyter demo, rustfs, and multi-platform demo image release workflows.

Reusable sub-agent role descriptions for Rust build/test delegation live in
`.claude/agents/rust-builder.md` and `.claude/agents/rust-tester.md`; treat
those files as the canonical role prompts instead of duplicating them elsewhere.

## Scope

- Keep this file short and repo-specific.
- Do not edit `DEVELOPER.md` for agent guidance extraction; it is the stable human developer guide.
- Keep `.github/copilot-instructions.md` usable for Copilot users that cannot load Codex skills.
