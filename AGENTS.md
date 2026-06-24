# AGENTS.md

Small project-specific guidance for coding agents working in this repository.

## Validation

- Run `cargo fmt` after edits.
- Run `make clippy` before considering the task finished.
- Treat Clippy warnings as errors to fix, not to ignore.

## Tests

- Prefer `cargo nextest run` over `cargo test` for targeted test execution.
- Prefer workspace-level incremental builds with test filters instead of narrowing by package when possible.
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

## Scope

- Keep this file short and repo-specific.
- Do not edit `DEVELOPER.md` for agent guidance extraction; it is the stable human developer guide.
- Keep `.github/copilot-instructions.md` usable for Copilot users that cannot load Codex skills.
