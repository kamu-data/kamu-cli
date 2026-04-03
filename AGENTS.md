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

### Repository test suites

Storage-backed repository traits follow a shared three-layer pattern. When adding tests for a new repository trait, replicate this structure:

1. **`repo-tests` crate** (`src/infra/<domain>/repo-tests/`) — a plain library crate with no dev-dependencies. Contains one `*_test_suite.rs` file per repository trait, exporting `pub async fn test_*(catalog: &Catalog)` functions. Each function resolves the trait from the catalog via `catalog.get_one::<dyn MyRepo>().unwrap()` and calls only the public interface. `lib.rs` declares each suite file as a `mod` and re-exports everything with `pub use`. Dependencies are the domain crate, `database-common`, supporting utility crates, and test data helpers (e.g., `uuid`, `serde_json`, `chrono`). Add the crate to `[workspace]` members and `[workspace.dependencies]` in the root `Cargo.toml`.

2. **Test wiring in each implementation crate** (`inmem/`, `postgres/`, `sqlite/`) — a `tests/` directory with `mod.rs`, `repos/mod.rs`, and one `test_<storage>_<repo>.rs` file. That file uses the `database_transactional_test!` macro once per suite function, wires it to a local *harness* struct, and nothing else. The harness has a `catalog: Catalog` field and a `new()` constructor (plus `new(pool)` for DB-backed storages) that registers the concrete implementation and transaction manager into the catalog. Add `database-common-macros` and `kamu-<domain>-repo-tests` to `[dev-dependencies]` of each implementation crate.

3. **`database_transactional_test!` macro** from `database-common-macros` generates the `#[test]` boilerplate. `storage = inmem` emits a plain tokio test; `storage = postgres` / `storage = sqlite` emits an `sqlx::test` that receives a pool, runs migrations, and wraps the fixture in a transaction runner.

Example skeleton for an `inmem` test file:
```rust
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_<domain>_inmem::InMemory<Repo>;

database_transactional_test!(
    storage = inmem,
    fixture = kamu_<domain>_repo_tests::test_something,
    harness = InMemory<Repo>Harness
);

struct InMemory<Repo>Harness { catalog: Catalog }
impl InMemory<Repo>Harness {
    pub fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<InMemory<Repo>>();
        Self { catalog: b.build() }
    }
}
```


## Postgres / sqlx

- Do not assume Postgres is unavailable.
- This repo relies local Dockerized Postgres instance for `sqlx` macro validation and related checks.
- Similarly, there is a local SQLite database created for similar purposes.
- In Postgres/SQLite repositories, prefer `sqlx::query!`, `sqlx::query_as!`, and similar macros over function-based queries for compile-time SQL checking.
- When adding new DB-backed infra crates to the Makefile SQLx crate lists, or when adding new migrations, run `make sqlx-local-setup` to recreate the local databases, rerun migrations, and refresh `.env` bindings for all affected crates.
- When modifying SQLx queries, run `make sqlx-prepare`.
- `make lint` includes SQLx cache validation via `make lint-sqlx`.
- If a sandbox blocks DB access, rerun the relevant command with the needed permissions rather than changing the workflow.


## Style

- Follow existing Rust style and naming in surrounding code.
- Prefer inline formatting like `format!("value={value}")`.
- Prefer checked numeric conversions like `usize::try_from(x).unwrap()` when narrowing types.
- Respect exact long separator comment style where surrounding files use it.
- Name lookup methods consistently: `get_xxx` returns the thing or a not-found error, while `find_xxx` returns `Option<T>` and treats absence as non-error.

## GraphQL

- GraphQL schema is code-first in `kamu-adapter-graphql`; never edit `resources/schema.gql` by hand.
- Add new API surface through grouped root objects in `src/adapter/graphql/src/root.rs`, with query and mutation modules following the existing crate structure.
- For schema-only prototypes explicitly requested by the user, resolver bodies may be left as `todo!()`.
- Regenerate the schema with the existing test: `cargo nextest run -E 'test(update_graphql_schema)`.


## Design Notes

### Outbox

- For outbox events that leave the bounded context, prefer snapshot-style payloads over incremental deltas.
- In such resource change detection flows, optimize detection around the emission gate 
(`did effective state change?`), then re-query current state for the outgoing message.
- In tests with `MockOutbox`, prefer hiding message expectation setup in harness/helper methods instead of inline closures in each test.

### Repositories
- Keep repository layers simple when possible; prefer domain/service-level complex algorithms rather than repositories unless storage-specific behavior is the actual concern.

### Error Handling
- Prefer operation-specific error types and keep impossible variants out of operation-specific APIs when practical.
- When translation is structural only, implement `From` near the error type definition and keep service/use-case code at a higher level.
- Reuse shared domain error types for repeated concepts such as resource not found, type mismatch, and load failures.
- Do not use catch-all match arms for error conversion; list variants explicitly.
- Prefer plain `InternalError` for generic infrastructure/read failures; only introduce named wrappers when the distinction is meaningful at the boundary.
- Prefer `.int_err()` plus `.with_context(...)` over ad hoc `InternalError::new(format!(...))` when converting errors.


## Scope

- Keep new guidance here short and repo-specific.
- Use `.github/copilot-instructions.md` as supporting context, but prefer this file for the highest-signal local workflow notes.
