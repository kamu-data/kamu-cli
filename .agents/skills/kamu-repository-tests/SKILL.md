---
name: kamu-repository-tests
description: Storage-backed repository test-suite workflow for Kamu CLI. Use when adding or changing repository trait tests, repo-tests crates, in-memory/Postgres/SQLite repository test wiring, Catalog harnesses, or database_transactional_test! macro usage.
---

# Kamu Repository Tests

Use the shared three-layer pattern when adding tests for a storage-backed repository trait.

## Repo-Tests Crate

Create a plain library crate under `src/infra/<domain>/repo-tests/`.

- Add one `*_test_suite.rs` file per repository trait.
- Export `pub async fn test_*(catalog: &Catalog)` functions.
- Resolve the trait only through the public catalog API:
  `catalog.get_one::<dyn MyRepo>().unwrap()`.
- Test only the public repository interface; do not depend on implementation internals.
- In `lib.rs`, declare each suite module and re-export with `pub use`.
- Keep the crate free of dev-dependencies.
- Add the crate to root `[workspace]` members and `[workspace.dependencies]`.
- Depend on the domain crate, `database-common`, supporting utility crates, and test data helpers needed by the suite.

## Implementation Crate Wiring

For each storage implementation crate (`inmem/`, `postgres/`, `sqlite/`):

- Add a `tests/` tree with `mod.rs`, `repos/mod.rs`, and one `test_<storage>_<repo>.rs` file.
- Use `database_transactional_test!` once per suite function.
- Wire the macro to a local harness struct and keep the file otherwise minimal.
- Add `database-common-macros` and the new `kamu-<domain>-repo-tests` crate to `[dev-dependencies]`.

Harness shape:

- Include `catalog: Catalog`.
- Provide `new()` for in-memory storage.
- Provide `new(pool)` for DB-backed storage.
- Register the concrete repository implementation and transaction manager into `CatalogBuilder`.

## Macro Usage

Use `database_common_macros::database_transactional_test`.

- `storage = inmem` emits a plain tokio test.
- `storage = postgres` and `storage = sqlite` emit an `sqlx::test`, receive a pool, run migrations, and wrap the fixture in a transaction runner.

Example:

```rust
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_<domain>_inmem::InMemory<Repo>;

database_transactional_test!(
    storage = inmem,
    fixture = kamu_<domain>_repo_tests::test_something,
    harness = InMemory<Repo>Harness
);

struct InMemory<Repo>Harness {
    catalog: Catalog,
}

impl InMemory<Repo>Harness {
    pub fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<InMemory<Repo>>();
        Self { catalog: b.build() }
    }
}
```
