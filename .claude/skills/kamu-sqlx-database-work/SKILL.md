---
name: kamu-sqlx-database-work
description: SQLx and database workflow for Kamu CLI. Use when modifying Postgres or SQLite repositories, SQLx queries or macros, database migrations, SQLx offline cache data, DB-backed infra crates, or local database validation commands.
---

# Kamu SQLx Database Work

Use compile-time SQL checking for DB-backed repositories.

## Query Style

### Basic Rules

1. **Prefer SQLx macros**: Use `sqlx::query!`, `sqlx::query_as!`, and related macros over function-based queries for compile-time checking.
2. **Local DB validation is available**: Do not assume Postgres or SQLite are unavailable — this repo uses local Dockerized databases for SQLx macro validation.
3. **Keep repositories storage-focused**: Put domain-level algorithms in services unless storage-specific behavior is the actual concern.

### Row Structs

4. **Declare explicit row structs**: Implement `sqlx::FromRow` for query results instead of using name-based dynamic column resolutions.
5. **Use `query_as!` with structs**: Ensure compile-time column verification and avoid runtime column name lookups.
6. **Share structs across databases when possible**: When row structs can be shared across Postgres/SQLite, place them in a domain crate where repository traits are defined, and use `cfg_attr` to conditionally derive `sqlx::FromRow`:

```rust
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MyEntityRow {
    pub entity_id: Uuid,
    pub key: String,
    pub value: Vec<u8>,
}

```

### Avoiding dynamic QueryBuilder in Postgres

For Postgres queries that filter on a set of composite keys (e.g. `(id, kind)` pairs), prefer static `UNNEST`-based queries over dynamically built `OR` chains:

```sql
-- Prefer this:
WHERE (resource_id, resource_kind) IN (
    SELECT * FROM UNNEST($1::uuid[], $2::text[])
)

-- Over a dynamically built:
-- WHERE (resource_id = $1 AND resource_kind = $2) OR (resource_id = $3 AND resource_kind = $4) ...
```

Extract the arrays before entering any `async_stream::stream!` block so they are captured cleanly:

```rust
let ids: Vec<uuid::Uuid> = queries.iter().map(|q| *q.id.as_ref()).collect();
let kinds: Vec<String> = queries.iter().map(|q| q.kind.clone()).collect();
```

### SQLite and dynamic QueryBuilder

SQLite does not have `UNNEST` or a type-safe array unpacking equivalent. Using `json_each()` is fragile because SQLite stores UUIDs as blobs while `json_each` returns text, causing type mismatch in comparisons. For multi-key filtering in SQLite, keep the dynamic `QueryBuilder` with `OR` chains — it is the appropriate approach.

Dynamic `QueryBuilder` is also necessary in SQLite for variable-row bulk `INSERT ... VALUES` (e.g. chunked inserts). Do not try to eliminate this usage.

## Local SQLx Setup

The repo-wide default is offline mode from the committed `.sqlx` cache, which is fine when the task does not touch DB queries or repositories. Never override `SQLX_OFFLINE` from the shell (AGENTS.md, "Hard rules").

When modifying DB-backed repositories, adding DB infra crates, or changing migrations:

```sh
make sqlx-local-setup
```

This starts local DB containers, applies migrations, and writes crate-local `.env` files with `DATABASE_URL` and SQLx offline disabled.

After SQL or schema changes:

```sh
make sqlx-prepare
```

Commit updated `.sqlx` offline data when it changes.

When finished with local DB containers:

```sh
make sqlx-local-clean
```

## Migrations

- Store migrations in `migrations/<db-engine>/`. They are forward-only single files — never write down migrations.
- `make sqlx-add-migration NAME=<name>` creates the Postgres and SQLite files together.
- Run migration commands from a database-specific crate directory with a `.env` from `make sqlx-local-setup`, such as `src/infra/accounts/postgres` (all of them: `POSTGRES_CRATES` / `SQLITE_CRATES` in the `Makefile`), unless `DATABASE_URL` is set manually.
- Typical commands:

```sh
sqlx migrate add --source <migrations_dir_path> <description>
sqlx migrate run --source <migrations_dir_path>
sqlx migrate info --source <migrations_dir_path>
```

## Validation

- `make lint` includes SQLx cache validation through `make lint-sqlx`.
- If modifying SQLx queries, run `make sqlx-prepare` before final validation.
- If sandboxing blocks DB access, rerun the relevant command with the needed permissions instead of changing the workflow.

## Rejected approaches

Do not re-propose these without new evidence.

| Approach | Why it was rejected |
|---|---|
| Dynamic `OR` chains via `QueryBuilder` in Postgres | Not checked at compile time by `query!`; the static `UNNEST` form is. |
| `json_each()` to emulate `UNNEST` in SQLite | SQLite stores UUIDs as blobs, `json_each` yields text; comparisons silently mismatch. |
| Down (reversible) migrations | Migrations are forward-only (AGENTS.md). |
| Forcing `SQLX_OFFLINE` from the shell | Checks queries against a stale cache and hides schema drift (AGENTS.md, "Hard rules"). |

## What lives elsewhere

- Repository trait test suites and storage harnesses: `kamu-repository-tests`.
- Cross-context references (no foreign keys) and repository naming: `kamu-domain-design`.
- `LISTEN`/`NOTIFY` triggers in migrations: [`docs/internal/wakeup-listeners.md`](../../../docs/internal/wakeup-listeners.md).
