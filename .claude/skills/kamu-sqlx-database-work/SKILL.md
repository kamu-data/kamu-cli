---
name: kamu-sqlx-database-work
description: SQLx and database workflow for Kamu CLI. Use when modifying Postgres or SQLite repositories, event stores and their concurrent-modification checks, SQLx queries or macros, database migrations, SQLx offline cache data, DB-backed infra crates, or local database validation commands.
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

### SQLite bulk writes: one JSON parameter

SQLite has no `UNNEST`. For multi-row `INSERT`/`DELETE` over string and integer columns, pass the
rows as one JSON array and unpack it with `json_each($1)` inside `sqlx::query!`, so the statement
is checked at compile time and has no bound-parameter limit:

```sql
INSERT INTO flow_events (flow_id, event_time, event_type, event_payload)
SELECT value ->> 'flow_id', value ->> 'event_time', value ->> 'event_type', value ->> 'event_payload'
FROM json_each($1)
ORDER BY key
```

- Name the JSON keys after the columns; `query!` does not check them, NOT NULL constraints and
  the repository tests do.
- `ORDER BY key` when IDs are assigned on insert, so they follow the batch order.
- Timestamps: format with `database_common::sqlite_datetime_text`, the text `sqlx` itself stores.
- JSON payload columns: embed the serialized text as a string and read it with `->>`; a nested
  JSON value is re-rendered by SQLite.
- Blob columns: hex-encode and wrap in `unhex(...)`.
- UUID columns are excluded (see "Rejected approaches"): keep the dynamic `QueryBuilder` there.

Multi-key reads (`WHERE ... IN (...)` lists, `OR` chains) still use the dynamic `QueryBuilder` or
placeholder helpers; with UUID keys they must.

The JSON form is slower than `QueryBuilder`: at 1000+ rows about 1.2–1.4× for text rows and 3×
with blob payloads; at a few rows the gap is tens to hundreds of microseconds (up to 2× with
blobs). SQLite serves single-tenant CLI and edge deployments where batches stay small, so
compile-time checking wins there.

## Concurrent Modifications in Event Stores

`save_events(query, expected_last_event_id, events)` must fail with a concurrent modification when
another transaction appended to the same aggregate after the caller loaded it. Two transactions in
flight at once are the case to design for: under Postgres `READ COMMITTED` neither sees the other's
uncommitted rows, so any check that only reads passes in both.

The detection must make the second writer **wait** for the first and then fail. Only a row lock
or a unique index entry does that. Use optimistic patterns only:

| Aggregate shape | Pattern | Examples |
|---|---|---|
| Has its own row (surrogate ID) | Single-statement compare-and-set: `UPDATE … SET last_event_id = $new WHERE id = $1 AND last_event_id IS NOT DISTINCT FROM $expected`; zero rows means concurrent modification | `flows`, `tasks`, `webhook_subscriptions`, `resources` |
| Events only, keyed by a natural key | `prev_event_id` on the events table: the first event of each saved batch stores the expected ID (0 for none), behind a partial unique index on (key, `prev_event_id`); map the unique violation to concurrent modification | flow triggers and configurations, account quotas |
| Rows replaced per version | Unique constraint on (aggregate, version); map the unique violation | configuration variable and secret set projections |

Rules that the patterns rely on:

- **The compare must be in the clause Postgres re-checks.** A writer blocked on a row lock
  re-evaluates the `UPDATE`'s own `WHERE` against the committed row, never a CTE or subquery,
  which keep the statement snapshot. A compare-and-set inside a CTE only is a lost update.
- **Never use the highest event ID as the concurrency check.** IDs come from a sequence and commit
  out of order across transactions. Compare the exact expected ID, and let the row or index
  serialize writers. A `SELECT MAX(event_id)` before the insert may stay as input validation (an expected ID
  that was never last), never as the concurrency guard.
- **Adding `prev_event_id` to an existing table needs a backfill** that chains each old event to its
  predecessor (`LAG(...) OVER (PARTITION BY key ORDER BY id)`, 0 for the first), so the index also
  covers writers based on old events.
- **Map only the unique violation** (`as_database_error()` + `is_unique_violation()`); other errors
  stay internal.
- **Prove it with a race test** on Postgres: two `TransactionRefT`s on one pool, the first saves,
  the second save is spawned, the first commits after a short sleep, and the second must return a
  concurrent modification. Examples:
  `src/infra/flow-system/postgres/tests/tests/test_postgres_flow_binding_concurrent_writes.rs`,
  `src/infra/resources/postgres/tests/repos/test_postgres_resource_repository_concurrent_updates.rs`.
  SQLite runs one connection per process, so the same test cannot race there.

## Local SQLx Setup

The repo-wide default is offline mode from the committed `.sqlx` cache, which is fine when the task does not touch DB queries or repositories. Never override `SQLX_OFFLINE` from the shell (AGENTS.md, "Hard rules").

When modifying DB-backed repositories, adding DB infra crates, or changing migrations:

```sh
make sqlx-local-setup
```

This starts local DB containers, applies migrations, and writes crate-local `.env` files with a `DATABASE_URL`, which switches those crates to live checking.

After SQL changes, prepare only the crates whose queries changed:

```sh
(cd src/infra/<domain>/sqlite && cargo sqlx prepare)
```

After schema changes, or when unsure which crates are affected, prepare all of them with
`make sqlx-prepare` (or `make sqlx-prepare-postgres` / `make sqlx-prepare-sqlite`).

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

`database-common` embeds the migrations with `sqlx::migrate!`, which does not notice a new file.
After adding one, touch `src/utils/database-common/src/plugins/{postgres,sqlite}_plugin.rs` before
running e2e tests, or a stale binary fails on the missing schema; if the SQLite migrator still runs
the old set, do a clean build.

Before committing the migration run `make resources-db-schema` to update the final schema snapshots in `/resources/db` directory.

## Validation

- `make lint` includes SQLx cache validation through `make lint-sqlx`.
- If modifying SQLx queries, prepare the affected crates (see "Local SQLx Setup") before final validation.
- If sandboxing blocks DB access, rerun the relevant command with the needed permissions instead of changing the workflow.

## Rejected approaches

Do not re-propose these without new evidence.

| Approach | Why it was rejected |
|---|---|
| Dynamic `OR` chains via `QueryBuilder` in Postgres | Not checked at compile time by `query!`; the static `UNNEST` form is. |
| `json_each()` over UUID columns in SQLite | `sqlx` binds `uuid::Uuid` as a blob, `json_each` yields text; comparisons silently mismatch. |
| Per-row `query!` in a loop for SQLite bulk writes | Measured 4–5× slower than one multi-row statement: each `execute` is a round-trip to the SQLite worker thread. |
| Down (reversible) migrations | Migrations are forward-only (AGENTS.md). |
| Forcing `SQLX_OFFLINE` from the shell | Checks queries against a stale cache and hides schema drift (AGENTS.md, "Hard rules"). |
| Advisory locks (`pg_advisory_xact_lock`) to serialize writers of an aggregate | Not used in this system; concurrency detection is optimistic only. |
| A separate per-aggregate table only to hold `last_event_id` for an events-only aggregate | Adds a table per aggregate; `prev_event_id` with a unique index on the events table does the same job. |
| `SELECT MAX(event_id)` before or after the insert as the only check | Two in-flight transactions both pass: neither sees the other's uncommitted rows. |
| Compare-and-set on `last_event_id` inside a CTE | The CTE keeps the statement snapshot; a writer blocked on the row lock re-checks only the outer `WHERE` and overwrites the other update. |
| `pg_visible_in_snapshot(tx_id, ...)` as the guard of a `(tx_id, id)` delivery cursor | Delivers a newer transaction while an older one is in flight, so the cursor passes the older ID and its rows are skipped forever; read below `pg_snapshot_xmin(pg_current_snapshot())` ([outbox.md](../../../docs/internal/outbox.md#reading-below-the-oldest-running-transaction)). |

## What lives elsewhere

- Repository trait test suites and storage harnesses: `kamu-repository-tests`.
- Cross-context references (no foreign keys) and repository naming: `kamu-domain-design`.
- `LISTEN`/`NOTIFY` triggers in migrations: [`docs/internal/wakeup-listeners.md`](../../../docs/internal/wakeup-listeners.md).
