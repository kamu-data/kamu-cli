---
name: kamu-sqlx-database-work
description: SQLx and database workflow for Kamu CLI. Use when modifying Postgres or SQLite repositories, SQLx queries or macros, database migrations, SQLx offline cache data, DB-backed infra crates, or local database validation commands.
---

# Kamu SQLx Database Work

Use compile-time SQL checking for DB-backed repositories.

## Query Style

- Prefer `sqlx::query!`, `sqlx::query_as!`, and related SQLx macros over function-based queries.
- Do not assume Postgres is unavailable; this repo uses a local Dockerized Postgres for SQLx macro validation.
- SQLite-backed repositories have a local database setup for the same purpose.
- Keep repository layers simple; put domain-level algorithms in services unless storage-specific behavior is the actual concern.

## Local SQLx Setup

Default builds use `SQLX_OFFLINE=true`, which is fine when the task does not touch DB queries or repositories.

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

- Store migrations in `migrations/<db-engine>/`.
- Run migration commands from the database-specific crate directory, such as `src/database/sqlx-postgres`, unless `DATABASE_URL` is set manually.
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
