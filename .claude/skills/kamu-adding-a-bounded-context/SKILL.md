---
name: kamu-adding-a-bounded-context
description: Ordered checklist for adding a new bounded context (domain) to Kamu CLI — domain and services crates, in-memory/Postgres/SQLite repositories, repo-tests, migrations, SQLx cache, dill wiring in the CLI app, outbox producers/consumers, config, GraphQL. Use when creating a new domain area or a new set of repository crates; follow it in order.
---

# Adding A Bounded Context

`webhooks` is the reference: it is small and has every layer. `configuration` and `resources` are
larger examples of the same shape. Copy structure from them rather than inventing it.

Throughout, `<ctx>` is the context name (`webhooks`) and package names follow
`kamu-<ctx>`, `kamu-<ctx>-services`, `kamu-<ctx>-{inmem,postgres,sqlite,repo-tests}`.

## The checklist

Each step names the skill to load and what proves the step is done.

| # | Step | Files | Load | Proven by |
|---|---|---|---|---|
| 1 | **Domain crate** `kamu-<ctx>`: entities, aggregates, repository traits, messages, use-case traits, errors. Features: `sqlx = ["dep:sqlx"]` for `sqlx::Type`/`FromRow` derives, `testing = ["dep:mockall"]` for `automock` | `src/domain/<ctx>/domain/` | `kamu-domain-design` | `cargo build` |
| 2 | **Services crate** `kamu-<ctx>-services`: implementations, message consumers, `pub use kamu_<ctx> as domain;`, and `dependencies.rs` with `pub fn register_dependencies(catalog_builder: &mut CatalogBuilder)` | `src/domain/<ctx>/services/` | `kamu-dill-di` | `cargo build` |
| 3 | **Workspace registration**: add each crate to `[workspace] members` and `[workspace.dependencies]` (`version` = current workspace version, `path`, `default-features = false`) | root `Cargo.toml` | — | `cargo build` |
| 4 | **Repo-tests crate**: storage-agnostic suites, `pub async fn test_x(catalog: &dill::Catalog)`; no dev-dependencies | `src/infra/<ctx>/repo-tests/` | `kamu-repository-tests` | compiles |
| 5 | **In-memory repositories** (test-only, no transaction modelling) + `tests/` wiring with `database_transactional_test!(storage = inmem, ...)` | `src/infra/<ctx>/inmem/` | `kamu-repository-tests` | inmem suite green |
| 6 | **Migrations** for both engines: forward-only, one file each, `YYYYMMDDHHMMSS_<description>.sql` via `make sqlx-add-migration NAME=<ctx>_<what>`. No foreign keys into other contexts | `migrations/{postgres,sqlite}/` | `kamu-sqlx-database-work` | `make sqlx-local-setup` applies them |
| 7 | **Postgres and SQLite repositories** with `query!`/`query_as!`; add both crate dirs to `POSTGRES_CRATES` / `SQLITE_CRATES` in the `Makefile` (that list drives `.env` setup, `sqlx-prepare` and `lint-sqlx`) | `src/infra/<ctx>/{postgres,sqlite}/`, `Makefile` | `kamu-sqlx-database-work` | `make sqlx-local-setup` (re-run, for the new `.env`), `make sqlx-prepare` writes `<crate>/.sqlx/` |
| 8 | **Repository tests** per engine: `tests/kamu_<ctx>_<engine>_tests.rs` + `tests/repos/test_<engine>_<repo>.rs` with `database_transactional_test!(storage = postgres \| sqlite, ...)` | `src/infra/<ctx>/{postgres,sqlite}/tests/` | `kamu-repository-tests` | all three engines green |
| 9 | **Repository wiring** in all three branches: `DatabaseProvider::Postgres`, `DatabaseProvider::Sqlite`, and `configure_in_memory_components` | `src/app/cli/src/database.rs` | `kamu-dill-di` | `cargo build` |
| 10 | **Service wiring**: call `kamu_<ctx>_services::register_dependencies(&mut b)` in `configure_base_catalog` (CLI and server) or `configure_server_catalog` (server only); add the crates to `src/app/cli/Cargo.toml` | `src/app/cli/src/app.rs` | `kamu-dill-di` | `cargo nextest run -E 'test(test_di_graph)'`, which also regenerates `resources/di.puml` |
| 11 | **Outbox**: producer/consumer name constants `dev.kamu.domain.<ctx>.<Component>` (persisted — pick them carefully), `register_message_dispatcher::<Msg>(&mut b, PRODUCER)` in `app.rs`, consumers via `#[meta(MessageConsumerMeta { ... })]` | domain `messages/`, `app.rs` | [`outbox.md`](../../../docs/internal/outbox.md) | outbox tests; forgotten dispatcher = agent panic at the first stored message |
| 12 | **Cross-context cleanup**: subscribe to other contexts' lifecycle messages (e.g. `DatasetLifecycleMessage::Deleted`) instead of foreign keys | services `message_handlers/` or `services/` | `kamu-domain-design` | a test deleting the foreign entity |
| 13 | **Config** (if any): setty struct in `src/app/cli/src/services/config/models.rs`, validation and `add_value` in `app.rs` | — | — | `make resources`; review the `resources/config-*` diff |
| 14 | **API** (if any): GraphQL roots/queries/mutations, REST handlers | `src/adapter/graphql/`, `src/adapter/http/` | `kamu-graphql-api` | `make resources-graphql-schema`; GraphQL tests |
| 15 | **E2E** (if user-visible through the CLI): scenarios in `src/e2e/app/cli/repo-tests`, wired in sqlite and postgres in lockstep | `src/e2e/app/cli/` | `kamu-cli-e2e-tests` | SQLite permutation green |
| 16 | **Docs**: add a `docs/internal/<ctx>.md` when the context has non-obvious mechanics, and route it in AGENTS.md | `docs/internal/`, `AGENTS.md` | `kamu-prose-and-comments` | `make lint-harness` |

Finish with `cargo fmt`, `make clippy`, and — since new DB crates were added — `make lint-sqlx`.

## Traps

- **Steps 7 and 9 are the ones usually half-done:** a crate missing from the Makefile lists has no
  `.env`, so its queries are checked offline against a cache that does not exist yet; a repository
  missing from one `database.rs` branch fails only when the app runs on that engine.
- **Persisted names are decided at step 11**, not later: renaming producers, consumers or event
  `typename()`s after release needs data migrations (`kamu-renaming-a-concept`).
- **A Singleton service must not depend on a repository** (`kamu-dill-di`).

## What lives elsewhere

- Renaming parts of an existing context: `kamu-renaming-a-concept`.
- Test harnesses for the services crate: `kamu-test-harness`.
