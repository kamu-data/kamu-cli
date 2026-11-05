# SQL Session Views — Architecture and Findings

> **Status:** session views work in `kamu sql`; every API entry point accepts queries only by
> default. This page explains how views live in a SQL session, what each entry point can do with
> them, and which extensions were tried and rejected. Findings come from experiments against
> `kamu sql`, `kamu sql server` with the Python ADBC FlightSQL driver, and `kamu system
> api-server`; the commands to repeat them are in [§6](#6-how-the-findings-were-obtained).
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** A view is a named logical plan stored in the session's default
schema (`kamu.kamu`, served by `KamuSchema`). It is created by `CREATE [TEMP] VIEW` when the
session's `SqlPolicy` allows session views, is never persisted, and disappears with the session.
Views are only useful where a session outlives one statement: the `kamu sql` shell, and FlightSQL.
HTTP `/query`, GraphQL `data.query`, `kamu sql -c` and exports run a single statement in a fresh
session, so a view created there can never be used; CTEs (`WITH …`) cover that need. A session
reads each dataset as of the first time it scans it, so views see the same frozen snapshot as the
rest of the session.

| You want to… | Start at |
| --- | --- |
| Know what a view captures and when data is read | [§2 How a session view works](#2-how-a-session-view-works) |
| Know which entry point keeps a session, and for how long | [§3 Sessions per entry point](#3-sessions-per-entry-point) |
| Decide whether to allow views for an entry point | [§4 Findings](#4-findings) |
| Avoid re-proposing a dead end | [§5 Rejected approaches](#5-rejected-approaches) |
| Find the file for X | [§8 Reference map](#8-reference-map) |

---

## 1. Purpose & scope

This page covers session views created with SQL (`CREATE VIEW`, `DROP VIEW`). It does not cover
ODF named queries in a transform (`SetTransform` `queries`), nor saved queries as a product
feature. Which statements a session accepts is decided by `SqlPolicy`
([`sql_policy.rs`](/src/domain/core/src/services/sql_policy.rs)), configured per kind of caller
under `engine.datafusionEmbedded.sqlPolicy` (`local` for `kamu sql`, `api` for everything else).

## 2. How a session view works

- **Storage.** `KamuSchema` keeps datasets and session-created tables in two maps
  ([`kamu_schema.rs`](/src/infra/core/src/services/query/kamu_schema.rs)). `register_table`
  re-checks the session's policy and refuses any name already taken by a dataset loaded into the
  session; `deregister_table` refuses dataset names, so `CREATE OR REPLACE VIEW <dataset>` and
  `DROP VIEW <dataset>` fail.
- **`TEMP`.** `DataFusion` rejects `CREATE TEMP VIEW`. Every view is session-scoped here, so
  `SqlPolicy::check_plan` runs a `TEMP` view as a plain one.
- **Schemas.** Views can be created as `v`, `kamu.v` or `kamu.kamu.v`. Other schemas do not exist
  (`failed to resolve schema`), and `information_schema` refuses registration.
- **Names.** Identifier normalization is off, so names are case-sensitive: `"FOO"` is a different
  name from dataset `foo`. A view may take any name that no dataset loaded into the session uses,
  including `"acc/ds"`.
- **A view stores a plan, not SQL.** Its plan is built at creation, so a view over another view
  keeps the definition it saw: dropping or replacing the inner view does not change it.
- **Snapshot.** `KamuTable` resolves a dataset's `HEAD` on the first scan in the session and keeps
  it for the session's lifetime. Data committed before that first scan is read even by views
  created earlier; data committed after it is not. Datasets created after the session started are
  not visible in it, so their names are free for views in that session.
- **Discovery.** `SHOW TABLES` and `information_schema.tables` list views with type `VIEW`;
  `information_schema.views` and `SHOW CREATE TABLE` return the definition; `DESCRIBE` works.
- **`to_table()`** works inside a view body and follows the same snapshot rule.
- **Cost.** 5 000 views in one `kamu sql` session took under a second and about 18 MB.

## 3. Sessions per entry point

| Entry point | Session | Statements per session | Policy scope |
| --- | --- | --- | --- |
| `kamu sql` shell | One per run | Many | `local` |
| `kamu sql -c`, export | One per call | One | `local` |
| HTTP `/query` | One per request | One | `api` |
| GraphQL `data.query` | One per request | One | `api` |
| FlightSQL (`kamu sql server`, `kamu notebook`, `api-server`) | One per bearer token, see below | Many | `api` |

FlightSQL keys sessions by the client's token
([`session_manager_caching.rs`](/src/adapter/flight-sql/src/session_manager_caching.rs)): each
anonymous handshake gets its own token, so clients never share a session. A session lives for the
expiration timeout (30 min by default), but its `SessionContext`, which holds the views, is
dropped after the inactivity timeout (5 s by default) and rebuilt empty on the next request.
Prepared plans are stored in the session, not the context, so they survive that release.

## 4. Findings

| Capability | `kamu sql` shell | `-c` / export | HTTP / GraphQL | FlightSQL |
| --- | --- | --- | --- | --- |
| Create and use a view | Works | Pointless: the session ends with the statement | Pointless, same reason | Works while the client sends a request at least every inactivity timeout |
| View survives a pause | Yes | — | — | No: lost after a gap longer than the inactivity timeout (seen with a 6 s gap) |
| `CREATE VIEW` over a dataset as the only statement | Works | Fails: "No field named …" | Fails: "No field named …" | Works |
| Several statements in one call | Works | "Internal error" | HTTP 500, empty body | One per request by protocol |
| Isolation between users | — | — | — | Holds: another token never sees the view |
| Plain-statement protocol (`GetFlightInfo` then `DoGet`) | — | — | — | DDL runs twice: the view is created, then `DoGet` fails with "already exists" |
| Prepared-statement protocol (ADBC, every statement) | — | — | — | Works: DDL runs once, at prepare time |
| Metadata listing | `SHOW TABLES` | — | — | `GetTables` reports views as `TABLE`; ADBC `get_objects` fails with "Invalid schema returned" for every listing, views or not |
| Server memory per view | ≈4 KB | — | — | ≈110 KB, held until the session expires |
| Named subquery in one statement | CTE | CTE | CTE, and `/query` records its input datasets | CTE |

**Recommendation.**

- **Shell:** keep views on (`local` default).
- **HTTP, GraphQL, `-c`:** keep views off. They can never be used across statements; point users
  to CTEs instead.
- **FlightSQL:** views fit the notebook use case, but enabling them by default needs the follow-ups
  in [§7](#7-follow-ups) first: views must survive the context release, DDL must run once on the
  plain-statement path, the number of views per session must be capped, and FlightSQL needs its own
  policy scope so it can differ from HTTP and GraphQL.

## 5. Rejected approaches

| Approach | Why it was rejected |
| --- | --- |
| Allowing views over HTTP `/query` and GraphQL | Each request is one statement in a fresh session, so a view can never be used by a later statement. CTEs give the same result in one statement, and `/query` records the datasets a CTE reads, so commitments stay reproducible |
| Persistent views shared across sessions or users | Needs storage, ownership, authorization and versioning: that is a saved-query feature, not a SQL session feature. It would also break `/verify`, which re-runs only the committed SQL text over the recorded datasets: a view's definition and inputs are in neither |
| Views in derivative transforms | ODF already names intermediate queries (`SetTransform` `queries: [{alias, query}]`); ingest preprocessing runs them as views in [`engine_datafusion_inproc.rs`](/src/infra/core/src/engine/engine_datafusion_inproc.rs) |
| Distinguishing `TEMP` from non-`TEMP` views | Every view is session-scoped, so the two would behave identically |
| Refreshing a view's datasets to their latest `HEAD` on every query | A session reads one consistent snapshot; refreshing only views would make a view and a direct query of the same dataset disagree within the session |

## 6. How the findings were obtained

- **Shell semantics:** the tests listed in [§8](#8-reference-map).
- **FlightSQL:** `kamu config set engine.datafusionEmbedded.sqlPolicy.api.allowSessionViews true`,
  then `kamu sql server --address 127.0.0.1 --port <port>` in a workspace with one dataset, driven by
  `adbc_driver_flightsql.dbapi.connect("grpc://127.0.0.1:<port>", …)` with the anonymous
  login used in `images/demo/user-home/XX - Other Examples/flight-sql/python/client_flightsql_adbc.py`.
  Lifetime: create a view, then query it after pauses of 2, 4 and 6 s. Memory: compare the
  server's `VmRSS` before and after creating 5 000 views on one connection.
- **HTTP:** the same workspace under `kamu system api-server`, posting each statement to `/query`.

## 7. Follow-ups

| Item | Size |
| --- | --- |
| FlightSQL: keep view definitions in the session (not the context) and re-create them when a context is rebuilt | M |
| FlightSQL: run DDL once on the plain-statement path, e.g. answer `DoGet` of a DDL ticket with an empty result instead of re-planning the SQL text | S |
| FlightSQL: own policy scope, separate from HTTP and GraphQL | S |
| Cap the number of views per session | S |
| FlightSQL: `GetTables` should report views as `VIEW`; ADBC `get_objects` fails on any schema listing | S |
| Statements other than `SELECT` (`DESCRIBE`, `SHOW COLUMNS`, `CREATE VIEW`) see datasets without their schema in one-statement calls, so `DESCRIBE foo` returns no rows over HTTP and `kamu sql -c` | S |
| Several statements in one call: report "only a single statement is supported" as a bad request instead of an internal error | S |
| FlightSQL retains about 16 KB per executed statement until the session expires | M |

## 8. Reference map

| What | Where |
| --- | --- |
| Policy and its checks | [`sql_policy.rs`](/src/domain/core/src/services/sql_policy.rs), [`sql_policy_config.rs`](/src/domain/core/src/services/sql_policy_config.rs) |
| Where views are stored | [`kamu_schema.rs`](/src/infra/core/src/services/query/kamu_schema.rs) |
| Snapshot of a dataset in a session | [`kamu_table.rs`](/src/infra/core/src/services/query/kamu_table.rs) |
| Sessions and one-statement queries | [`query_service_impl.rs`](/src/infra/core/src/services/query_service_impl.rs) |
| SQL shell | [`sql_shell_session_context.rs`](/src/app/cli/src/explore/sql_shell_session_context.rs) |
| FlightSQL sessions and handlers | [`session_manager_caching.rs`](/src/adapter/flight-sql/src/session_manager_caching.rs), [`service.rs`](/src/adapter/flight-sql/src/service.rs) |
| Tests of view behaviour | [`test_query_service_sql_policy.rs`](/src/infra/core/tests/tests/queries/test_query_service_sql_policy.rs): `test_local_session_views`, `test_local_session_views_cannot_replace_datasets`, `test_local_session_view_keeps_definition_of_views_it_reads`, `test_local_session_views_live_in_default_schema`, `test_local_session_views_are_listed`, `test_session_reads_datasets_as_of_first_scan` |
| E2E | `test_datafusion_cli_session_views`, `test_sql_query_rejects_session_views` in [`src/e2e/app/cli/repo-tests`](/src/e2e/app/cli/repo-tests) |
