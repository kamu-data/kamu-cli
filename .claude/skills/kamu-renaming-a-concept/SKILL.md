---
name: kamu-renaming-a-concept
description: Blast-radius checklist for renaming anything in Kamu CLI — a type, trait, module, crate, field, enum variant, message, consumer, flow/task type, metric, CLI flag, config key, GraphQL or REST name. Use before starting any rename, including "small" ones, to find which layers the name crosses and which persisted names must NOT move.
---

# Renaming A Concept In Kamu

A name in this codebase usually lives in more places than the compiler sees. Rust identifiers
are the easy part — the compiler finds every use. The dangerous part is **strings derived from
or stored next to those identifiers**: they compile fine after a rename and break at runtime, in
a user's database, or in someone else's dashboard.

## Step 1 — Classify every occurrence

Grep for the shape, not just the exact name: `CamelCase`, `snake_case`, `kebab-case`,
`SCREAMING_CASE`, the camelCase of a struct field, and the dotted `dev.kamu....` form.

```bash
git grep -n -i -E 'old_name|OldName|old-name|oldName'
```

Then sort each hit into one of the layers below.

## Step 2 — The layers

| # | Layer | Where | Safe to rename? | What it takes |
|---|---|---|---|---|
| 1 | Rust identifiers (types, fns, modules, local names) | anywhere | Yes | Compiler-guided; `make clippy` |
| 2 | Crate / package names | root `Cargo.toml` members + `[workspace.dependencies]`, every dependent `Cargo.toml`, Makefile `POSTGRES_CRATES`/`SQLITE_CRATES` for DB crates | Yes, but wide | Update all three places; `cargo deny check` |
| 3 | dill wiring | `src/app/cli/src/app.rs`, `database.rs` (fully qualified paths), each `dependencies.rs::register_dependencies` | Yes | These are the sites a rename most often misses — they compile only when the whole app builds |
| 4 | Test harnesses and e2e regexes | `src/e2e/**` assertions match CLI output by regex | Yes | Re-run the affected e2e tests; a regex can silently stop matching what it should |
| 5 | Generated API contracts | `resources/schema.gql` (GraphQL), `resources/openapi.json` (REST), `resources/cli-reference.md` (CLI flags), `resources/config-schema.json` + `config-reference.md` (config keys, camelCase of setty fields) | **Breaking for clients** if released | Regenerate (`make resources-graphql-schema`, `make resources`); check `git show <last-tag>:<file>` to see if the old name shipped — if so, ask before breaking it |
| 6 | DB tables, columns, Postgres enum types and their values | `migrations/{postgres,sqlite}`, `sqlx(type_name = ..., rename_all = ...)` on Rust enums | Only with migrations | New forward migration for **both** engines (never edit an applied one), then `make sqlx-prepare` |
| 7 | Serialized event and message payloads | Event-sourcing `event_payload` JSONB (serde, externally tagged by **Rust variant name**) and outbox `content_json` | **No** without a data migration | Renaming an enum variant or field changes the stored JSON shape; stored aggregates and pending messages stop deserializing. Needs a JSONB-rewriting migration (precedent: `*_update_flow_event_payloads.sql`) or a `#[serde(rename = "Old")]` |
| 8 | Persisted string identifiers | see the table below | **No** without a migration | Treat as data, not code |
| 9 | Observability names | metric names and label values ([`docs/internal/metrics.md`](../../../docs/internal/metrics.md)) — label values include agent, flow type, plan type, producer, consumer names | Breaks dashboards silently | Alert rules live in deployment repos; tell the user so they can update them |
| 10 | Docs | `docs/internal/*.md`, skills, `AGENTS.md`, `DEVELOPER.md`, `CHANGELOG.md` (only when finalizing) | Yes | Amend in the same change (AGENTS.md, "Documentation classes") |

### Persisted identifiers that must not move silently

| Identifier | Example | Stored in | Renaming without a migration |
|---|---|---|---|
| Outbox producer name | `"dev.kamu.domain.webhooks.WebhookSubscriptionService"` | `outbox_messages.producer_name` | Stored messages are orphaned |
| Outbox consumer name | `"dev.kamu.domain.webhooks.WebhookDatasetRemovalHandler"` | `outbox_message_consumptions.consumer_name` | Consumer restarts from its initial boundary — skips or replays messages |
| Message `version()` | `WEBHOOK_SUBSCRIPTION_LIFECYCLE_OUTBOX_VERSION` | `outbox_messages.version` | A bump silently drops unconsumed history ([outbox.md §3.3](../../../docs/internal/outbox.md#33-evolving-messages-and-consumers)) |
| Event `typename()` | `"WebhookSubscriptionEventUpdated"` (variant is `Modified`) | `*_events.event_type` | The stored string is the stable one; the Rust variant may already differ |
| Flow type | `"dev.kamu.flow.dataset.ingest"` | `flow_type` columns, event JSON, metric label | Flows/configs no longer match their controller |
| Task definition / logical plan type | `"dev.kamu.tasks.webhook.deliver"`, `"DeliverWebhook"` | task event JSON, `plan_type` metric label | Tasks fail to deserialize |
| Task result / error / flow config rule type | `"UpdateDatasetResult"`, `"IngestRule"` | event JSON | Same |
| Projector name | `"dev.kamu.domain.flow-system.FlowProcessStateProjector"` | `flow_system_projected_offsets.projector` | Re-projects from offset 0 |
| Background agent name | `FLOW_AGENT_NAME`, `TASK_AGENT_NAME` | outbox producer names, `agent` metric label | Orphaned messages, broken dashboards |
| `NOTIFY` channel | `'tasks_queued'` | trigger functions in migrations; mirrored in Rust constants for postgres, sqlite and inmem | Listeners never wake and fall back to timeout polling ([wakeup-listeners.md](../../../docs/internal/wakeup-listeners.md)) |
| Resource schema URI | `"https://opendatafabric.org/schemas/storage/v1alpha1/Storage"` | `resources.resource_schema`, `resource_events.resource_schema` | Stored resources orphaned; user manifests stop validating |
| Manifest / spec field names | `#[serde(rename_all = "camelCase")]`, `#[serde(rename = "$schema")]` | user manifests, `spec` JSONB | User files break |
| Webhook event types | `"DATASET.REF.UPDATED"` | `webhook_subscriptions.event_types`, `x-webhook-event-type` header | Subscribers stop receiving events |

The usual pattern: rename the Rust constant's *name* freely, keep its *value*. If the value
itself must change, write a forward migration that `UPDATE`s the stored strings (precedents:
`migrations/postgres/20241217205719_executor2agent.sql`,
`20250605174105_rename-dataset-account-deletion-handler.sql`) for both engines.

## Step 3 — Prove it

1. `make clippy` — covers layers 1–3.
2. `make resources` and `make resources-graphql-schema`; read the `resources/` diff — every
   change there is a client-visible rename (layer 5).
3. If a migration was added: `make sqlx-prepare`, then the repository tests for both engines.
4. Re-grep for every case variant of the old name. Remaining hits must each be a deliberate
   keep (a persisted value, a migration, a changelog line); name them in the hand-back.

## What lives elsewhere

- Migration and SQLx mechanics: `kamu-sqlx-database-work`.
- Outbox message evolution: [`docs/internal/outbox.md`](../../../docs/internal/outbox.md).
- Whether a GraphQL change is breaking: `kamu-graphql-api`.
