# Messaging Outbox — Architecture

> **Status:** stable, the backbone of cross-domain communication in kamu-cli.
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** A domain service that changed something (a dataset was created, an
account was renamed, a task finished) **posts a message** to the `Outbox` under its **producer
name**. Other domains react to it by registering **consumers** for that producer. A consumer is
either **immediate** — called synchronously inside the producer's transaction — or **durable** —
the message is stored in `outbox_messages` as part of the producer's transaction, and the
**outbox agent** delivers it later, in its own transaction, tracking per consumer how far it got
(`outbox_message_consumptions`). Durable delivery is ordered per producer by transaction ID, then by
the order messages were posted, survives restarts, and isolates failing consumers. Wiring is
declarative: message types are plain serde structs, consumers are dill components annotated with
`MessageConsumerMeta`, and routes are discovered from the catalog at startup.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Add a new message type / producer | [§3.1 Recipe: new message type](#31-recipe-a-new-message-type-and-producer) |
| Add a consumer for an existing producer | [§3.2 Recipe: new consumer](#32-recipe-a-new-consumer-for-an-existing-producer) |
| Change, rename or retire messages and consumers | [§3.3 Evolving](#33-evolving-messages-and-consumers) |
| Pick a consumption mode | [§4 Consumption modes](#4-consumption-modes) |
| Know what ordering and delivery you can rely on | [§6 Guarantees](#6-ordering--delivery-guarantees) |
| Understand the agent's processing loop | [§5 The outbox agent](#5-the-outbox-agent) |
| Tune batching / concurrency, read metrics | [§8 Configuration & metrics](#8-configuration--metrics) |
| Test code that posts or consumes messages | [§9 Testing](#9-testing) |
| Find the file for X | [§11 Reference map](#11-filecrate-reference-map) |

---

## Table of contents

- [1. Purpose \& scope](#1-purpose--scope)
- [2. Concepts](#2-concepts)
- [3. Recipes](#3-recipes)
- [4. Consumption modes](#4-consumption-modes)
- [5. The outbox agent](#5-the-outbox-agent)
- [6. Ordering \& delivery guarantees](#6-ordering--delivery-guarantees)
- [7. Storage backends](#7-storage-backends)
- [8. Configuration \& metrics](#8-configuration--metrics)
- [9. Testing](#9-testing)
- [10. Gotchas](#10-gotchas)
- [11. File/crate reference map](#11-filecrate-reference-map)

---

## 1. Purpose & scope

Domains must react to each other's changes without depending on each other's services, and without
losing the reaction when the process crashes right after the change committed. The outbox is the
transactional-outbox pattern: the message is written in the **same transaction** as the change that
caused it, so it exists if and only if the change does, and is delivered afterwards.

In scope: the `Outbox` API, message and consumer declarations, the dispatching between immediate
and durable consumers, the outbox agent, and the storage bridges. Out of scope: how the agent is
woken up (see [wakeup-listeners.md](wakeup-listeners.md)), and the flow system's own event
projections (`FlowSystemEventAgent`), which are a separate mechanism.

---

## 2. Concepts

| Concept | What it is | In code |
| --- | --- | --- |
| **Message** | A serde-serializable type with a schema `version()` | `trait Message` |
| **Producer** | A stable string name under which messages are posted; one message type per producer | `MESSAGE_PRODUCER_*` constants |
| **Dispatcher** | Deserializes a producer's JSON into its message type and calls consumers | `register_message_dispatcher::<M>(b, PRODUCER)` |
| **Consumer** | A dill component implementing `MessageConsumerT<M>`, annotated with `MessageConsumerMeta` | `MESSAGE_CONSUMER_*` constants |
| **Route** | A (producer, consumer) pair, derived from `feeding_producers` of each consumer | `MessageSubscription` |
| **Boundary** | Position in a producer's stream: `(tx_id, message_id)` | `OutboxMessageBoundary` |
| **Consumption record** | The last boundary a consumer has consumed, per route | `outbox_message_consumptions` table |

```text
 producer's transaction                                    outbox agent (later)
┌───────────────────────────────────┐                ┌──────────────────────────────────────┐
│ service changes its state         │                │ per producer, per message in order:  │
│ outbox.post_message(PRODUCER, m)  │                │   per lagging consumer (concurrently)│
│   ├─ Immediate consumers: called  │   commit       │     own transaction:                 │
│   │  right here, same transaction │ ─────────────► │       consume_message(m)             │
│   └─ Durable consumers exist?     │ outbox_messages│       mark_consumed(boundary)        │
│      INSERT INTO outbox_messages  │                │                                      │
└───────────────────────────────────┘                └──────────────────────────────────────┘
```

`OutboxDispatchingImpl` (bound as `dyn Outbox` in the app) classifies producers once from consumer
metadata: if a producer has immediate consumers, the message goes to `OutboxImmediateImpl`; if it has
durable consumers, it is also stored via `OutboxTransactionalImpl`. A producer with no consumers of
either kind is posted to nobody — nothing is stored.

---

## 3. Recipes

The recipes use the real account lifecycle messages as the running example.

### 3.1 Recipe: a new message type and producer

**1. Declare the producer name** in the domain crate (`<domain>/domain/src/messages/*_message_producers.rs`).
The string is persisted in the database, so pick it once:

```rust
pub const MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE: &str = "dev.kamu.domain.accounts.AccountsService";
```

**2. Declare the message type** next to it (`account_lifecycle_message.rs`). Use an enum when a
producer reports several kinds of events — one producer carries exactly one message type:

```rust
const ACCOUNT_LIFECYCLE_OUTBOX_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountLifecycleMessage {
    Created(AccountLifecycleMessageCreated),
    Updated(AccountLifecycleMessageUpdated),
    PasswordChanged(AccountLifecycleMessagePasswordChanged),
    Deleted(AccountLifecycleMessageDeleted),
}

impl Message for AccountLifecycleMessage {
    fn version() -> u32 {
        ACCOUNT_LIFECYCLE_OUTBOX_VERSION
    }
}
```

**3. Register a dispatcher** for the producer, once per application — in `src/app/cli/src/app.rs`
(`configure_base_catalog`), next to the others:

```rust
register_message_dispatcher::<AccountLifecycleMessage>(
    &mut b,
    MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
);
```

Without it, immediate consumers silently get nothing, and the outbox agent panics
(`No dispatcher for producer`) when it meets a stored message of this producer.

**4. Post the message** from the service, inside the transaction that made the change. Inject
`Arc<dyn Outbox>` and use `OutboxExt::post_message`:

```rust
use messaging_outbox::OutboxExt;

self.outbox
    .post_message(
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
        AccountLifecycleMessage::created(/* ... */),
    )
    .await
```

Post *after* the change is written but in the same transaction: an immediate consumer may read
the new state, and a durable message must not exist without the change.

**5. Add at least one consumer** ([§3.2](#32-recipe-a-new-consumer-for-an-existing-producer)) —
until then, messages are dropped at posting time.

### 3.2 Recipe: a new consumer for an existing producer

**1. Declare the consumer name** in the consuming domain
(`<domain>/domain/src/messages/*_message_consumers.rs`). It keys the consumer's progress in
`outbox_message_consumptions`, so it must stay stable:

```rust
pub const MESSAGE_CONSUMER_KAMU_DATASETS_LIFECYCLE_HANDLER: &str = "dev.kamu.domain.datasets.DatasetLifecycleHandler";
```

**2. Implement the consumer** as a dill component. It needs both interfaces, and the metadata that
defines its routes:

```rust
use messaging_outbox::prelude::*;

#[dill::component(pub)]
#[dill::interface(dyn MessageConsumer)]
#[dill::interface(dyn MessageConsumerT<AccountLifecycleMessage>)]
#[dill::meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASETS_LIFECYCLE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
    ],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct DatasetAccountLifecycleHandler {
    dataset_registry: Arc<dyn DatasetRegistry>,
    // ...
}

impl MessageConsumer for DatasetAccountLifecycleHandler {}

#[async_trait::async_trait]
impl MessageConsumerT<AccountLifecycleMessage> for DatasetAccountLifecycleHandler {
    async fn consume_message(
        &self,
        _: &dill::Catalog,
        message: &AccountLifecycleMessage,
    ) -> Result<(), InternalError> {
        match message {
            AccountLifecycleMessage::Updated(m) => self.handle_updated(m).await,
            AccountLifecycleMessage::Deleted(m) => self.handle_deleted(m).await,
            // Match exhaustively: a new variant should make you decide here
            AccountLifecycleMessage::Created(_) | AccountLifecycleMessage::PasswordChanged(_) => Ok(()),
        }
    }
}
```

**3. Choose the two metadata knobs:**

- `consumption_mode` — see [§4](#4-consumption-modes). Default choice: `TransactionalWrapped`.
- `initial_consumer_boundary` — where a **newly deployed** consumer starts:
  - `Latest`: only messages produced after its first startup. Use when the consumer's state can be
    (or is) built from current data, e.g. an index that is rebuilt on its own.
  - `All`: replay the producer's whole history still stored in `outbox_messages`. Use when the
    consumer derives state from past events that exist nowhere else.

**4. Register the component** in the domain's `register_dependencies` (e.g.
`src/domain/datasets/services/src/dependencies.rs`):

```rust
b.add::<DatasetAccountLifecycleHandler>();
```

That is all: at the next startup the agent discovers the new route, creates its consumption record
at the initial boundary ([§5.1](#51-startup)), and starts delivering. One consumer may listen to
several producers — list them in `feeding_producers` and implement `MessageConsumerT<M>` (plus the
`#[dill::interface]`) for each message type.

### 3.3 Evolving messages and consumers

| Change | What to do |
| --- | --- |
| Add an optional field / new enum variant | Compatible if old stored JSON still deserializes (`#[serde(default)]` for new fields); no version bump. Update consumers' matches |
| Breaking change of the message structure | Bump the message `version()`. Stored messages with another version are **skipped** by consumers (logged as an error, boundary still advances), so consumers must tolerate missing them |
| Rename a producer or consumer | Write a migration updating `outbox_messages.producer_name` / `outbox_message_consumptions` (see `20241217205719_executor2agent.sql`), otherwise a renamed consumer starts over as new |
| Re-process history for a consumer | Reset its consumption record in a migration (see `20241024110339_dataset_entries_reindexing.sql`) |
| Remove a consumer | Delete the component; its consumption record becomes unused and is ignored |
| Change consumption mode | Safe between Wrapped and SelfManaged (same record). Switching to/from Immediate changes whether messages are stored at all |

---

## 4. Consumption modes

| Mode | When it runs | Transaction | On failure | Use for |
| --- | --- | --- | --- | --- |
| `Immediate` | Synchronously inside `post_message` | The producer's own | Error propagates: the producer's operation fails and rolls back | Logical separation **within** a domain, when the reaction must be atomic with the change. Refrain from cross-domain use |
| `TransactionalWrapped` | Later, by the outbox agent | A new one per (message, consumer), shared by the consumer's work **and** the boundary update | Rolled back, consumer marked failed ([§5.4](#54-failures)) | Default for durable reactions to database state |
| `TransactionalSelfManaged` | Later, by the outbox agent | None given: the consumer gets the base catalog and opens its own; the boundary is advanced in a separate transaction afterwards | Consumer marked failed; boundary not advanced | Work touching external systems (search index), long operations, or several transactions per message |

Wrapped consumers' database effects and progress commit atomically, so each message's effects
apply exactly once. Self-managed consumers get at-least-once delivery: a crash between their work and
the boundary update redelivers the message, so they must be idempotent.

Resolve dependencies from the catalog you are given, or inject them into the component: a wrapped
consumer is built from the transaction catalog, so its injected repositories use that transaction.

---

## 5. The outbox agent

`OutboxAgentImpl` (`src/utils/messaging-outbox/src/agent`) is a singleton `InitOnStartup` +
`BackgroundAgent`. Only durable (`TransactionalWrapped`/`TransactionalSelfManaged`) routes are
its concern.

### 5.1 Startup

`run_initialization` (job `JOB_MESSAGING_OUTBOX_STARTUP`) enumerates routes from consumer metadata
and makes sure every route has a consumption record. A missing one is created at the consumer's
initial boundary: `Latest` → the producer's latest visible message, `All` (or no messages yet) →
`(0, 0)`.

### 5.2 Main loop

```text
run():  drain  → loop { wait_wake(maxListeningTimeout, minDebounceInterval); drain }
drain:  loop { n = consumption_iteration(); if n == 0 break }
```

Waking is described in [wakeup-listeners.md](wakeup-listeners.md) (channel
`outbox_messages_ready`). The same drain is callable as `run_while_has_tasks()`: the CLI calls it
after commands that need it, and the HTTP E2E middleware after every successful mutating request,
so E2E tests observe consumers' effects synchronously. A `run_lock` keeps these entrances from
overlapping with the main loop.

### 5.3 One iteration

1. **Plan** (one read-only transaction, `OutboxConsumptionIterationPlanner`):
   - read the latest message boundary per producer, and all consumption records;
   - per producer, skip consumers already marked failed; the **processed boundary** is the minimum
     over the remaining consumers' boundaries (a consumer without a record forces `(0, 0)`);
   - producers whose processed boundary is below their latest message are behind; load one batch
     (`batching.outboxMessages`) of messages above those boundaries, ordered by `(tx_id, message_id)`.
2. **Execute** — producers run concurrently, each as a `ProducerConsumptionJob`:
   - messages strictly in order; for each message, every non-failed consumer whose own boundary is
     below it becomes a `ConsumeMessageTask`, spawned as a tokio task;
   - each task waits for a permit of a semaphore **shared by all producers**
     (`concurrency.outboxConsumers`), then runs in its mode ([§4](#4-consumption-modes)) and
     advances its boundary to the message;
   - all tasks of a message finish before the next message starts.
3. The iteration returns how many (message, consumer) tasks succeeded; the drain stops at zero.

`mark_consumed` only ever moves a boundary forward (`WHERE new > old`), so replays and races between
entrances cannot move progress back.

### 5.4 Failures

A consumer that fails a message is **blocked for the rest of the iteration** (it cannot skip over
the failed message), and added to the job's failed set: the planner excludes it from then on, so it
neither retries nor holds back other consumers of the same producer. The failed set lives in memory
— **a restart retries** from the stored boundary. `outbox_failed_consumers_total` shows it as `1`,
and `outbox_messages_pending_total` grows. When all consumers of a producer have failed, the job
stops processing that producer.

---

## 6. Ordering & delivery guarantees

- **Per producer, messages are delivered in transaction ID order, then in the order they were
  posted** (message ID) — the order of `OutboxMessageBoundary`: `(tx_id, message_id)`. On Postgres
  `tx_id` is the inserting transaction's `xid8` (column default `pg_current_xact_id()`); only messages
  of committed transactions (or the reader's own) are visible to the agent.
- **Per consumer, strictly sequential**: message N+1 is never handed to a consumer before it
  finished N. Different consumers of the same message run concurrently.
- **Across producers: no ordering.** Producers are processed concurrently and independently; a
  consumer listening to two producers may see their messages interleaved in any order.
- **Durable delivery survives restarts**: progress is stored per route; after a crash, delivery
  resumes from the last committed boundary (Wrapped: exactly-once effects; SelfManaged:
  at-least-once).
- **Immediate consumers** see messages in posting order within the producer's transaction, and
  nothing about them is stored.

### Why boundaries carry `tx_id`

Originally consumers tracked only `last_consumed_message_id`. Message IDs come from a Postgres
sequence, which is not transactional: IDs are reserved at insert time, while transactions commit in
a different order. With concurrent transactions of one producer, a message with a smaller ID could
become visible *after* the consumer had moved past a larger one, and was then lost forever
([#1398](https://github.com/kamu-data/kamu-cli/issues/1398),
[background article](https://event-driven.io/en/ordering_in_postgres_outbox/)).

The alternatives weighed in #1398 were:

- a per-producer counter row locked for the whole transaction;
- a look-back window over late messages, with de-duplication;
- IDs assigned at commit time by a deferred trigger;
- idempotent consumers remembering every processed message;
- logical decoding of the WAL (Debezium-style).

The chosen one is the lightest: order by the inserting transaction's ID (`xid8`), then by message ID. Experiments
with the flow system's event bridge had proved it before, and `flow_events` uses the same
`(tx_id, event_id)` scheme.

Consequently a boundary must be the **last message in `(tx_id, message_id)` order**, not the one with
the highest message ID. In a batch like

```text
tx 226813: messages 7004–7006, 7009–7011, 7013–7018
tx 226814: message  7003
tx 226815: messages 7007–7008
```

the boundary to record is `(226815, 7008)`; recording `(226813, 7018)` would re-deliver the
messages of transactions 226814 and 226815 (see `impl Ord for OutboxMessageBoundary`).

### Deviation from the article

The article reads only transactions older than every running one
(`tx_id < pg_snapshot_xmin(pg_current_snapshot())`). Our bridges filter by visibility instead
(`pg_visible_in_snapshot(tx_id, pg_current_snapshot())`), so that unrelated long transactions do not
hold delivery back. The trade-off: a message from an older transaction that commits after a newer one
was already delivered sorts below the consumer's boundary and is skipped. The Postgres agent test
does not cover this interleaving.

---

## 7. Storage backends

`OutboxMessageBridge` (`src/utils/messaging-outbox/src/repos`) abstracts storage; the agent and
`OutboxTransactionalImpl` use nothing else.

| Backend | Crate | Ordering key | Wakeup |
| --- | --- | --- | --- |
| Postgres | `kamu-messaging-outbox-postgres` | `(tx_id xid8, message_id)`, index `idx_om_tx_order` | `outbox_notify` trigger → `outbox_messages_ready` |
| SQLite | `kamu-messaging-outbox-sqlite` | `message_id` only (`tx_id` is always `0`): one connection, so transactions never interleave | `SqlitePollingHub`: `SELECT MAX(message_id)` |
| In-memory | `kamu-messaging-outbox-inmem` | `message_id` only | `InMemoryWakeupHub` signal on push |

Tables: `outbox_messages(message_id, producer_name, content_json, occurred_on, version, tx_id)` and
`outbox_message_consumptions(consumer_name, producer_name, last_consumed_message_id, last_tx_id)`.
Messages are never deleted by the agent; `wipe_outbox_data()` (Postgres) resets both tables when a
migration invalidates history.

Shared bridge behaviour is covered by `src/infra/messaging-outbox/repo-tests`, run against all
three backends.

---

## 8. Configuration & metrics

```yaml
backgroundAgents:
  minDebounceInterval: 20ms   # wakeup, see wakeup-listeners.md
  maxListeningTimeout: 2s
  batching:
    outboxMessages: 20        # OutboxAgentConfig::batch_size — messages loaded per iteration
  concurrency:
    outboxConsumers: 1        # OutboxAgentConfig::consumer_concurrency — consumer tasks at once
```

- `batching.outboxMessages` — messages planned per iteration, across producers. Larger batches
  save planning round trips when catching up on a backlog; smaller ones hand the SQLite connection
  back to API requests sooner.
- `concurrency.outboxConsumers` — consumer tasks running at once, across all producers, each holding
  a pooled connection. 1 by default for SQLite's single connection; 8 in `production_default()`. It shares the pool with the flow, task and flow system
  event agents, the Postgres `LISTEN` connection and API requests, so size `database.maxConnections`
  (default `20`) for their sum.

Prometheus metrics (`OutboxAgentMetrics`), labelled by `producer` and `consumer`:

| Metric | Meaning |
| --- | --- |
| `outbox_messages_processed_total` | Messages successfully consumed |
| `outbox_messages_pending_total` | Best-effort backlog estimate (latest message ID − consumed message ID) |
| `outbox_failed_consumers_total` | `1` while the consumer is failed (until restart) |

Like every wakeup-driven agent, it also reports `wakeup_listener_last_heartbeat_timestamp_seconds`,
beating after every consumption iteration.
Recommended alerts on these metrics are in [metrics.md](metrics.md#4-recommended-alerts).

---

## 9. Testing

| Need | Use |
| --- | --- |
| Code posts messages, consumers irrelevant | `OutboxProvider::Dummy` (`DummyOutboxImpl`) |
| Assert what gets posted | `MockOutbox` with helpers like `kamu_accounts::testing::mock_messages` |
| Run consumers synchronously in-process | `OutboxProvider::Immediate { force_immediate: true }` — calls **all** consumers inside `post_message`, regardless of mode |
| Full durable path | `OutboxProvider::Dispatching` + an outbox bridge, then `outbox_agent.run_while_has_tasks()` |
| Consumer logic alone | Call `consume_message` on the component directly |

`OutboxProvider` lives in `messaging_outbox::testing` (feature `testing`). The agent's own tests
(`src/utils/messaging-outbox/tests/tests/test_outbox_agent.rs`) cover routing, `Latest`/`All`
boundaries, failure isolation, self-managed consumers, batching and concurrency limits;
`src/infra/messaging-outbox/postgres/tests/agent` covers `tx_id` ordering on a real database.

---

## 10. Gotchas

- **Forgotten dispatcher registration** — immediate consumers never fire; the agent panics on the
  first stored message of that producer. Register in `app.rs`.
- **Forgotten consumer registration** — no route, no consumption record, and if it was the producer's
  only durable consumer, its messages are not even stored.
- **Two message types under one producer** — not supported: a producer has one dispatcher, hence
  one type. Use an enum.
- **A failing consumer stalls only itself**, silently until restart — watch
  `outbox_failed_consumers_total`.
- **Version bumps drop history** for consumers that have not consumed it yet ([§3.3](#33-evolving-messages-and-consumers)).
- **Immediate consumers across domains** couple the producer's transaction to foreign code: a bug
  there fails the producer's operation. Prefer durable modes across domains.
- **Consumer names are persisted** — renaming a constant without a migration re-registers the
  consumer from its initial boundary.

---

## 11. File/crate reference map

| Layer | Crate | Directory | Key files |
| --- | --- | --- | --- |
| API, messages | `messaging-outbox` | `src/utils/messaging-outbox/src` | `message.rs`, `services/outbox.rs` |
| Posting | `messaging-outbox` | `src/utils/messaging-outbox/src/services/implementation` | `outbox_dispatching_impl.rs`, `outbox_immediate_impl.rs`, `outbox_transactional_impl.rs` |
| Consumers, dispatch | `messaging-outbox` | `src/utils/messaging-outbox/src/consumers` | `message_consumer.rs`, `message_dispatcher.rs`, `message_consumers_utils.rs` |
| Agent | `messaging-outbox` | `src/utils/messaging-outbox/src/agent` | `outbox_agent_impl.rs`, `outbox_consumption_iteration_planner.rs`, `outbox_producer_consumption_job.rs`, `outbox_agent_metrics.rs` |
| Boundaries | `messaging-outbox` | `src/utils/messaging-outbox/src/entities` | `outbox_message_boundary.rs` |
| Storage contract | `messaging-outbox` | `src/utils/messaging-outbox/src/repos` | `outbox_message_bridge.rs` |
| Storage | `kamu-messaging-outbox-{postgres,sqlite,inmem}` | `src/infra/messaging-outbox/*/src/repos` | `*_outbox_message_bridge.rs` |
| Testing helpers | `messaging-outbox` (`testing`) | `src/utils/messaging-outbox/src/services/testing` | `test_outbox_provider.rs`, `mock_outbox_impl.rs` |
| Schema | — | `migrations/{postgres,sqlite}` | `*_outbox_messages_consumptions.sql`, `*_outbox_message_version.sql`, `*_outbox_listen_notify.sql`, `*_outbox_wipe*.sql` |
| App wiring | `kamu-cli` | `src/app/cli/src` | `app.rs` (outbox components, dispatchers), `services/config/models.rs` (`BackgroundAgentsConfig`) |
| Flushing | `kamu-cli`, `kamu-adapter-http` | `src/app/cli/src`, `src/adapter/http/src/e2e` | `app.rs` (`run_while_has_tasks` after commands), `e2e_middleware.rs` |
