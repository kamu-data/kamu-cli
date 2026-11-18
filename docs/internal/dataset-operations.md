# Dataset Operations — Shared Shape

Every operation that changes a local dataset — ingest, transform, sync, compaction, reset — follows
one shape: plan in a short transaction, do the heavy work with no transaction open, then publish
the result by moving `HEAD` with a compare-and-swap. This page owns that shape. Each operation's own
page describes only what is specific to it:

| Operation | Page |
| --- | --- |
| Pull: choosing and ordering the operations for many datasets | [dataset-pull.md](dataset-pull.md) |
| Polling ingest into a root dataset | [polling-ingest.md](polling-ingest.md) |
| Push ingest into a root dataset | [push-ingest.md](push-ingest.md) |
| Read, merge and write steps both ingest kinds share | [ingest-data-writer.md](ingest-data-writer.md) |
| Transform of a derivative | [derived-dataset-transform.md](derived-dataset-transform.md) |
| Sync from or to a remote | [dataset-sync.md](dataset-sync.md) |
| Hard compaction | [dataset-hard-compaction.md](dataset-hard-compaction.md) |
| Reset to a block, reset to metadata | [dataset-reset.md](dataset-reset.md) |

---

## At a glance

- **Blocks first, `HEAD` last.** An operation appends its blocks and data files without moving
  `HEAD`. Until `HEAD` moves, no reader sees them.
- **One way to move a reference.** `DatasetReferenceUpdater::update_reference` moves a dataset's
  reference (today only `HEAD`) from an expected block to a new one inside the caller's
  transaction, and fails when another writer moved it first. There is no unconditional move.
- **No transaction around slow work.** Fetching, engines and transfers run for minutes; they run
  between two short transactions, never inside one.
- **A lost race leaves garbage, not corruption.** The loser's blocks stay in storage unreferenced;
  the chain `HEAD` points at is never mixed.
- **One job per operation, for the CLI and the server.** `kamu pull` and the update task run the
  same job (`PollingIngestJob`, `TransformJob`, …), which owns the operation's transactions; they
  differ in who drives it and as whom it runs.

---

## 1. Components

```mermaid
flowchart LR
    subgraph Entry["Entry points"]
        CLI["CLI commands<br/>pull, push, compact, reset, verify"]
        GQL["GraphQL / REST"]
        FLOW["Flows → tasks<br/>(task agent)"]
    end

    subgraph Orchestration
        UC["Pull and push use cases<br/>plan, loop, report"]
        RUN["Task runners<br/>map outcomes"]
    end

    JOBS["Operation jobs<br/>authorize, short transactions"]
    PLAN["Planners<br/>ingest · transform · compaction · reset"]
    EXEC["Executors, no transaction<br/>ingest · transform · sync · compaction"]
    COMMIT["DatasetReferenceUpdater<br/>CAS on HEAD"]
    OUTBOX[("Outbox:<br/>DatasetReferenceMessage")]

    CLI --> UC
    CLI -- "compact, reset, verify" --> JOBS
    GQL --> UC
    FLOW --> RUN
    UC --> JOBS
    RUN --> JOBS
    JOBS -- plan --> PLAN
    JOBS -- execute --> EXEC
    JOBS -- commit --> COMMIT
    COMMIT -- "via DatasetReferenceService" --> OUTBOX
```

- **Operation jobs** run one operation on one dataset from start to end: `PollingIngestJob` (one
  iteration), `TransformJob`, `SyncFromJob`, `SyncToJob`, `CompactionJob` (also reset to metadata),
  `ResetJob` and `VerifyJob` (traits in
  [`kamu-core/src/dataset_jobs/`](../../src/domain/core/src/dataset_jobs/)).
  A job authorizes its principal through `DatasetOperationGuard`, plans in a short transaction,
  executes with none open, and commits in another short one, re-resolving the dataset by its ID
  each time. Jobs own these transactions; nothing above them opens one around an operation
  ([database-transactions.md](database-transactions.md#31-who-owns-a-transaction)).
- **Use cases** that cover several operations (`PullDatasetUseCase`, `PushDatasetUseCase`) plan
  over many datasets, run the jobs in order and report per-dataset results. `kamu system compact`,
  `kamu reset` and `kamu verify` run their one job directly. `PushIngestDataUseCase` is not a job:
  it runs in the request's or the command's transaction ([push-ingest.md](push-ingest.md)).
- **Task runners** map a task definition to a job and the job's result to a task outcome. They run
  as `Principal::Node` ([§5](#5-who-runs-it)).

---

## 2. Plan, execute, commit

Sequence diagrams in these pages shade a database transaction; a call outside a shaded box holds
no transaction. An ingest iteration of `kamu pull`:

```mermaid
sequenceDiagram
    participant J as PollingIngestJob
    participant G as DatasetOperationGuard
    participant P as PollingIngestPlanner
    participant I as PollingIngestService
    participant C as DatasetReferenceUpdater

    rect rgba(100, 150, 255, 0.15)
    J->>G: check PollingIngest for the principal
    J->>P: plan at current HEAD, resolve env vars
    P-->>J: plan, dataset detached
    end
    J->>I: ingest: fetch, read, stage, append blocks
    I-->>J: Updated { old_head, new_head }
    rect rgba(100, 150, 255, 0.15)
    J->>C: re-resolve, CAS HEAD old_head → new_head
    end
```

| Step | Transaction | Reads or writes |
| --- | --- | --- |
| **Plan** | a short one | reads metadata at the current `HEAD`; the result names that head as the expected one |
| **Execute** | none; the datasets are detached | writes data files, checkpoints and blocks chained onto the expected head, with `update_block_ref: false` |
| **Commit** | a new short one | moves `HEAD` from the expected head to the last new block |

A dataset detached from its transaction can still be read, but moving its `HEAD` panics, so every
commit re-resolves the dataset first.

How each operation fills in the steps:

| Operation | Job | Plan | Execute (writes blocks) | Commit |
| --- | --- | --- | --- | --- |
| Polling ingest | `PollingIngestJob` | `PollingIngestPlanner`, env vars | `PollingIngestService::ingest`, one iteration | `DatasetReferenceUpdater` |
| Transform | `TransformJob` | `TransformPlanner::elaborate_transform` | `TransformExecutor` | `DatasetReferenceUpdater` |
| Sync into a local dataset | `SyncFromJob` | reads the destination's head | `SyncService::sync_from` | `DatasetReferenceUpdater`, or `CreateDatasetUseCase` for a new dataset |
| Hard compaction, reset to metadata | `CompactionJob` | `CompactionPlanner` | `CompactionExecutor` | `DatasetReferenceUpdater` |
| Reset to a block | `ResetJob` | `ResetPlanner` | — (the block already exists) | `ResetExecutor`, in the same transaction as the plan |
| Push ingest | — | `PushIngestPlanner` | the push ingest executor | in the same transaction as the plan: the request's or the command's |

Reset and push ingest run in one transaction from plan to commit. `SyncToJob` and `VerifyJob`
change no local `HEAD`: they authorize and resolve in one transaction and then run with none open.
The task-side view of the same table, with the outcome of each commit, is in
[task-system.md](task-system.md#who-moves-head).

---

## 3. Committing `HEAD`

`DatasetReferenceUpdater::update_reference(target, block_ref, expected_block_hash, new_block_hash)`
is the single way to move a dataset's reference; the `reference_writes` lint in `kamu-repo-tools`
rejects direct reference writes elsewhere. It runs in the caller's transaction, and:

- fails with `CASFailed` when `block_ref` no longer points at `expected_block_hash`, committing
  nothing;
- fails with `BlockNotFound` when `new_block_hash` is not in the dataset's block store;
- with `expected_block_hash = None`, requires the reference not to exist yet.

The compare-and-swap belongs to ODF itself: `MetadataChain::set_ref` takes the expected block hash
as an argument, and an append that moves a reference expects the block's previous block, so the
chain API has no unconditional write. The updater is still the single entry point, for audit and
for injecting races in tests (`RacingDatasetReferenceUpdater`).

Every layer below the operation takes `block_ref`; the operation picks it. Today every operation
moves `HEAD`, the only reference ODF defines, and passes `odf::BlockRef::Head` where it calls the
update. Errors and task outcomes for a lost race (`RefMoved`) carry the reference that moved.

On a database-backed dataset the move goes through `DatasetReferenceServiceImpl::set_reference`,
which writes the reference row and posts `DatasetReferenceMessage::Updated` in the same
transaction. Its consumers write the storage-level reference, index the new blocks (which feeds the
dependency graph), and update statistics and search ([outbox.md](outbox.md)). Push ingest and the
smart push server also post `DatasetExternallyChangedMessage`, which flows use to tell user pushes
from their own updates.

A job reports a commit that did not happen as a variant of its own error type:

| Commit | Job error | `kamu pull` reports | A task ends |
| --- | --- | --- | --- |
| `HEAD` moved | — (the job's result) | the job's result | with its result |
| Another writer moved `HEAD` since planning | `RefCASFailed` (sync: `SyncError::RefMoved`) | `PullError::RefMoved` | failed, recoverable |
| The dataset is gone | `NotFound` | `NotFound` | failed, unrecoverable |
| The principal may not move `HEAD` | `Access` | `Access` | failed, unrecoverable |

The pull mapping is owned by [dataset-pull.md](dataset-pull.md#62-per-job-runners), the task
mapping by [task-system.md](task-system.md#who-moves-head).

---

## 4. Concurrency

No operation takes a lock. Two writers on one dataset both chain their blocks onto the head they
planned at; the first commit wins, and the second fails its CAS.

```mermaid
sequenceDiagram
    participant A as Writer A
    participant D as Dataset HEAD = h1
    participant B as Writer B
    A->>D: plan at h1
    B->>D: plan at h1
    A->>A: append a2 (parent h1)
    B->>B: append b2 (parent h1)
    A->>D: CAS h1 → a2 ✓
    B->>D: CAS h1 → b2 ✗ RefMoved
    Note over B: b2 stays in storage, unreferenced
```

Consequences shared by every operation:

- **Readers see one operation's blocks together.** `HEAD` moves once, after all of them.
- **A crash before the commit is harmless.** A retry plans again from the current `HEAD`; the
  blocks of the failed attempt are never referenced. Neither `kamu verify` nor `kamu system gc`
  removes them.
- **Only the dataset being written is protected.** Inputs of a transform are read without locks:
  an input that moves after planning is picked up by the next run.
- **Nothing retries in-process.** A lost race is reported; the flow's retry policy runs a new task
  ([task-system.md](task-system.md#8-failure-handling)).

---

## 5. Who runs it

| Entry point | Principal | Where access is checked |
| --- | --- | --- |
| CLI command, GraphQL mutation, REST request | the caller | in the job, through `DatasetOperationGuard`, per operation and dataset; `kamu pull` and `kamu push` also check the whole batch before running any job |
| Task run by a flow | `Principal::Node` | when the flow is configured or triggered, by the GraphQL mutations ([flow-system.md](flow-system.md#12-graphql-api)); the job's own check always passes for the node |

Why background work runs as the node, and how its catalog is built, is owned by
[identity.md](identity.md#background-work-runs-as-the-node). The permission each operation needs is
in its page's authorization or entry-point section.

---

## Glossary

| Term | Meaning |
| --- | --- |
| Expected head | The `HEAD` an operation planned at; its commit succeeds only while `HEAD` still points there |
| CAS | Compare-and-swap: move `HEAD` only if it still equals the expected head |
| Detached | A dataset handle released from the transaction it was resolved in, so slow work can read it with no transaction open |
| Iteration | One ingest run of a polling source; a source with more data reports `has_more` ([polling-ingest.md](polling-ingest.md#7-iterating-has_more)) |
| Depth | A pull's ordering level: sources first, each derivative one deeper than its deepest input ([dataset-pull.md](dataset-pull.md#5-planning)) |
| Elaborate | Turn a transform plan into a concrete request: which input slices and watermarks to consume ([derived-dataset-transform.md](derived-dataset-transform.md#42-elaborate)) |
| Savepoint | A fetch result kept on the local disk, so a retry on the same node skips the download ([polling-ingest.md](polling-ingest.md#6-savepoints)) |
| History rewrite | A reset, compaction or forced sync that leaves `HEAD` on a block that is not a descendant of the old one ([dataset-reset.md](dataset-reset.md#7-after-a-history-rewrite)) |
