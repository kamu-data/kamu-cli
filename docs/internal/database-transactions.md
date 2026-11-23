# Database transactions

How a database transaction opens, what takes part in it, how it ends, and where in an algorithm
it belongs. Repositories and SQL are in the `kamu-sqlx-database-work` skill; the dataset-specific
rules (plan, execute, commit) are in [dataset-operations.md](dataset-operations.md).

## At a glance

- **A transaction is a catalog.** Opening one chains a catalog on the caller's and registers a
  `TransactionRef` in it. Every component resolved from that catalog shares the transaction;
  nothing resolved elsewhere does.
- **The database is reached lazily.** The connection is taken from the pool on the first query, so
  a transaction that never queries holds no connection.
- **It ends explicitly.** `Ok` commits and `Err` rolls back. A transaction dropped without either
  is rolled back by the database and logged as `Transaction DROPPED`.
- **No slow work inside.** Fetching, engines, transfers, HTTP calls and waits run between short
  transactions, never inside one ([dataset operations](dataset-operations.md#at-a-glance)).
- **Three levels own transactions:** the request or command boundary, the job of one dataset
  operation, and the agents. Orchestration and domain services never open one around an operation
  ([§3.1](#31-who-owns-a-transaction)).
- **The method that orders the work shows the boundaries.** An algorithm with several
  transactions reads as a sequence of `transaction!` blocks ([§3.2](#32-inside-a-level)).

## 1. Machinery

| Piece | Role |
|---|---|
| `TransactionRef` / `TransactionRefT<DB>` | Shared handle to one `sqlx::Transaction`, injected into repositories. Begins the transaction on first use |
| `DatabaseTransactionManager` | Makes a `TransactionRef` and commits or rolls it back. Registered by the database plugin |
| `TransactionScope` | One open transaction: `begin` chains the catalog, `catalog` and `resolve` reach it, `finish` commits or rolls back |
| `DatabaseTransactionRunner` | Callback form over `TransactionScope`, used by test helpers and the database test macros |

| Manager | Used with | Commit and rollback |
|---|---|---|
| `PostgresTransactionManager`, `SqliteTransactionManager` | the database plugins | `COMMIT` / `ROLLBACK` on the connection, if the transaction began |
| `NoOpTransactionManager` | in-memory repositories (no database) | nothing to do; in-memory changes apply at once |
| `InMemoryTransactionManager` | tests that opt into rollback | applies the working sets of participating repositories (`InMemoryTransactionalMap`) in all or none ([AGENTS.md](../../AGENTS.md#in-memory-repositories-review-context)) |

**Ending needs the only reference.** The database managers take the `sqlx::Transaction` out of the
`TransactionRef` to commit it, and panic when anything else still holds the reference: a component
resolved inside the transaction that outlived it, such as a singleton that captured a repository
([kamu-dill-di](../../.claude/skills/kamu-dill-di/SKILL.md#singletons-must-not-depend-on-repositories)).
`TransactionScope::finish` drops its own catalog first. Values that leave a transaction and keep a
dataset are detached from it first (`detach_from_transaction`), as the
[dataset operations](dataset-operations.md#2-plan-execute-commit) describe.

## 2. Opening a transaction in code

| Form | Opens | Use for |
|---|---|---|
| `transaction!(\|a: Arc<dyn A>\| { .. })` | a block inside a method | each step of a method that orders several transactions, or one transaction among other work |
| `#[transactional_method1..4(a: Arc<dyn A>, ..)]`, or `#[transactional_method]` for the bare `transaction_catalog` | the whole method, from `self.catalog` | a method that is itself one transactional step |
| `#[transactional_handler]` | the whole axum handler, from its `Extension(catalog)` | HTTP handlers |
| `DatabaseTransactionRunner::transactional(\|catalog\| ..)` | a callback | test helpers and generated code; production code uses a macro |

All of them resolve what they declare from the transaction catalog, and need the error type to
implement `From<InternalError>`, the error of opening or ending a transaction.

## 3. Where a transaction opens

### 3.1 Who owns a transaction

Transactions are opened at three levels only. Everything else runs in the caller's transaction.

| Level | Opens | Never |
|---|---|---|
| **Boundary**: HTTP and GraphQL middleware, a transactional CLI command | one transaction for request-sized work | slow work: engines, fetching, transfers |
| **Operation job**: one per dataset operation (`CompactionJob`, `TransformJob`, …) | the short plan and commit transactions of one operation on one dataset | a transaction held across the execution |
| **Agent**: task, flow, outbox, webhooks | bookkeeping transactions around its own state | — |

- **Orchestration** opens no transaction around an operation; it calls jobs. This covers dependency
  traversal, repeated ingest iterations, recovery policies, and work over several datasets. It opens a
  block only for its own planning reads, such as the pull plan.
- **Domain services** (planners, executors, a dataset's `MetadataChain`, repositories) always run in the
  caller's transaction.

A job is the same code for the CLI and for flows: legacy task runners only map a task definition to
a job and the job's result to a task outcome. Long-running work is headed for the task system. Task
workers will take the jobs' place for flows, and CLI commands will submit tasks and wait for them
rather than call jobs, which makes them work the same against a remote node
([IaC task system](https://github.com/kamu-data/kamu-node/issues/348)).

### 3.2 Inside a level

| Shape | Write | Precedent |
|---|---|---|
| A method running several transactions in sequence, or slow work next to one | One `transaction!` block per transaction in that method; each block stays short and calls step methods | `CompactionJobImpl::compact_and_commit`, `WebhookDeliveryWorkerImpl::deliver_webhook` |
| A method that is itself one transactional step | `#[transactional_methodN]` on it | the task planners, `PushCommand::get_filtered_datasets` |
| A private method annotated only so that its caller gets a transaction | Neither: the caller opens the block, and the method takes the components as parameters | `PushDatasetUseCaseImpl::make_authorization_checks` |
| A transaction only in some configurations | `#[when(<bool>)]` on the block | `run_startup_jobs`, the command run in `app.rs` |

The blocks are the algorithm: a reader sees each transaction where the method runs it.
Deduplication stops at a block's edge. A helper may run inside a block, but never opens one of the
method's transactions or folds several into one call, even when the same block repeats across
jobs.

Step methods take components as `&dyn Iface`. A domain function that needs a `'static` trait object
(an aggregate's `load` from an event store) takes `&Arc<dyn Iface>` instead.

## 4. `transaction!`

```rust
// 1: Plan
let (target, plan) = transaction!(|registry: Arc<dyn DatasetRegistry>,
                                   planner: Arc<dyn CompactionPlanner>| {
    let target = registry.get_dataset_by_handle(handle).await;
    let plan = planner.plan_compaction(target.clone(), options, None).await?;
    target.detach_from_transaction();
    (target, plan)
});

// 2: Merge, with no transaction open
let result = self.compaction_executor.execute(target, plan, None).await?;

// 3: Commit, handling the error here
let committed = transaction!(
    |dataset_registry: Arc<dyn DatasetRegistry>| -> Result<_, odf::dataset::SetChainRefError> {
        let target = dataset_registry.get_dataset_by_handle(&handle).await;
        target.as_metadata_chain().set_ref(..).await
    }
);
```

| Aspect | Behaviour |
|---|---|
| Bindings | `name: Arc<dyn Iface>` is resolved from the transaction; a bare `name` is the transaction catalog |
| Without a return type | The block is the body's value. `?` rolls back and leaves the enclosing function, as in straight-line code; `return` is a compile error |
| With `-> Result<T, E>` | The block is the body's `Result`: `Ok` commits, `Err` rolls back. `return` leaves the body, as in a closure |
| Captures | The body borrows the locals it uses and moves only what it consumes, so nothing is cloned for it |
| Catalog | `self.catalog` (a `Catalog` or a singleton's `CatalogWeakRef`), or `#[catalog(expr)]` |
| Conditional | `#[when(bool)]`: when false the body runs on the catalog as given, with no transaction |
| Span | A debug span named `transaction` at the call site, or `#[span(span)]` |

rustfmt formats the closure-shaped body. When a binding does not fit on its line, rustfmt splits
its generic type across lines: name bindings short enough to fit.

## 5. Tests

| Need | Use |
|---|---|
| A repository suite in one transaction per storage | `database_transactional_test!` (`kamu-repository-tests`) |
| Setup or checks committed outside the transactions under test | `database_common::testing::in_committed_transaction` |
| Two overlapping transactions | `database_common::testing::race_transactions` with `database_concurrent_transactions_test!` (`kamu-sqlx-database-work`) |
| Rollback of in-memory repositories | `InMemoryTransactionManager` |

## Rejected approaches

| Approach | Why it was rejected |
|---|---|
| `transaction!(catalog, \|..\| ..)`, the catalog as the first argument | rustfmt overflows the closure onto the catalog's line and splits every binding's generic type, so the catalog is an attribute, `#[catalog(..)]` |
| A `macro_rules!` macro | It cannot reject `return` in a block without a return type, where `return Ok(..)` would silently commit and continue instead of leaving the function |
| Rolling back by dropping the scope, so the body can be plain inline code | Relies on the driver's deferred rollback, skips the in-memory manager's rollback until the next transaction, and logs every rollback as `Transaction DROPPED` |
| An `async move` body | Forces a clone of every local used after the block, as the `DatabaseTransactionRunner` callbacks do |
| A shared helper that opens a block repeated across jobs (a commit helper with the transaction inside) | Hides a step of each job's algorithm behind a call; the repeated block is the price of reading every job top-down ([§3.2](#32-inside-a-level)) |
| A step service that holds one phase of every operation (plan an ingest, elaborate a transform, commit a head, each in its own transaction) | Each operation is then composed twice, once by the CLI use case and once by the task runner, and its transactions spread over the steps, the use case, the task planner and the runner. A job per operation owns them in one place ([§3.1](#31-who-owns-a-transaction)) |
