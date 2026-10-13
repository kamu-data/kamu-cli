# Dataset Pull — Architecture

> **Status:** in production; covers `kamu pull` and the pull planner that the server's update task
> shares. Behaviour that surprises newcomers is listed in [§10](#10-testing--gotchas).
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** "Pull" means *bring these datasets up to date*, whatever that
takes for each one. A root dataset is updated by **ingest** from its polling source, a derivative
by **transform** of its inputs, and any dataset with a remote **pull alias** by **sync** from that
remote. Pull itself does none of this work: it is a planner and a scheduler. The **planner**
resolves the requested references (local names, wildcards, remote refs), optionally walks upstream
dependencies, gives every dataset a **depth** (sources first, each derivative one deeper than its
deepest input) and turns each one into a job: `Ingest`, `Transform` or `Sync`. The **use case**
runs the plan one depth at a time. It checks authorization for the whole depth, runs that depth's
jobs concurrently, moves each dataset's `HEAD` after its job commits, and stops at the first depth
that has an error. The server's update task uses the same planner for one dataset and runs the
single job it gets back.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Know what `kamu pull` flags do | [§4 The CLI command](#4-the-cli-command) |
| Understand how references become a plan | [§5 Planning](#5-planning) |
| See how a plan runs | [§6 Execution](#6-execution) |
| Know who may pull what | [§7 Authorization](#7-authorization) |
| See how the server reuses pull | [§8 Server use](#8-server-use) |
| Find the file for X | [§11 Reference map](#11-filecrate-reference-map) |

---

## Table of contents

- [1. Purpose \& scope](#1-purpose--scope)
- [2. Concepts](#2-concepts)
- [3. Layers and crates](#3-layers-and-crates)
- [4. The CLI command](#4-the-cli-command)
- [5. Planning](#5-planning)
- [6. Execution](#6-execution)
- [7. Authorization](#7-authorization)
- [8. Server use](#8-server-use)
- [9. Results and errors](#9-results-and-errors)
- [10. Testing \& gotchas](#10-testing--gotchas)
- [11. File/crate reference map](#11-filecrate-reference-map)

---

## 1. Purpose & scope

This page covers how a pull request becomes a plan and how the plan runs: reference resolution,
dependency traversal, depth ordering, job selection, authorization, transactions, concurrency,
`HEAD` updates and result reporting.

Out of scope, each with its own owner:

| Topic | Owner |
| --- | --- |
| What an `Ingest` job does: fetch, read, merge, write; `has_more` | [root-dataset-ingest.md](root-dataset-ingest.md) |
| What a `Transform` job does: elaboration, engines, commit, diverged inputs | [derived-dataset-transform.md](derived-dataset-transform.md) |
| What a `Sync` job does: `SyncService`, transfer protocols, remote aliases and repositories | [dataset-sync.md](dataset-sync.md) |
| How the update task is queued, planned and run | [task-system.md](task-system.md) |
| When the server updates a dataset: flows, sensors, triggers | [flow-system.md](flow-system.md) |

---

## 2. Concepts

| Term | Meaning |
| --- | --- |
| `PullRequest` | one thing to pull: `Local(DatasetRef)` or `Remote { remote_ref, maybe_local_alias }` |
| Pull alias | a `RemoteAliasKind::Pull` entry in a dataset's remote aliases, added by `kamu repo alias add --pull` or recorded when the dataset was first synced from a remote ([dataset-sync.md](dataset-sync.md#9-remote-repositories-and-aliases)); it makes the dataset a sync target on every later pull |
| Depth | the dataset's position in dependency order. Sync targets and roots are depth 0; a derivative is one deeper than its deepest input |
| Plan iteration | `PullPlanIteration { depth, jobs }`: every job at one depth. Jobs within an iteration are independent |
| Job | `PullPlanIterationJob`: `Ingest(PullIngestItem)`, `Transform(PullTransformItem)` or `Sync(PullSyncItem)`, each carrying what its service needs to start |
| Explicit vs implicit item | an item the caller asked for carries its `maybe_original_request`; an upstream added by `--recursive` does not |

---

## 3. Layers and crates

| Layer | Crate / path | Contents |
| --- | --- | --- |
| Domain types | `kamu-core` — [`pull_request_planner.rs`](../../src/domain/core/src/services/pull_request_planner.rs) | `PullRequestPlanner`, `PullRequest`, `PullOptions`, plan and job types, `PullResponse`, `PullResult`, `PullError`, listeners |
| Use case trait | `kamu-core` — [`pull_dataset_use_case.rs`](../../src/domain/core/src/use_cases/pull_dataset_use_case.rs) | `PullDatasetUseCase::{execute, execute_multi, execute_all_owned}` |
| Planner | `kamu` — [`pull_request_planner_impl.rs`](../../src/infra/core/src/services/pull_request_planner_impl.rs) | `PullRequestPlannerImpl`, `PullGraphDepthFirstTraversal` |
| Use case | `kamu` — [`pull_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/pull_dataset_use_case_impl.rs) | `PullDatasetUseCaseImpl`: authorization, iteration loop, per-job runners, `HEAD` updates |
| CLI | `kamu-cli` — [`pull_command.rs`](../../src/app/cli/src/commands/pull_command.rs) | `PullCommand`, progress listeners, result summary |
| Server | `kamu-adapter-task-dataset` | `UpdateDatasetTaskPlanner` calls the planner; `UpdateDatasetTaskRunner` runs the job ([task-system.md](task-system.md)) |

---

## 4. The CLI command

`kamu pull` is dispatched in [`cli_commands.rs`](../../src/app/cli/src/cli_commands.rs): with
`--set-watermark` it becomes `SetWatermarkCommand` and nothing below applies; otherwise it builds
`PullCommand`. `PullCommand` runs without an outer transaction (the use case opens its own short
ones), and the CLI processes outbox messages after it succeeds.

| Flag | Effect |
| --- | --- |
| `<dataset>…` | local or remote references, or wildcard patterns. Patterns are expanded by `filter_datasets_by_any_pattern` (local datasets, or remote repositories through `SearchServiceRemote`); each result becomes a `PullRequest` via `PullRequest::from_any_ref` |
| `--all` | every dataset owned by the current account (`execute_all_owned`); cannot be combined with references or `--recursive` |
| `--recursive` | also pull every transitive upstream dependency |
| `--as <name>` | exactly one remote reference, synced into a local dataset with this name (`PullRequest::Remote` with a local alias); only `--force`, `--visibility` and `--no-alias` apply |
| `--fetch-uncacheable` | `PollingIngestOptions::fetch_uncacheable` |
| `--no-alias` | `add_aliases: false`: a dataset created by sync gets no pull alias |
| `--force` | `SyncOptions::force` |
| `--visibility` | visibility of datasets created by sync |
| `--reset-derivatives-on-diverged-input` | `TransformOptions::reset_derivatives_on_diverged_input` ([derived-dataset-transform.md](derived-dataset-transform.md#7-diverged-inputs)) |

The command always sets `exhaust_sources: true`, so a polling source is ingested until it has no
more data. It leaves `dataset_env_vars` empty; the use case resolves them per dataset
([§6.2](#62-per-job-runners)).

Reference interpretation depends on tenancy: in a single-tenant workspace a repo-like reference
(`repo/name`) is a remote repository; in a multi-tenant workspace it is `account/name`, and
repositories need an explicit URL form.

On a TTY at default verbosity, `PrettyPullProgress` shows per-dataset progress bars. Afterwards
the command prints how many datasets were updated and up to date, and returns a `BatchError`
listing each failure. A not-enough-permissions failure is shown as "not found"
(`sanitize_pull_error`), so the CLI does not reveal that a dataset exists; other access errors are
shown as they are.

---

## 5. Planning

`PullRequestPlannerImpl::build_pull_multi_plan(requests, options, tenancy)` runs in one
transaction (`PullDatasetUseCaseImpl::build_plan_for_requests`, or
`build_plan_for_all_owned_datasets` for `--all`), and its resolved datasets are
detached from it so later transactions can use them.

### 5.1 Resolving each request

`PullGraphDepthFirstTraversal::traverse_pull_graph` handles one request:

1. **Local handle.** A `Local` ref must resolve, or the result is `NotFound`. A `Remote` request
   first tries its local alias, then searches for a local dataset whose pull aliases contain the
   remote ref (`try_inverse_lookup_dataset_by_pull_alias`: a quick check by name, then a scan of
   all datasets). In a multi-tenant workspace a dataset found this way must belong to the current
   account; in any workspace it must match any alias the caller gave, or the result is
   `SaveUnderDifferentAlias`.
2. **Missing target.** With no local dataset and `create_if_not_exists` off, the request fails.
3. **Local alias.** The existing alias, the caller's alias, or one inferred from the remote ref:
   its dataset name, or for a URL the last path segment, then the host name. Remote refs by ID without
   a local alias are not supported and panic (`unimplemented!`).
4. **Remote ref.** A `Remote` request's own ref; otherwise the dataset's single pull alias. Two or
   more pull aliases fail with `AmbiguousSource`.
5. **Depth.** With a remote ref the item is a depth-0 sync. Otherwise its depth is one more than
   the deepest upstream dependency (`DependencyGraphService::get_upstream_dependencies`, visited
   recursively), so a root is 0.

Items are keyed by local alias, so a dataset reached twice is planned once; if a later visit is
explicit, the item becomes explicit.

### 5.2 Single dataset vs graph

| Case | Traversal | Depth |
| --- | --- | --- |
| One request, not `--recursive` (also every server update task) | `build_single_node_pull_graph`: upstreams are not visited | root 0, derivative 1, sync target 0 |
| Two or more requests, or `--recursive` (`--all` takes the single-node path when the account owns exactly one dataset) | `collect_pull_graph`: upstreams are visited for every request | from the graph |

Without `--recursive`, the visited upstreams are dropped afterwards (only explicit items are
kept), but the depths computed from them stay, so the requested datasets still run in dependency
order.

### 5.3 Turning items into jobs

Items are sorted by depth, then sync targets before local ones, then by alias, and sliced into one
iteration per depth:

| Item | Job | Built by | Holds |
| --- | --- | --- | --- |
| depth 0, no remote ref | `Ingest(PullIngestItem)` | `build_ingest_item` | target and `DataWriterMetadataState` read from `HEAD` |
| depth 0, remote ref | `Sync(PullSyncItem)` | `build_sync_item` → `SyncRequestBuilder` | local target (existing or to create), remote ref, `SyncRequest` |
| depth > 0 | `Transform(PullTransformItem)` | `build_transform_item` → `TransformRequestPlanner::build_transform_preliminary_plan` | target and preliminary transform plan with resolved inputs |

A dataset with a pull alias is always synced, whatever its kind: a derivative pulled from a
remote is copied, never transformed locally.

If any request fails to resolve, or any item fails to become a job, `execute_multi` returns only
those errors and runs nothing.

---

## 6. Execution

### 6.1 The iteration loop

`PullDatasetUseCaseImpl::pull_by_plan` runs the iterations in depth order. For each one it:

1. checks authorization for the whole iteration in one transaction ([§7](#7-authorization)); if
   any job fails, it records those failures and stops — the iteration's other jobs and every later
   iteration do not run;
2. spawns every job of the iteration on a `JoinSet` and waits for all of them;
3. records each job's `PullResponse`; if any job failed, it stops after this iteration.

Jobs of one iteration run concurrently and independently: one job's failure does not cancel the
others. Each job opens its own short transactions, and long work (fetching, engines, transfers)
runs outside them.

### 6.2 Per-job runners

| Job | Runner | Moves `HEAD` |
| --- | --- | --- |
| Ingest | `ingest`: resolves the dataset's env vars if the options carry none and secrets encryption is enabled; then `ingest_loop` calls `PollingIngestService::ingest` until `has_more` is false (with `exhaust_sources`), merging results so the response spans the first old head to the last new head | after every ingest iteration that returns `Updated`, `update_ref_transactionally` (CAS) |
| Transform | `transform`: elaborates in a transaction after `refresh_from_dataset_registry`, so it sees what earlier depths committed; then executes | after the commit, `update_ref_transactionally` (CAS) |
| Sync | `sync`: `SyncService::sync`; when a dataset was created by the sync (`old_head: None`) and `add_aliases` is on, stores the pull alias | by `SyncService` itself |

`update_ref_transactionally` sets `HEAD` with `validate_block_present` and `check_ref_is` set to the
head the job started from, which posts
`DatasetReferenceMessage` ([root-dataset-ingest.md](root-dataset-ingest.md#8-committing-and-concurrency)).
A CAS failure is an `InternalError`: for an ingest job it becomes that dataset's
`PollingIngestError::Internal`, for a transform job it aborts the whole pull
([§9](#9-results-and-errors)).

### 6.3 Listeners

`PullMultiListener` hands out one ingest, transform or sync listener per dataset.
`PullDatasetUseCase::execute` wraps a single-dataset `PullListener` in `ListenerMultiAdapter`.
Listeners only report progress; they cannot change the outcome.

---

## 7. Authorization

`make_authorization_checks` runs per iteration, in one transaction, through
`DatasetActionAuthorizer`:

| Check | Datasets |
| --- | --- |
| Write | every job's written dataset: the ingest or transform target, or a sync target that already exists |
| Read | every transform input (the preliminary plan's resolved datasets other than the target) |

A sync target that does not exist yet has nothing to check; creating it is governed by the sync.
`--all` only plans datasets the current account owns. Planning itself resolves datasets without
access checks; nothing runs before the iteration's checks pass.

---

## 8. Server use

The server never calls `PullDatasetUseCase`: the update task builds a single-node plan with
`build_pull_plan`, which asserts exactly one job or one error, and runs that job itself
([task-system.md](task-system.md#71-update-dataset)).

| | `kamu pull` | Update task |
| --- | --- | --- |
| Datasets per run | many, by depth | one |
| Authorization | per iteration ([§7](#7-authorization)) | none at run time; access is checked when the flow is configured or triggered |
| Polling loop | until `has_more` is false | one iteration; the flow decides whether to continue |
| Env vars | resolved per dataset by the use case | resolved by the task planner |
| Diverged-input reset | optional flag | never |
| Planning to running | immediate | separated by the task queue; the job's plan is from planning time |

---

## 9. Results and errors

Each dataset produces one `PullResponse`: the original request (empty for implicit upstreams),
the local and remote refs if resolved, and `Result<PullResult, PullError>`.

`PullResult` is `UpToDate(kind)` — polling ingest (with `uncacheable`), push ingest, transform or
sync — or `Updated { old_head, new_head, has_more }`. `PullError` wraps the per-job errors
(`PollingIngestError`, `TransformError`, `SyncError`) plus planning errors (`NotFound`,
`AmbiguousSource`, `SaveUnderDifferentAlias`, `InvalidOperation`, `ScanMetadata`), `Access` and
`Internal`.

An `InternalError` returned by a job runner itself (a failed transaction, a transform's `HEAD`
CAS failure, an env-var lookup failure) aborts the whole pull rather than becoming a per-dataset
error. A job that panics re-raises the panic in the command (`JoinSet::join_all`), aborting the
other jobs of its iteration.

Any failed dataset makes the command return an error, and then the CLI skips its final outbox
processing: deferred consumers of messages already posted (for datasets that did update) run on
the next command that processes the outbox.

---

## 10. Testing & gotchas

### Testing

- Planner tests: `src/infra/core/tests/tests/test_pull_request_planner_impl.rs`.
- Use case tests: `src/infra/core/tests/tests/use_cases/` (`test_pull_dataset_use_case.rs`).
- CLI scenarios: `src/e2e/app/cli/repo-tests/src/commands/test_pull_command.rs`.

### Behaviour by design

| Area | What happens |
| --- | --- |
| Stop on error | The first depth with an error is the last one run; later depths, including datasets unrelated to the failure, are not attempted |
| Planning errors | One unresolvable reference fails the whole command before anything runs |
| Pull aliases | A dataset with a pull alias is synced, never ingested or transformed, even when it has a source or a transform |
| Non-recursive multi-pull | Upstreams are traversed for ordering only; they are not pulled |
| Hidden datasets | The CLI reports an unauthorized dataset as not found |

---

## 11. File/crate reference map

| Concern | File |
| --- | --- |
| Planner trait, requests, options, plan, results, errors | [`pull_request_planner.rs`](../../src/domain/core/src/services/pull_request_planner.rs) |
| Use case trait | [`pull_dataset_use_case.rs`](../../src/domain/core/src/use_cases/pull_dataset_use_case.rs) |
| Planner: resolution, traversal, depths, job building | [`pull_request_planner_impl.rs`](../../src/infra/core/src/services/pull_request_planner_impl.rs) |
| Use case: authorization, iteration loop, runners, `HEAD` updates | [`pull_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/pull_dataset_use_case_impl.rs) |
| CLI command, progress, error display | [`pull_command.rs`](../../src/app/cli/src/commands/pull_command.rs) |
| CLI flags and dispatch | [`cli.rs`](../../src/app/cli/src/cli.rs), [`cli_commands.rs`](../../src/app/cli/src/cli_commands.rs) |
| Server update task | [`update_dataset_task_planner.rs`](../../src/adapter/task-dataset/src/planners/update_dataset_task_planner.rs), [`update_dataset_task_runner.rs`](../../src/adapter/task-dataset/src/runners/update_dataset_task_runner.rs) |
