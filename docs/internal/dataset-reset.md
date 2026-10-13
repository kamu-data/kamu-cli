# Dataset Reset — Architecture

> **Status:** in production; covers resetting a dataset's `HEAD` to an earlier block (by default its
> `Seed`) and resetting a dataset to metadata only, and what every history rewrite — reset,
> compaction, forced sync — does to the rest of the system. Behaviour that surprises newcomers is
> listed in [§9](#9-testing--gotchas).
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** Kamu has two unrelated mechanisms that are both called "reset".
**Reset to a block** moves `HEAD` to a block that already exists in the dataset's block store, by
default the `Seed`; nothing is rewritten. Usually the target is an ancestor and the chain simply
ends earlier, but nothing checks that, so it can also select another branch
([§5](#5-reset-to-a-block)). **Reset to metadata** keeps the dataset's
definition and drops its data: it runs the hard compaction services with `keep_metadata_only`,
which rebuild the chain on the original `Seed` from every metadata event except `AddData` and
`ExecuteTransform`. A derivative reset to metadata recomputes from its inputs' first blocks; this
is how derivatives recover after an input's history was rewritten. Neither deletes anything:
abandoned blocks and data stay in storage. Every history rewrite moves `HEAD` to a block that is
not a descendant of the old one, and the read models that follow `HEAD` (block index, statistics,
search, dependency graph) detect that and rebuild; this page owns that behaviour for all rewrites.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Know which entry point does what, and who may call it | [§4 Entry points](#4-entry-points) |
| Understand reset to a block | [§5 Reset to a block](#5-reset-to-a-block) |
| Understand what reset to metadata keeps | [§6 Reset to metadata](#6-reset-to-metadata) |
| Know what any history rewrite does to indexes, statistics and caches | [§7 After a history rewrite](#7-after-a-history-rewrite) |
| Find the file for X | [§10 Reference map](#10-filecrate-reference-map) |

---

## Table of contents

- [1. Purpose \& scope](#1-purpose--scope)
- [2. ODF concepts](#2-odf-concepts)
- [3. Layers and crates](#3-layers-and-crates)
- [4. Entry points](#4-entry-points)
- [5. Reset to a block](#5-reset-to-a-block)
- [6. Reset to metadata](#6-reset-to-metadata)
- [7. After a history rewrite](#7-after-a-history-rewrite)
- [8. Errors and results](#8-errors-and-results)
- [9. Testing \& gotchas](#9-testing--gotchas)
- [10. File/crate reference map](#10-filecrate-reference-map)

---

## 1. Purpose & scope

This page covers the reset planner and executor, `ResetDatasetUseCase`, `kamu reset`, the
dataset-side semantics of reset to metadata, and the consequences of any `HEAD` move that rewrites
history.

Out of scope, each with its own owner:

| Topic | Owner |
| --- | --- |
| The compaction planner and executor that reset to metadata runs | [dataset-hard-compaction.md](dataset-hard-compaction.md#5-planning) |
| How the reset and reset-to-metadata tasks are planned and run, their errors and results | [task-system.md](task-system.md#72-hard-compaction-and-reset-to-metadata), [task-system.md](task-system.md#73-reset) |
| Reset flow types, their configuration, GraphQL mutations, and breaking-change cascades | [flow-system.md](flow-system.md#10-flow-types), [flow-system.md](flow-system.md#12-graphql-api), [flow-system.md](flow-system.md#8-sensors-and-propagation) |
| How a derivative detects a diverged input and when it resets itself | [derived-dataset-transform.md](derived-dataset-transform.md#7-diverged-inputs) |
| Divergence and `force` in push and pull | [dataset-sync.md](dataset-sync.md#11-committing-validation-and-errors) |
| How `DatasetReferenceMessage` is delivered | [outbox.md](outbox.md) |

---

## 2. ODF concepts

The [Open Data Fabric specification](https://github.com/open-data-fabric/open-data-fabric) has no
notion of resetting a dataset: the metadata chain is append-only, and `HEAD` is a reference to its
latest block. The ideas reset relies on:

| Topic | Spec |
| --- | --- |
| The chain, its references and the `Seed` | [Metadata Chain](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#metadata-chain), [Seed](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#seed) |
| Why history is kept, and the sanctioned alternative to rewriting it | [Requirements](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#requirements), [Retractions and Corrections](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#retractions-and-corrections) |
| Why a derivative can be rebuilt from its inputs, which is what reset to metadata relies on | [Derivative Data Transience](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#derivative-data-transience) |
| What a derivative records about consumed inputs | [ExecuteTransform](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#executetransform) |

---

## 3. Layers and crates

| Layer | Crate / path | Contents |
| --- | --- | --- |
| Domain interfaces | `kamu-core` — [`src/domain/core/src/services/reset/`](../../src/domain/core/src/services/reset) | `ResetPlanner`, `ResetExecutor`, `ResetPlan`, `ResetResult`, errors |
| Use case | `kamu` — [`reset_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/reset_dataset_use_case_impl.rs) | `ResetDatasetUseCaseImpl`: authorization, plan, execute |
| Services | `kamu` — [`src/infra/core/src/services/reset/`](../../src/infra/core/src/services/reset) | `ResetPlannerImpl`, `ResetExecutorImpl` |
| Reset to metadata | `kamu` — compaction services | see [dataset-hard-compaction.md](dataset-hard-compaction.md#3-layers-and-crates) |
| CLI | `kamu-cli` — [`reset_command.rs`](../../src/app/cli/src/commands/reset_command.rs) | `kamu reset` |
| Task adapter | `kamu-adapter-task-dataset` | `ResetDatasetTaskPlanner` / `ResetDatasetTaskRunner`, `ResetToMetadataDatasetTaskPlanner` / `ResetDatasetToMetadataTaskRunner` |
| Flow adapter | `kamu-adapter-flow-dataset` | `FlowControllerReset`, `FlowControllerResetToMetadata`, `FlowConfigRuleReset`, `DerivedDatasetFlowSensor` |
| Read models that follow `HEAD` | `kamu-datasets-services` | `DatasetBlockUpdateHandler`, `DatasetStatisticsUpdateHandler`, `DatasetSearchUpdater`, `DependencyGraphImmediateListener` |

---

## 4. Entry points

| Entry point | Mechanism | Authorization | `HEAD` swap |
| --- | --- | --- | --- |
| `kamu reset <dataset> <hash>` | reset to a block, via `ResetDatasetUseCase` | Maintain, in the use case | none, see [§5](#5-reset-to-a-block) |
| GraphQL `runs.triggerResetFlow` | reset to a block (custom hash or `toSeed`), via the reset task | Maintain, checked by the mutation; the task runs as the system | none |
| `kamu system compact --hard --keep-metadata-only <pattern>…` | reset to metadata, via `CompactDatasetUseCase` | Maintain, in the use case | CAS on the planned head |
| GraphQL `runs.triggerResetToMetadataFlow` | reset to metadata, via the reset-to-metadata task | Maintain, checked by the mutation | CAS on the planned head |
| `DerivedDatasetFlowSensor` with the `Recover` rule | reset to metadata of a derivative whose input broke | the downstream owner's opt-in; runs as the system | CAS on the planned head |
| `kamu pull --reset-derivatives-on-diverged-input` | reset to metadata during transform elaboration | Write, as for any pull job | CAS on the planned head |

Reset flows run only when triggered, or (reset to metadata) when a sensor recovers from a breaking
change. The GraphQL preconditions, configuration and trigger rules are owned by
[flow-system.md](flow-system.md#12-graphql-api); the pull flag by
[derived-dataset-transform.md](derived-dataset-transform.md#7-diverged-inputs).

**`kamu reset`.** Takes a dataset and a block hash, both required: the CLI cannot reset to the
`Seed` without its hash. It asks for confirmation, calls the use case with no expected old head,
and prints "Dataset was reset". It runs in the command's transaction and processes the outbox
afterwards.

---

## 5. Reset to a block

**Plan.** `ResetPlannerImpl::plan_reset` takes an optional new head and an optional expected old
head:

- the new head defaults to the `Seed`, found with `SearchSeedVisitor`;
- the current `HEAD` is read; if an expected old head is given and differs, planning fails with
  `OldHeadMismatch`;
- the result is `ResetPlan { old_head: current head, new_head }`.

The planner does not check that the new head is an ancestor of the current one.

**Execute.** `ResetExecutorImpl::execute` calls `set_ref(Head, new_head)` with
`validate_block_present: true` and `check_ref_is: None`. "Present" means the block exists in the
dataset's block store, which also holds blocks no longer reachable from `HEAD` (from an earlier
reset, compaction, or an attempt that lost its `HEAD` swap). With no expected value, the
database-backed reference repository uses the dataset's cached reference as the expected head. In
the CLI the planner and executor share one resolved dataset, so that is the head the planner read
and a concurrent move fails as `CASFailed`; the reset task re-resolves the dataset in a new
transaction, reads the current head, and overwrites a move that landed after planning without an
error ([task-system.md](task-system.md#73-reset)).

Nothing else changes: no block is written, and data, checkpoints and blocks after the new head
stay in storage. The dataset's watermark, offsets, checkpoint and source state are whatever the
target block's history recorded, so the next ingest or transform continues from there.

---

## 6. Reset to metadata

Reset to metadata runs `CompactionPlanner::plan_compaction` with `keep_metadata_only: true` and
`CompactionExecutor::execute`; the compaction page owns the chain walk and the rebuild
([dataset-hard-compaction.md](dataset-hard-compaction.md#5-planning)). With the flag set:

| Event | Kept |
| --- | --- |
| `Seed` | yes, the same block: the new chain is built on it |
| `AddData` | no, with its data, checkpoint, watermark and source state |
| `ExecuteTransform` | no |
| Every other event (`SetDataSchema`, `SetTransform`, `SetPollingSource`, `AddPushSource`, `SetVocab`, `SetInfo`, `SetLicense`, `SetAttachments`, `Disable*`) | yes, re-committed in the original order, superseded ones included |

The kind check is skipped, so root datasets and derivatives both qualify. A dataset with no
`AddData` or `ExecuteTransform` yields `NothingToDo`.

What the next run sees:

| Dataset | Next run |
| --- | --- |
| Root | ingest starts at offset 0 with no watermark and no source state, so a polling source fetches as if for the first time (subject to savepoints, [§7](#7-after-a-history-rewrite)) |
| Derivative | the transform starts over from its inputs ([derived-dataset-transform.md](derived-dataset-transform.md#7-diverged-inputs)) |

`SetTransform` survives, so a derivative's dependencies do not change.

---

## 7. After a history rewrite

A compaction, a reset to metadata, a forced sync, and a reset to any block that is not a descendant
of the current head move `HEAD` off the previous head's line. On database-backed datasets every
`HEAD` move goes through `DatasetReferenceServiceImpl::set_reference`, which posts
`DatasetReferenceMessage::Updated` with the previous and new heads in the same transaction. The
consumers handle a rewrite as follows:

| Consumer | On a rewrite |
| --- | --- |
| Storage-level reference file | written from the message like any other update |
| `DatasetBlockUpdateHandler` (key and data block index) | walking from the new head to the previous one ends in an invalid interval; it sets `divergence_detected`, deletes the dataset's indexed blocks, re-saves the chain from the new head to the `Seed`, and posts `DatasetKeyBlocksMessage` with the divergence flag |
| Dependency graph | recomputes upstreams from the key blocks: the latest `SetTransform` replaces the edges, and a chain with no `SetTransform` (a derivative reset to its `Seed`) drops them |
| `DatasetStatisticsUpdateHandler` | the walk reaches the `Seed`, so the statistics are recomputed instead of incremented |
| `DatasetSearchUpdater` | an invalid interval triggers a full reindex of the dataset; otherwise only the new interval is indexed |
| Flows | a flow-driven reset or compaction reports `Breaking` through its controller; a forced smart-protocol push through `FlowDatasetsEventBridge` ([flow-system.md](flow-system.md#8-sensors-and-propagation)). `kamu reset` and `kamu system compact` notify no flow |
| Derivatives | their next transform finds the consumed block missing from the input's history ([derived-dataset-transform.md](derived-dataset-transform.md#7-diverged-inputs)) |
| Remote copies | push and pull refuse to continue, each protocol reporting it its own way; after a reset to an ancestor, a pull from an unchanged remote fast-forwards back; `--force` overwrites ([dataset-sync.md](dataset-sync.md#11-committing-validation-and-errors)) |
| Polling ingest savepoints | not touched. Savepoints are keyed by fetch step and previous source state, so rewinding the source state can make an older savepoint match again, and the next ingest resumes from it; when a savepoint is ignored is owned by [root-dataset-ingest.md](root-dataset-ingest.md#53-savepoints) |

---

## 8. Errors and results

| Error | Raised by | Meaning |
| --- | --- | --- |
| `ResetError::NotFound`, `Access` | use case | the dataset is missing, or the caller lacks Maintain |
| `ResetPlanningError::OldHeadMismatch` | planner | the head changed since the caller looked |
| `ResetExecutionError::SetReferenceFailed(BlockNotFound)` | executor | the target block is not in the dataset's block store |
| `ResetExecutionError::SetReferenceFailed(…)`, `Internal` | executor | storage or database failure, or a lost compare-and-swap (`CASFailed`) |

`ResetResult { old_head, new_head }` reports the head the planner read and the new head. The CLI
prints errors as failures; task and GraphQL mappings are owned by
[task-system.md](task-system.md#73-reset). Reset to metadata returns `CompactionResult`
([dataset-hard-compaction.md](dataset-hard-compaction.md#6-execution)).

---

## 9. Testing & gotchas

### Testing

| What | Where |
| --- | --- |
| Planner and executor: drop the last block, reset to the current head, unknown block, old-head mismatch, default `Seed` | [`test_reset_service_impl.rs`](../../src/infra/core/tests/tests/test_reset_service_impl.rs) |
| Use case: authorization | [`test_reset_dataset_use_case.rs`](../../src/infra/core/tests/tests/use_cases/test_reset_dataset_use_case.rs) |
| Metadata-only compaction; transform status after an input reset | `test_compaction_services_impl.rs` (`test_dataset_keep_metadata_only_compact`), `test_transform_services_impl.rs` |
| Flow controllers, sensor recovery, flow agent | `test_flow_controller_reset.rs`, `test_flow_controller_reset_to_metadata.rs`, `test_derived_dataset_flow_sensor.rs`, `test_flow_agent_impl.rs` |
| GraphQL triggers | `test_gql_dataset_flow_runs.rs` |
| CLI end to end, including a dependency drop on reset to `Seed`; pull with derivative reset; flow recovery | `src/e2e/app/cli/repo-tests/src/commands/test_reset_command.rs`, `test_pull_command.rs`, `src/e2e/app/cli/repo-tests/src/test_flow.rs` |

### Behaviour by design

| Behaviour | Why |
| --- | --- |
| Nothing is deleted by either reset | Readers holding the old head keep working, and a reset can be undone by hash. The `kamu reset` help text says data is deleted; it is not |
| Reset to metadata keeps superseded metadata events | It drops data events only; flattening metadata is a separate concern |
| Reset to a block writes no block | The chain already contains the target; moving the reference is enough |
| A rewrite is detected by consumers, not announced | The reference consumers already follow `DatasetReferenceMessage`, and an invalid interval between the two heads is their signal; the dependency graph follows the `DatasetKeyBlocksMessage` the block index posts and reacts to the `Seed` reappearing in the interval |

---

## 10. File/crate reference map

| Concern | File |
| --- | --- |
| Plan, result, errors | [`reset_planner.rs`](../../src/domain/core/src/services/reset/reset_planner.rs), [`reset_executor.rs`](../../src/domain/core/src/services/reset/reset_executor.rs) |
| Planner and executor | [`reset_planner_impl.rs`](../../src/infra/core/src/services/reset/reset_planner_impl.rs), [`reset_executor_impl.rs`](../../src/infra/core/src/services/reset/reset_executor_impl.rs) |
| Use case | [`reset_dataset_use_case.rs`](../../src/domain/core/src/use_cases/reset_dataset_use_case.rs), [`reset_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/reset_dataset_use_case_impl.rs) |
| CLI | [`reset_command.rs`](../../src/app/cli/src/commands/reset_command.rs), `Reset` in [`cli.rs`](../../src/app/cli/src/cli.rs) |
| Task planners and runners | [`reset_dataset_task_planner.rs`](../../src/adapter/task-dataset/src/planners/reset_dataset_task_planner.rs), [`reset_dataset_task_runner.rs`](../../src/adapter/task-dataset/src/runners/reset_dataset_task_runner.rs), [`reset_to_metadata_dataset_task_planner.rs`](../../src/adapter/task-dataset/src/planners/reset_to_metadata_dataset_task_planner.rs), [`reset_dataset_to_metadata_runner.rs`](../../src/adapter/task-dataset/src/runners/reset_dataset_to_metadata_runner.rs) |
| Flow controllers | [`flow_controller_reset.rs`](../../src/adapter/flow-dataset/src/flow_controllers/flow_controller_reset.rs), [`flow_controller_reset_to_metadata.rs`](../../src/adapter/flow-dataset/src/flow_controllers/flow_controller_reset_to_metadata.rs) |
| Block index, statistics, search, dependency graph after a rewrite | [`dataset_block_update_handler.rs`](../../src/domain/datasets/services/src/services/blocks/dataset_block_update_handler.rs), [`dataset_statistics_update_handler.rs`](../../src/domain/datasets/services/src/services/statistics/dataset_statistics_update_handler.rs), [`dataset_statistics_helper.rs`](../../src/domain/datasets/services/src/services/statistics/dataset_statistics_helper.rs), [`dataset_search_updater.rs`](../../src/domain/datasets/services/src/search/dataset_search_updater.rs), [`dataset_search_indexer.rs`](../../src/domain/datasets/services/src/search/dataset_search_indexer.rs), [`dependency_graph_immediate_listener.rs`](../../src/domain/datasets/services/src/services/graph/dependency_graph_immediate_listener.rs) |
| Dependency extraction | [`dependency_extraction_helper.rs`](../../src/domain/datasets/services/src/services/graph/dependency_extraction_helper.rs) |
