# Dataset Hard Compaction — Architecture

> **Status:** in production for root datasets; covers `kamu system compact --hard`, the hard
> compaction flow, and the compaction planner and executor, which reset to metadata also uses.
> Behaviour that surprises newcomers is listed in [§9](#9-testing--gotchas).
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** A root dataset that receives many small appends ends up with many
small Parquet files, one per `AddData` block that carries data, and queries pay for every file
header. **Hard compaction** rewrites the dataset's history as if the data had arrived in a few
large batches. The **planner** walks the chain from `HEAD` back to the `Seed` and groups runs of
consecutive `AddData` blocks into batches bounded by a maximum file size and record count; every
other metadata event ends a batch and is kept. The **executor** merges each batch's files into one
with embedded DataFusion, then rebuilds the chain on top of the original `Seed`: kept events are
re-committed, and each batch becomes one `AddData` covering the same offset range. The caller then
moves `HEAD` to the new chain with a compare-and-swap. Every block hash after the `Seed` changes,
so derivatives and remote copies that saw the old blocks can no longer continue from them. Nothing
is deleted: the old blocks and files stay in storage.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Know which entry point does what, and who may call it | [§4 Entry points](#4-entry-points) |
| Understand how blocks are grouped | [§5 Planning](#5-planning) |
| Follow the merge and the new chain | [§6 Execution](#6-execution) |
| Know what happens to old files | [§7 Storage](#7-storage) |
| Know what breaks downstream | [§8 After compaction](#8-after-compaction) |
| Find the file for X | [§10 Reference map](#10-filecrate-reference-map) |

---

## Table of contents

- [1. Purpose \& scope](#1-purpose--scope)
- [2. ODF concepts](#2-odf-concepts)
- [3. Layers and crates](#3-layers-and-crates)
- [4. Entry points](#4-entry-points)
- [5. Planning](#5-planning)
- [6. Execution](#6-execution)
- [7. Storage](#7-storage)
- [8. After compaction](#8-after-compaction)
- [9. Testing \& gotchas](#9-testing--gotchas)
- [10. File/crate reference map](#10-filecrate-reference-map)

---

## 1. Purpose & scope

This page covers the compaction planner and executor, `CompactDatasetUseCase`, the
`kamu system compact` command, and the dataset side of the hard compaction flow.

Out of scope, each with its own owner:

| Topic | Owner |
| --- | --- |
| Reset to metadata: the same services with `keep_metadata_only`, what it keeps and who triggers it | [dataset-reset.md](dataset-reset.md#6-reset-to-metadata) |
| What a rewritten `HEAD` does to block indexes, statistics, search and the dependency graph | [dataset-reset.md](dataset-reset.md#7-after-a-history-rewrite) |
| How the hard compaction task is planned and run, and its outcomes | [task-system.md](task-system.md#72-hard-compaction-and-reset-to-metadata) |
| The compaction flow type, its configuration rule and schedules | [flow-system.md](flow-system.md#10-flow-types), [flow-system.md](flow-system.md#12-graphql-api) |
| Breaking-change propagation to downstream flows | [flow-system.md](flow-system.md#8-sensors-and-propagation) |
| How a derivative detects a compacted input and recovers | [derived-dataset-transform.md](derived-dataset-transform.md#7-diverged-inputs) |
| Pushing or pulling a dataset whose history diverged | [dataset-sync.md](dataset-sync.md#11-committing-validation-and-errors) |

---

## 2. ODF concepts

The [Open Data Fabric specification](https://github.com/open-data-fabric/open-data-fabric) defines
compaction only as a declared task with size and record limits; it says nothing about rewriting
history and treats the metadata chain as append-only. The pieces compaction works with:

| Topic | Spec / RFC |
| --- | --- |
| Data slices and the metadata chain | [Data Slice](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#data-slice), [Metadata Chain](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#metadata-chain) |
| `AddData`: offsets, checkpoint, watermark, source state | [AddData](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#adddata), [RFC-001](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/001-record-offsets.md) |
| Why history is kept, and the sanctioned alternative to rewriting it | [Requirements](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#requirements), [Retractions and Corrections](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#retractions-and-corrections) |
| Compaction as a declared task | [CompactionParams](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#compactionparams), [TaskSpec::Compaction](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#taskspeccompaction), [RFC-019 Task Types](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/019-iac-task-flow-system.md#task-types) |
| Linked-object summaries, which compaction adds up | [RFC-017](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/017-large-files-linking.md#impact-on-existing-functionality) |

---

## 3. Layers and crates

| Layer | Crate / path | Contents |
| --- | --- | --- |
| Domain interfaces | `kamu-core` — [`src/domain/core/src/services/compaction/`](../../src/domain/core/src/services/compaction) | `CompactionPlanner`, `CompactionExecutor`, `CompactionOptions`, `CompactionPlan`, `CompactionResult`, listeners, errors |
| Use case | `kamu` — [`compact_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/compact_dataset_use_case_impl.rs) | `CompactDatasetUseCaseImpl`: authorization, plan, execute, move `HEAD` |
| Services | `kamu` — [`src/infra/core/src/services/compaction/`](../../src/infra/core/src/services/compaction) | `CompactionPlannerImpl`, `CompactionExecutorImpl` |
| DataFusion session config | `kamu` — [`engine_config_embedded.rs`](../../src/infra/core/src/engine/engine_config_embedded.rs) | `EngineConfigDatafusionEmbeddedCompaction` |
| CLI | `kamu-cli` — [`compact_command.rs`](../../src/app/cli/src/commands/compact_command.rs) | `kamu system compact` |
| Task adapter | `kamu-adapter-task-dataset` | `HardCompactDatasetTaskPlanner`, `HardCompactDatasetTaskRunner` |
| Flow adapter | `kamu-adapter-flow-dataset` | `FlowControllerCompact`, `FlowConfigRuleCompact` |

---

## 4. Entry points

| Entry point | Path | Authorization | Moves `HEAD` |
| --- | --- | --- | --- |
| `kamu system compact --hard <pattern>…` | `CompactCommand` → `CompactDatasetUseCase::execute_multi` | Maintain, per dataset, in the use case | the use case, CAS on the planned head |
| Compaction flow, manual or scheduled | GraphQL `runs.triggerCompactionFlow` or a schedule set with `triggers.setTrigger` → `FlowControllerCompact` → hard compaction task | Maintain, checked by the GraphQL mutations ([flow-system.md](flow-system.md#12-graphql-api)); the task runs as the system | the task runner, CAS on the planned head |
| Metadata-only, from `kamu pull` or a reset-to-metadata flow | see [dataset-reset.md](dataset-reset.md#6-reset-to-metadata) | | |

**CLI.** `kamu system compact` requires `--hard`; without it the command fails with "Soft
compactions are not yet supported". It expands local patterns, asks for confirmation, optionally
verifies every dataset first (`--verify`, stopping at the first failure), then compacts the
datasets one after another. `--max-slice-size` (bytes, default 300 MB) and `--max-slice-records`
(default 10 000) bound each merged file. `--keep-metadata-only` turns the run into a reset to
metadata. The command runs in a single database transaction, merges included, and processes the
outbox afterwards. If any dataset fails, the command returns an error and the transaction rolls
back the `HEAD` updates of every dataset, including those reported as compacted.

**Use case.** `CompactDatasetUseCaseImpl::execute` resolves the dataset with
`DatasetAction::Maintain`, plans, executes, and on `Success` sets `HEAD` with
`check_ref_is: Some(old_head)` and `validate_block_present`. `execute_multi` calls `execute` per
dataset and returns one `CompactionResponse` each; one failure does not stop the others.

**Flow.** The flow's preconditions, configuration and logical plan are owned by
[flow-system.md](flow-system.md#10-flow-types); how the task plans, runs and moves `HEAD`, by
[task-system.md](task-system.md#72-hard-compaction-and-reset-to-metadata).

---

## 5. Planning

`CompactionPlannerImpl::plan_compaction` rejects a non-root dataset with `InvalidDatasetKind`
unless `keep_metadata_only` is set, fills in the default limits, and walks the whole chain from
`HEAD` to the `Seed`, newest block first. There is no stopping point: an earlier compaction's
output is just more `AddData` blocks.

| Block | Planner action |
| --- | --- |
| `AddData` with data | added to the current batch; if adding it would exceed `max_slice_size` or `max_slice_records`, the current batch is closed first and this slice starts a new one |
| `AddData` without data (watermark or source state only) | contributes its watermark, source state and checkpoint to the current batch's upper bound and is never kept as a block of its own; if the batch ends as a `SingleBlock`, those values are lost |
| `Seed` | recorded as the base of the new chain |
| Any other event (`SetDataSchema`, `SetPollingSource`, `AddPushSource`, `SetVocab`, `SetInfo`, `ExecuteTransform`, …) | closes the current batch and is kept as a `SingleBlock` |

Batches therefore never cross a schema change or any other metadata event. A closed batch becomes:

- a **`CompactedBatch`** when it holds two or more slices: the slice URLs; a **lower bound** from
  the oldest block (`start_offset`, `prev_offset`, `prev_checkpoint`); an **upper bound** from the
  newest (`end_offset`, plus the newest non-empty `new_checkpoint`, `new_source_state` and
  `new_watermark`); and the summed linked-object summaries;
- a **`SingleBlock(hash)`** when it holds one slice: the original block is carried over and the
  upper bound is not used;
- nothing when it is empty.

`CompactionPlan` holds the batches newest first, the `Seed` hash, the old head and block count, and
the offset column name from the oldest `SetVocab` in the chain (the default vocabulary if there is
none). A plan has no effect when the number of batches plus one (the `Seed`) equals the old block
count.

---

## 6. Execution

`CompactionExecutorImpl::execute` returns `NothingToDo` for a plan with no effect. Otherwise:

1. **Merge.** For every `CompactedBatch`, a DataFusion session built from
   `EngineConfigDatafusionEmbeddedCompaction` reads the batch's Parquet files, sorts by the offset
   column, and writes one file into `<run dir>/compaction-<random>/`. No engine container is
   involved. The session config can be overridden under `engine.datafusionEmbedded.compaction` in
   the CLI config.
2. **Rebuild the chain.** Starting from the original `Seed`, oldest batch first, each
   `SingleBlock` event is re-committed unchanged and each `CompactedBatch` becomes one
   `commit_add_data` with the bounds above: the same offset range, the newest checkpoint referenced
   by its existing hash, and the merged file, whose hashes and size are computed on commit. Every
   block gets a new `system_time` and `prev_block_hash`, so every hash after the `Seed` changes.
   Blocks are written with `update_block_ref: false`.
3. **Report.** `Success { old_head, new_head, old_num_blocks, new_num_blocks }`. The executor never
   moves `HEAD`.

The caller moves `HEAD` with a compare-and-swap against `old_head`. A write that moved `HEAD`
after planning (an ingest, a push, another compaction) makes the swap fail; the failure surfaces as
an internal error, and the new blocks and merged files stay unreferenced.

`CompactionListener` receives the plan, the result, and the `GatherChainInfo`, `MergeDataslices`
and `CommitNewBlocks` phases. In the CLI, `CompactionMultiProgress` (a
`CompactionMultiListener`) hands each dataset a `CompactionProgress` listener.

---

## 7. Storage

| Object | After compaction |
| --- | --- |
| Old blocks, data files, checkpoints | kept, and nothing deletes them. From the new `HEAD` only the `Seed`, the data files and checkpoints of blocks re-committed unchanged (`SingleBlock`), and the newest checkpoint of each merged batch stay reachable; the other old blocks and the original data files of merged batches do not |
| Merged files | moved into the dataset's data store on commit |
| `compaction-*` run directories | left in place; every `kamu` invocation, the API server included, wipes the run directory at startup, so they accumulate only for the lifetime of a long-running process |
| Blocks and files from an attempt that lost the `HEAD` swap | kept, unreferenced |

`kamu system gc` only purges the workspace cache; it does not collect unreferenced objects.
Because nothing is deleted, readers holding the old head keep working, and a reset can still move
`HEAD` back to a pre-compaction block ([dataset-reset.md](dataset-reset.md#5-reset-to-a-block)).

---

## 8. After compaction

Compaction changes every block hash after the `Seed`, so it is a history rewrite. What a rewrite
does to read models, derivatives, flows and remote copies is owned by
[dataset-reset.md](dataset-reset.md#7-after-a-history-rewrite). Specific to compaction: a
`NothingToDo` result moves nothing and reports no change downstream, and because the `Seed` is
unchanged, the Smart protocol accepts a forced push of the compacted chain.

---

## 9. Testing & gotchas

### Testing

| What | Where |
| --- | --- |
| Planner and executor: batching, limits, watermark-only blocks, offsets, schema changes, S3, metadata-only | [`test_compaction_services_impl.rs`](../../src/infra/core/tests/tests/test_compaction_services_impl.rs) |
| Use case: authorization, several datasets | [`test_compact_dataset_use_case.rs`](../../src/infra/core/tests/tests/use_cases/test_compact_dataset_use_case.rs) |
| Derivatives after compaction | `test_transform_services_impl.rs` (`test_transform_with_compaction_retry`, `test_transform_status_input_hard_compacted`) |
| Flow controller and flow agent | `test_flow_controller_compact.rs`, `test_flow_agent_impl.rs` |
| GraphQL configs and runs | `test_gql_dataset_flow_configs.rs`, `test_gql_dataset_flow_runs.rs` |
| CLI end to end; push after compaction | `src/e2e/app/cli/repo-tests/src/commands/test_compact_command.rs`, `test_smart_transfer_protocol.rs` |

### Behaviour by design

| Behaviour | Why |
| --- | --- |
| Only root datasets can be hard-compacted | A derivative's blocks record what it consumed from each input; merging them would break verification by replay. Metadata-only compaction is allowed for any kind |
| A single-slice batch keeps its original block | Rewriting a lone file would change nothing but the hash |
| Offsets are preserved | Offset ranges stay valid for anything that reads by offset |
| Nothing is deleted | Readers of the old head keep working. No collector reclaims the space, although the command's help text says compaction does |
| "Soft" compaction appears in the help but is rejected | Only hard compaction is implemented |

---

## 10. File/crate reference map

| Concern | File |
| --- | --- |
| Options, plan, result, errors, listeners | [`src/domain/core/src/services/compaction/`](../../src/domain/core/src/services/compaction) |
| Planner | [`compaction_planner_impl.rs`](../../src/infra/core/src/services/compaction/compaction_planner_impl.rs) |
| Executor | [`compaction_executor_impl.rs`](../../src/infra/core/src/services/compaction/compaction_executor_impl.rs) |
| Use case | [`compact_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/compact_dataset_use_case_impl.rs) |
| CLI command, arguments, progress | [`compact_command.rs`](../../src/app/cli/src/commands/compact_command.rs), `SystemCompact` in [`cli.rs`](../../src/app/cli/src/cli.rs), [`compact_progress.rs`](../../src/app/cli/src/output/compact_progress.rs) |
| Task planner and runner | [`hard_compact_dataset_task_planner.rs`](../../src/adapter/task-dataset/src/planners/hard_compact_dataset_task_planner.rs), [`hard_compact_dataset_task_runner.rs`](../../src/adapter/task-dataset/src/runners/hard_compact_dataset_task_runner.rs) |
| Flow controller and rule | [`flow_controller_compact.rs`](../../src/adapter/flow-dataset/src/flow_controllers/flow_controller_compact.rs), [`flow_config_rule_compact.rs`](../../src/adapter/flow-dataset/src/entities/flow_config_rules/flow_config_rule_compact.rs) |
