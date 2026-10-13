# Derived Dataset Transform — Architecture

> **Status:** in production; covers transforms of derivative datasets by `kamu pull`, transform
> flows and `kamu verify`. Behaviour that surprises newcomers is listed in
> [§11](#11-testing--gotchas).
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** A **derivative dataset** declares a `SetTransform`: a list of
input datasets (by ID, each with a query alias) and a SQL transform for a named **engine**. Kamu
never runs that SQL itself: it hands it to a versioned **engine container** (Spark, Flink,
DataFusion, RisingWave) through the ODF engine protocol. A transform run has three phases. **Plan**
reads the derivative's chain: the transform, its schema, and the last `ExecuteTransform`, which
records how far each input was consumed. **Elaborate** turns that into a request: for every input,
the data slices and explicit watermarks added since the consumed block, or "up to date" when there
are none. **Execute** provisions an engine, mounts the slices and the previous checkpoint into the
container, runs the query, then appends `SetDataSchema` (first time only) and one
`ExecuteTransform` block **without moving `HEAD`**. The caller moves `HEAD` with a
compare-and-swap, exactly as ingest does. If an input's history is rewritten (compaction, reset),
the derivative can no longer continue from the consumed block; it must be reset to metadata and
recomputed from scratch.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Know the ODF concepts this builds on | [§2 ODF concepts](#2-odf-concepts) |
| Follow a transform end to end | [§4 Plan, elaborate, execute](#4-plan-elaborate-execute) |
| Understand engine containers and their I/O | [§5 Engines](#5-engines) |
| See which entry point does what | [§6 Entry points](#6-entry-points) |
| Understand rewritten inputs and resets | [§7 Diverged inputs](#7-diverged-inputs) |
| Know how failures map to task outcomes | [§9 Errors](#9-errors) |
| Find the file for X | [§12 Reference map](#12-filecrate-reference-map) |

---

## Table of contents

- [1. Purpose \& scope](#1-purpose--scope)
- [2. ODF concepts](#2-odf-concepts)
- [3. Layers and crates](#3-layers-and-crates)
- [4. Plan, elaborate, execute](#4-plan-elaborate-execute)
- [5. Engines](#5-engines)
- [6. Entry points](#6-entry-points)
- [7. Diverged inputs](#7-diverged-inputs)
- [8. Committing and concurrency](#8-committing-and-concurrency)
- [9. Errors](#9-errors)
- [10. Local files](#10-local-files)
- [11. Testing \& gotchas](#11-testing--gotchas)
- [12. File/crate reference map](#12-filecrate-reference-map)

---

## 1. Purpose & scope

This page covers the path from a `SetTransform` to a new `HEAD` of a derivative dataset: the domain
interfaces, the three transform services, the engine provisioner and `ODFEngine` (the host-side
engine client), the entry points that drive them, and transform replay during verification.

Out of scope, each with its own owner:

| Topic | Owner |
| --- | --- |
| How root datasets get data; the shared commit-then-CAS pattern | [root-dataset-ingest.md](root-dataset-ingest.md) |
| How `kamu pull` plans, orders and authorizes transform jobs among others | [dataset-pull.md](dataset-pull.md) |
| When a transform flow runs: sensors, reactive rules, batching, breaking-change cascades | [flow-system.md](flow-system.md) |
| How the update and reset-to-metadata tasks are queued, planned and run | [task-system.md](task-system.md) |
| What reset to metadata keeps, and how compaction rewrites an input's history | [dataset-reset.md](dataset-reset.md#6-reset-to-metadata), [dataset-hard-compaction.md](dataset-hard-compaction.md) |
| How `DatasetReferenceMessage` and other outbox messages are delivered | [outbox.md](outbox.md) |
| What an engine does with the request | the engine's own repository and the [ODF engine contract](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#engine-contract) |

---

## 2. ODF concepts

The [Open Data Fabric specification](https://github.com/open-data-fabric/open-data-fabric) owns
the metadata events, the engine protocol, and the meaning of watermarks, retractions and
verifiability. This page does not restate them; read these first:

| Topic | Spec / RFC |
| --- | --- |
| Derivative datasets and transformations | [Derivative Dataset](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#derivative-dataset), [Nature of Transformations](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#nature-of-transformations), [Derivative Transformations](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#derivative-transformations) |
| `SetTransform`, `TransformInput`, `Transform::Sql` | [SetTransform](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#settransform), [TransformInput](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#transforminput) |
| `ExecuteTransform` and its per-input block and offset intervals | [ExecuteTransform](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#executetransform), [ExecuteTransformInput](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#executetransforminput) |
| Engines and the engine protocol | [Engine](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#engine), [Engine Contract](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#engine-contract), [TransformRequest](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#transformrequest), [TransformResponse](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#transformresponse) |
| Engine versions and query evolution | [Engine Versioning](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#engine-versioning), [Query Evolution](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#query-evolution) |
| Watermarks | [Watermark](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#watermark) |
| Retractions and corrections in derived data | [Retractions and Corrections](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#retractions-and-corrections), [RFC-015](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/015-unified-changelog-stream-schema.md) |
| Verifying derived data by replay | [Verifiability](https://github.com/open-data-fabric/open-data-fabric/blob/master/open-data-fabric.md#verifiability) |
| Offsets and schema in metadata | [RFC-001](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/001-record-offsets.md), [RFC-010](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/010-data-schema-in-metadata.md), [RFC-014](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/014-minimize-offset-scanning.md) |
| Checkpoints | [RFC-006](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/006-checkpoints-as-files.md) |

---

## 3. Layers and crates

| Layer | Crate / path | Contents |
| --- | --- | --- |
| Domain interfaces | `kamu-core` — [`src/domain/core/src/services/transform/`](../../src/domain/core/src/services/transform) | `TransformRequestPlanner`, `TransformElaborationService`, `TransformExecutor`, `TransformListener`, `TransformStatus`, error types |
| Engine interfaces | `kamu-core` — [`engine.rs`](../../src/domain/core/src/entities/engine.rs), [`engine_provisioner.rs`](../../src/domain/core/src/services/engine_provisioner.rs) | `Engine`, `TransformRequestExt`, `TransformResponseExt`, `EngineError`; `EngineProvisioner`, `EngineProvisioningError` |
| Transform services | `kamu` — [`src/infra/core/src/services/transform/`](../../src/infra/core/src/services/transform) | `TransformRequestPlannerImpl`, `TransformElaborationServiceImpl`, `TransformExecutorImpl`, `transform_helpers` |
| Engines | `kamu` — [`src/infra/core/src/engine/`](../../src/infra/core/src/engine) | `EngineProvisionerLocal`, `ODFEngine`, `EngineContainer`, the I/O strategies |
| Engine protocol client | `opendatafabric-metadata` — `odf::metadata::engines::EngineGrpcClient` | gRPC client for the ODF engine adapter inside the container |
| Chain validation | `opendatafabric-dataset-impl` — [`metadata_chain_validators.rs`](../../src/odf/dataset-impl/src/entities/metadata_chain_validators.rs) | `ValidateSetTransformVisitor`, `ValidateExecuteTransformVisitor` |
| Pull planning and execution | `kamu` — [`pull_request_planner_impl.rs`](../../src/infra/core/src/services/pull_request_planner_impl.rs), [`pull_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/pull_dataset_use_case_impl.rs) | `kamu pull` |
| Task adapter | `kamu-adapter-task-dataset` | `UpdateDatasetTaskPlanner`, `UpdateDatasetTaskRunner::run_transform_update` |
| Flow adapter | `kamu-adapter-flow-dataset` | `FlowControllerTransform`, `DerivedDatasetFlowSensor`, `TransformFlowEvaluatorImpl` |
| Verification | `kamu` — [`verification_service_impl.rs`](../../src/infra/core/src/services/verification_service_impl.rs) | replays `ExecuteTransform` blocks |

The transform services never parse or run SQL. `EngineDatafusionInproc` runs DataFusion inside the
process, but only for ingest `preprocess` steps; its `execute_transform` is
`unimplemented!`, because a derivative must be reproducible by a versioned engine.

---

## 4. Plan, elaborate, execute

### 4.1 Plan

`TransformRequestPlanner::build_transform_preliminary_plan(target)` calls
`build_preliminary_request_ext`, which walks the derivative's chain from `HEAD` once and collects:

| Field | From |
| --- | --- |
| `head` | the resolved `HEAD`; the commit is chained onto it |
| `transform`, input declarations | latest `SetTransform` |
| `schema` | latest `SetDataSchema`, if any |
| `vocab` | latest `SetVocab`, or the default vocabulary |
| `input_states` | each `SetTransform` input paired with its entry in the latest `ExecuteTransform`, or `None` before the first run |
| `prev_offset`, `prev_checkpoint` | latest `ExecuteTransform` |

The planner also resolves every input by ID through `DatasetRegistry` into a `ResolvedDatasetsMap`
that later phases use to read inputs. A non-derivative target, or one without `SetTransform`,
fails with `TransformNotDefinedError`.

### 4.2 Elaborate

`TransformElaborationService::elaborate_transform(target, plan, options, listener)` turns each
input state into a `TransformRequestInputExt`:

1. `get_transform_query_input` resolves the input's current `HEAD` and its last data offset. The
   consumed position is `prev_block_hash` / `prev_offset`; anything newer becomes
   `new_block_hash` / `new_offset`.
2. It reads the input's schema from its latest `SetDataSchema`; an input with no schema fails with
   `InputSchemaNotDefinedError` before any interval check.
3. `collect_unprocessed_input_blocks` iterates the input chain from the new head back to (but
   excluding) the consumed block. If the consumed block is not an ancestor of the new head, it
   fails with `InvalidInputIntervalError` ([§7](#7-diverged-inputs)).
4. From those blocks, oldest first, it collects the physical hashes of new data slices and every
   `newWatermark` as an explicit watermark.

The result is `UpToDate` when every input has no new slices and no new watermarks **and** the
derivative already has a schema. A derivative without a schema always runs, so that the engine can
establish the output schema even from empty inputs. `InputSchemaNotDefinedError` is caught here
and reported as up to date: a derivative of a never-ingested root silently waits.

Otherwise the request gets a fresh `operation_id` and `system_time`, which become the new block's
system time.

### 4.3 Execute and commit

`TransformExecutor::execute_transform(target, plan, listener)`:

```text
provision_engine(sql.engine)            EngineProvisionerLocal: pull image if missing,
                                        wait for a concurrency slot
engine.execute_transform(request)       ODFEngine: materialize inputs, start container,
                                        gRPC TransformRequest, fix file ownership, stop container,
                                        read the output schema from the data file
commit_execute_transform(response)
 ├─ schema known      → validate_schema_compatible(old, new)       (no event)
 │  schema unknown    → commit SetDataSchema(stripped of encoding) update_block_ref = false
 ├─ commit_execute_transform(ExecuteTransform, data, checkpoint)   update_block_ref = false
 │     query_inputs, prev_offset, prev_checkpoint from the request;
 │     new offsets, watermark, data, checkpoint from the engine
 └─ NoOpEvent from the chain → treated as success
```

The result is `Updated { old_head, new_head }`; the executor never returns `UpToDate`. `HEAD` is
moved by the caller ([§8](#8-committing-and-concurrency)).

The appended block passes `ValidateExecuteTransformVisitor`: inputs listed in `SetTransform`
order, each input's `prevBlockHash` / `prevOffset` equal to what the previous `ExecuteTransform`
consumed, and a `SetDataSchema` before the first data.

---

## 5. Engines

### 5.1 Provisioning

`EngineProvisionerLocal` maps the transform's engine name to a configured image (`spark`, `flink`,
`datafusion`, `risingwave`; defaults in [`docker_images.rs`](../../src/infra/core/src/utils/docker_images.rs),
overridable through `EngineProvisionerLocalConfig`). An unknown name is an internal error.

Before handing out an engine it:

- checks the image locally and pulls it once per process if missing;
- waits for a concurrency slot. In host networking mode only one engine runs at a time, and a
  configured `max_concurrency` is ignored with a warning; with a private network namespace
  `max_concurrency` applies, and when unset or `0` there is no limit.

The returned `EngineHandle` gives the slot back when it is dropped.

### 5.2 One transform, one container

`ODFEngine::execute_transform` starts a new container for every request and stops it afterwards;
containers are not reused. Per operation it:

1. creates `RunInfoDir/transform-<operation_id>/` with `in/`, `out/` and `logs/`;
2. picks an I/O strategy from the **target's** data repo protocol:

   | Strategy | When | Inputs |
   | --- | --- | --- |
   | `EngineIoStrategyLocalVolume` | local filesystem repo | each input slice and the previous checkpoint is bind-mounted read-only from the dataset's data/checkpoint repo |
   | `EngineIoStrategyRemoteProxy` | S3, HTTP, in-memory | each object is downloaded into `in/`, then mounted read-only |

3. writes each input's schema as an empty Parquet file and mounts it next to the data;
4. sends the `TransformRequest` with `nextOffset` = `prev_offset + 1` (0 before the first output),
   each input's offset interval and explicit watermarks, and the container paths for the new data
   and checkpoint (`out/` is mounted read-write);
5. under Docker on Unix, `chown`s the outputs back to the current user, since the engine writes
   as root;
6. terminates the container and reads `out/data` and `out/checkpoint` back.

The response is checked against the contract: an offset interval without a data file, or an
output that is not a plain file, is an `EngineError::ContractError`. A data file with no offset
interval is read only for its schema and then discarded.

Engine stdout and stderr go to `logs/`. ODF adapters log JSON lines tagged by process; when an
`EngineError` is built, the stdout log is demultiplexed into one file per process and stream
(`engine-<process>.<stream>.txt`), and the error carries the log paths so the CLI can show them.
A successful run leaves only the raw stdout and stderr files.

---

## 6. Entry points

| Entry point | Path | Authorization | Transaction scope | Moves `HEAD` |
| --- | --- | --- | --- | --- |
| `kamu pull <derivative>` | `PullCommand` → `PullDatasetUseCaseImpl` → planner, elaboration, executor | per pull ([dataset-pull.md](dataset-pull.md#7-authorization)) | planning in one transaction per pull; per job, elaboration in its own transaction, the engine outside, the ref update in its own ([dataset-pull.md](dataset-pull.md#62-per-job-runners)) | after each job |
| Transform flow | `DerivedDatasetFlowSensor` → `FlowControllerTransform` → `LogicalPlanDatasetUpdate` → `UpdateDatasetTaskPlanner` → `UpdateDatasetTaskRunner::run_transform_update` | system; the GraphQL mutations that configure or trigger the flow check access ([flow-system.md](flow-system.md#12-graphql-api)) | planning in one transaction; elaboration and execution outside; ref update in its own | once |
| `kamu verify <derivative>` | `VerifyDatasetUseCaseImpl` → `VerificationServiceImpl` → `build_transform_verification_plan` → `execute_verify_transform` | Read | the command's | never |

**`kamu pull`.** Planning, depth ordering, concurrency and authorization belong to pull
([dataset-pull.md](dataset-pull.md)). What matters for transforms: a transform at depth 1 sees
what depth 0 just committed, because elaboration re-reads input heads
([dataset-pull.md](dataset-pull.md#62-per-job-runners)).

**Transform flow.** The flow system decides when a transform runs
([flow-system.md](flow-system.md#8-sensors-and-propagation)). Transform-specific pieces that
live here:

- `TransformFlowEvaluatorImpl` calls `TransformRequestPlanner::evaluate_transform_status`, which
  returns `UpToDate`, `NewInputDataAvailable` (inputs whose last data offset moved), or
  `InputBreakingChange` ([§7](#7-diverged-inputs)). The sensor uses it when it is activated.
- The task planner builds the same pull plan as `kamu pull` for a single derivative with default
  options. The runner elaborates with `TransformOptions::default()`, so it never resets on its own.

**Verify.** `build_transform_verification_plan` collects the `ExecuteTransform` blocks in the
requested range and rebuilds a request for each from the block's own inputs, offsets, checkpoint
and system time, using the **latest** `SetTransform` and `SetDataSchema`.
`execute_verify_transform` runs each through the engine and builds the event with
`prepare_execute_transform`, without committing. It then compares the result with the recorded
event, ignoring the physical hash and size of the data slice and the whole checkpoint, which are
not reproducible.

---

## 7. Diverged inputs

When an input's history is rewritten — hard compaction, reset, a forced pull — the block the last
`ExecuteTransform` consumed is no longer an ancestor of the input's `HEAD`. Offsets cannot be
compared across the rewrite, so the derivative cannot continue.

| Path | Detection | What happens |
| --- | --- | --- |
| `kamu pull` | elaboration fails with `InvalidInputIntervalError` | error, unless `--reset-derivatives-on-diverged-input`: then the derivative is reset to metadata ([dataset-reset.md](dataset-reset.md#6-reset-to-metadata)), `HEAD` is CAS-moved to the compacted head, and elaboration runs once more from scratch |
| Transform task | elaboration fails with `InvalidInputIntervalError` | task fails with `InputDatasetCompacted`, which is unrecoverable |
| Flow sensor | `evaluate_transform_status` returns `InputBreakingChange` (checked before new data); or an upstream flow reports a `Breaking` change | with the `Recover` rule, a reset-to-metadata flow; with `NoAction`, a warning ([flow-system.md](flow-system.md#8-sensors-and-propagation)) |

After a reset to metadata the derivative has no `ExecuteTransform`, so the next transform consumes
every input from its first block.

---

## 8. Committing and concurrency

Transforms use the same split as ingest
([root-dataset-ingest.md](root-dataset-ingest.md#8-committing-and-concurrency)): the executor
appends `SetDataSchema` and `ExecuteTransform` with `update_block_ref: false`, and the caller moves
`HEAD` with `check_ref_is: Some(old_head)`. That ref update posts `DatasetReferenceMessage`, which
downstream sensors, the dependency graph and statistics react to.

Consequences specific to transforms:

- `old_head` is the head the **plan** saw. For flows that is task planning time. Another writer
  that moves the derivative's `HEAD` in between (a manual `kamu pull`, a reset) makes the CAS fail,
  and the committed blocks and data file stay unreferenced.
- The CAS covers only the derivative. Inputs are read without locks: an input that moves after
  elaboration is picked up by the next run, and an input that is compacted between elaboration and
  execution fails inside the engine or at materialization.
- The schema is committed in its own block before `ExecuteTransform`, but `HEAD` moves only after
  both, so readers never see one without the other.

---

## 9. Errors

`TransformError` wraps `TransformPlanError` (`TransformNotDefined`), `TransformElaborateError`
(`InputSchemaNotDefined`, `InvalidInputInterval`), `TransformExecuteError`
(`EngineProvisioningError`, `EngineError`, `CommitError`) and `Internal`. `EngineError` is
`InvalidQuery` (the engine rejected the SQL), `ProcessError`, `ContractError` or `InternalError`,
and carries engine log paths.

How the update task turns these into recoverable or unrecoverable outcomes (an invalid input
interval becomes `InputDatasetCompacted`) is owned by
[task-system.md](task-system.md#71-update-dataset).

---

## 10. Local files

| Directory | Holds | Cleaned |
| --- | --- | --- |
| `RunInfoDir/transform-<operation_id>/in/` | input schema files; with the remote proxy, downloaded slices and checkpoint | when the process starts |
| `RunInfoDir/transform-<operation_id>/out/` | `data` and `checkpoint` written by the engine | both move into the dataset's repos on commit; a data file with no offset interval is deleted |
| `RunInfoDir/transform-<operation_id>/logs/` | engine stdout and stderr; on failure, the demultiplexed adapter logs | when the process starts |

---

## 11. Testing & gotchas

### Testing

- Service tests: `src/infra/core/tests/tests/test_transform_services_impl.rs`,
  `test_verification_service_impl.rs`; engine tests that run real containers:
  `src/infra/core/tests/tests/engine/` (`test_engine_transform.rs`, `test_engine_io.rs`).
- Mocks in `src/infra/core/src/testing/`: `MockTransformRequestPlanner`,
  `MockTransformElaborationService`, `MockTransformExecutionService`;
  `MockTransformFlowEvaluator` (flow-dataset `testing` feature) for sensor tests.
- CLI scenarios: `src/e2e/app/cli/repo-tests/src/commands/test_pull_command.rs`,
  `test_verify_command.rs`.
- Engine tests need Podman or Docker and pull the engine images; see
  [`DEVELOPER.md`](../../DEVELOPER.md).

### Behaviour by design

| Area | What happens |
| --- | --- |
| Engines | Every transform runs out of process in a pinned engine image; the in-process DataFusion engine is never used for transforms |
| Containers | One container per transform; nothing is reused between runs |
| Schema-establishing runs | A derivative without a schema runs the engine even when no input has new data |
| Inputs never ingested | Reported as up to date, not as an error |
| Diverged inputs | Never recovered automatically by the task; recovery is the CLI flag or the flow's `Recover` rule |
| Verification | Replays every `ExecuteTransform` in the range with the latest `SetTransform`, and compares the whole event except the data slice's physical hash and size and the checkpoint |

---

## 12. File/crate reference map

| Concern | File |
| --- | --- |
| Planner trait, status, plan errors | [`transform_request_planner.rs`](../../src/domain/core/src/services/transform/transform_request_planner.rs) |
| Elaboration and executor traits | [`transform_elaboration_service.rs`](../../src/domain/core/src/services/transform/transform_elaboration_service.rs), [`transform_executor.rs`](../../src/domain/core/src/services/transform/transform_executor.rs) |
| Options, results, shared errors | [`transform_types.rs`](../../src/domain/core/src/services/transform/transform_types.rs) |
| Preliminary request, input slicing | [`transform_helpers.rs`](../../src/infra/core/src/services/transform/transform_helpers.rs) |
| Planner, status, verification plan | [`transform_request_planner_impl.rs`](../../src/infra/core/src/services/transform/transform_request_planner_impl.rs) |
| Elaboration, reset on diverged input | [`transform_elaboration_service_impl.rs`](../../src/infra/core/src/services/transform/transform_elaboration_service_impl.rs) |
| Executor, commit, verification replay | [`transform_executor_impl.rs`](../../src/infra/core/src/services/transform/transform_executor_impl.rs) |
| Engine trait and request/response types | [`engine.rs`](../../src/domain/core/src/entities/engine.rs) |
| Provisioner, concurrency, images | [`engine_provisioner_local.rs`](../../src/infra/core/src/engine/engine_provisioner_local.rs) |
| `ODFEngine` (host-side engine client) | [`engine_odf.rs`](../../src/infra/core/src/engine/engine_odf.rs) |
| Container lifecycle and logs | [`engine_container.rs`](../../src/infra/core/src/engine/engine_container.rs) |
| Input materialization | [`engine_io_strategy.rs`](../../src/infra/core/src/engine/engine_io_strategy.rs) |
| Chain validators | [`metadata_chain_validators.rs`](../../src/odf/dataset-impl/src/entities/metadata_chain_validators.rs) |
| Pull planning and execution | [`pull_request_planner_impl.rs`](../../src/infra/core/src/services/pull_request_planner_impl.rs), [`pull_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/pull_dataset_use_case_impl.rs) |
| Update task planner / runner | [`update_dataset_task_planner.rs`](../../src/adapter/task-dataset/src/planners/update_dataset_task_planner.rs), [`update_dataset_task_runner.rs`](../../src/adapter/task-dataset/src/runners/update_dataset_task_runner.rs) |
| Transform flow controller and sensor | [`flow_controller_transform.rs`](../../src/adapter/flow-dataset/src/flow_controllers/flow_controller_transform.rs), [`derived_dataset_flow_sensor.rs`](../../src/adapter/flow-dataset/src/flow_sensors/derived_dataset_flow_sensor.rs) |
| Status evaluator for flows | [`transform_flow_evaluator_impl.rs`](../../src/adapter/flow-dataset/src/services/transform_flow_evaluator_impl.rs) |
| Verification | [`verification_service_impl.rs`](../../src/infra/core/src/services/verification_service_impl.rs) |
