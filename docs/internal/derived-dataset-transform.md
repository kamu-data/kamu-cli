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
interfaces, the transform planner and executor, the engine provisioner and `ODFEngine` (the host-side
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
| What an engine does with the request | the engine's own repository and the [ODF engine contract](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/transformations/engine-contract.md) |

---

## 2. ODF concepts

The [Open Data Fabric specification](https://github.com/open-data-fabric/open-data-fabric) owns
the metadata events, the engine protocol, and the meaning of watermarks, retractions and
verifiability. This page does not restate them; read these first:

| Topic | Spec / RFC |
| --- | --- |
| Derivative datasets and transformations | [Derivative Dataset](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/glossary.md#derivative-dataset), [Nature of Transformations](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/overview.md#nature-of-transformations), [Derivative Transformations](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/transformations/processing-model.md#derivative-transformations) |
| `SetTransform`, `TransformInput`, `Transform::Sql` | [SetTransform](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/reference.md#settransform), [TransformInput](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/reference.md#transforminput) |
| `ExecuteTransform` and its per-input block and offset intervals | [ExecuteTransform](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/reference.md#executetransform), [ExecuteTransformInput](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/reference.md#executetransforminput) |
| Engines and the engine protocol | [Engine](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/glossary.md#engine), [Engine Contract](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/transformations/engine-contract.md), [TransformRequest](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/reference.md#transformrequest), [TransformResponse](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/reference.md#transformresponse) |
| Engine versions and query evolution | [Engine Versioning](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/transformations/engine-contract.md#engine-versioning), [Query Evolution](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/transformations/processing-model.md#query-evolution) |
| Watermarks | [Watermark](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/glossary.md#watermark) |
| Retractions and corrections in derived data | [Retractions and Corrections](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/transformations/processing-model.md#retractions-and-corrections), [RFC-015](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/015-unified-changelog-stream-schema.md) |
| Verifying derived data by replay | [Verifiability](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/glossary.md#verifiability) |
| Offsets and schema in metadata | [RFC-001](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/001-record-offsets.md), [RFC-010](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/010-data-schema-in-metadata.md), [RFC-014](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/014-minimize-offset-scanning.md) |
| Checkpoints | [RFC-006](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/006-checkpoints-as-files.md) |

---

## 3. Layers and crates

| Layer | Crate / path | Contents |
| --- | --- | --- |
| Domain interfaces | `kamu-core` — [`src/domain/core/src/services/transform/`](../../src/domain/core/src/services/transform) | `TransformPlanner`, `TransformExecutor`, `TransformListener`, `TransformPlanPayload`, `TransformStatus`, error types |
| Engine interfaces | `kamu-core` — [`engine.rs`](../../src/domain/core/src/entities/engine.rs), [`engine_provisioner.rs`](../../src/domain/core/src/services/engine_provisioner.rs) | `Engine`, `TransformRequestExt`, `TransformResponseExt`, `EngineError`; `EngineProvisioner`, `EngineProvisioningError` |
| Transform services | `kamu` — [`src/infra/core/src/services/transform/`](../../src/infra/core/src/services/transform) | `TransformPlannerImpl`, `TransformExecutorImpl`, `transform_helpers` |
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

Planning and elaboration are one service, `TransformPlanner`. It only reads, and it resolves
inputs through `DatasetRegistry`, so it is called in a transaction with the target resolved in
that transaction ([§4.4](#44-transactions)):

| Method | Returns | Used by |
| --- | --- | --- |
| `describe_transform(target)` | `TransformDescription`: the `SetTransform` block in effect, the transform, and the input handles in `SetTransform` order | pull planning, to fail a broken transform early and to check `Read` on the inputs |
| `elaborate_transform(target)` | `Elaborated(TransformExecutionPlan)` (the request and the resolved inputs) or `UpToDate`, at the current heads and time | `kamu pull`, the update task |
| `rebuild_transform(target, stored)` | the `TransformExecutionPlan` of a stored plan ([§4.5](#45-rebuilding-a-stored-plan)) | workers that store a plan before running it |
| `evaluate_transform_status(target)` | `TransformStatus` ([§6](#6-entry-points)) | the transform flow sensor |
| `build_transform_verification_plan(target, range)` | one replay step per `ExecuteTransform` ([§6](#6-entry-points)) | `kamu verify` |

### 4.1 Plan

`elaborate_transform`, `rebuild_transform` and `evaluate_transform_status` start from a
preliminary request: `build_preliminary_request_ext` walks the derivative's chain once, from
`HEAD` or from a given block when rebuilding, and collects the fields below. `describe_transform`
reads only the `SetTransform` in effect; the verification plan reads its own history
([§6](#6-entry-points)).

| Field | From |
| --- | --- |
| `head` | the resolved `HEAD`; the commit is chained onto it |
| `transform`, input declarations | latest `SetTransform` |
| `transform_block` | hash of that `SetTransform` block |
| `schema` | latest `SetDataSchema`, if any |
| `vocab` | latest `SetVocab`, or the default vocabulary |
| `input_states` | each `SetTransform` input paired by dataset ID with its entry in the latest `ExecuteTransform`, or `None` before the first run |
| `prev_offset`, `prev_checkpoint` | latest `ExecuteTransform` |

The planner also resolves every input by ID through `DatasetRegistry` into a `ResolvedDatasetsMap`
that later phases use to read inputs. A non-derivative target, or one without `SetTransform`,
fails with `TransformNotDefinedError`. When the inputs of the latest `SetTransform` differ from
those the last `ExecuteTransform` consumed, planning fails with `TransformInputsChangedError`
([§11](#11-testing--gotchas)).

### 4.2 Elaborate

`elaborate_transform` turns each input state into a `TransformRequestInputExt`. It writes no
blocks, moves no reference and reports to no listener.

1. `get_transform_query_input` resolves the input's current `HEAD` and its last data offset. The
   consumed position is `prev_block_hash` / `prev_offset`; anything newer becomes
   `new_block_hash` / `new_offset`.
2. It reads the input's schema and vocabulary as of the last block it consumes, not the input's
   current `HEAD`. An input with no schema fails with `InputNotPulledError`, naming the input,
   before any interval check.
3. `collect_unprocessed_input_blocks` iterates the input chain from the new head back to (but
   excluding) the consumed block. If the consumed block is not an ancestor of the new head, it
   fails with `InvalidInputIntervalError`, naming the input ([§7](#7-diverged-inputs)).
4. From those blocks, oldest first, it collects the physical hashes of new data slices and every
   `newWatermark` as an explicit watermark.

The result is `UpToDate` when every input has no new slices and no new watermarks **and** the
derivative already has a schema. A derivative without a schema always runs, so that the engine can
establish the output schema even from empty inputs. `InputNotPulledError` is returned to the
caller: `kamu pull` reports it as the derivative's error, while the update task treats it as up to
date ([§6](#6-entry-points)).

Otherwise the request gets the current time as `system_time`, which becomes the new block's system
time.

### 4.3 Execute and commit

`TransformExecutor::execute_transform(target, plan, operation_id, listener)`. `operation_id`
identifies the execution attempt ([§10](#10-local-files)):

```text
provision_engine(sql.engine)            EngineProvisionerLocal: pull image if missing,
                                        wait for a concurrency slot
engine.execute_transform(request)       ODFEngine: materialize inputs, start container,
                                        gRPC TransformRequest, fix file ownership, stop container,
                                        read the output schema from the data file
commit_execute_transform(response)
 ├─ schema known      → validate_schema_compatible(old, new)       → IncompatibleSchema
 │  schema unknown    → commit SetDataSchema(stripped of encoding) update_block_ref = false
 ├─ commit_execute_transform(ExecuteTransform, data, checkpoint)   update_block_ref = false
 │     query_inputs, prev_offset, prev_checkpoint from the request;
 │     new offsets, watermark, data, checkpoint from the engine
 └─ NoOpEvent from the chain → nothing appended
```

The result is `Updated { old_head, new_head, new_offset_interval, increment }`, or `UpToDate` when
nothing was appended. `new_offset_interval` is the records the engine produced, if any.
`increment` holds the blocks and records appended and the watermark the transform advanced to,
computed from the appended blocks with `get_increment_for_interval`, so callers need not query the
chain again. `HEAD` is moved by the caller ([§8](#8-committing-and-concurrency)).

The appended block passes `ValidateExecuteTransformVisitor`: inputs listed in `SetTransform`
order, each input's `prevBlockHash` / `prevOffset` equal to what the previous `ExecuteTransform`
consumed, and a `SetDataSchema` before the first data.

### 4.4 Transactions

Every caller follows one rule:

| Step | Transaction | Why |
| --- | --- | --- |
| `elaborate_transform` | one, with the target and inputs resolved through `DatasetRegistry` inside it | the expected head and every input head come from one database snapshot |
| Execute | none; the target and the plan's datasets are detached first | the engine runs for minutes |
| Commit | a new one, through `DatasetHeadCommitter` with `old_head` (the request's `head`) as the expected head | the CAS fails if another writer moved `HEAD` since elaboration |

`kamu pull` and the update task both plan when the job runs, not when the pull or the task was
planned ([§6](#6-entry-points)).

### 4.5 Rebuilding a stored plan

A worker that stores a plan before running the engine must rebuild the same request after a
restart. `TransformPlanPayload::from_request` maps a request to the compact form stored as the
`TransformPlan` condition: the dataset, `system_time`, the expected head, `transform_block`, and
per input the head it was elaborated at, its offset interval and watermark. It names no data,
checkpoint or staging paths, which are derived from the heads again.

`rebuild_transform(target, stored)` reads the target's current `HEAD` itself:

| Outcome | Meaning | The worker |
| --- | --- | --- |
| `TransformExecutionPlan` | `HEAD` is still the stored expected head; the request is elaborated at the stored input heads and system time | runs it |
| `HeadMoved` | another writer moved `HEAD` since the plan was stored | discards the plan and elaborates again |
| `Mismatch` | the rebuilt request differs from the stored one | discards the plan; this is a defect |
| `Internal` | a read failed | retries |

Metadata at a fixed head is immutable, so the rebuilt request equals the original; the method
checks this by mapping the result back and comparing it with the stored payload.

---

## 5. Engines

### 5.1 Provisioning

`EngineProvisionerLocal` maps the transform's engine name to a configured image (`spark`, `flink`,
`datafusion`, `risingwave`; defaults in [`docker_images.rs`](../../src/infra/core/src/utils/docker_images.rs),
overridable through `EngineProvisionerLocalConfig`), ignoring case. An unknown name fails with
`UnsupportedEngineError`.

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

The response is checked against the contract: an offset interval without a data file, a data
file whose row count differs from the offset interval, or an output that is not a plain file, is
an `EngineError::ContractError`. A data file with no offset interval is read only for its schema
and then discarded.

Engine stdout and stderr go to `logs/`. ODF adapters log JSON lines tagged by process; when an
`EngineError` is built, the stdout log is demultiplexed into one file per process and stream
(`engine-<process>.<stream>.txt`), and the error carries the log paths so the CLI can show them.
A successful run leaves nothing behind ([§10](#10-local-files)).

---

## 6. Entry points

| Entry point | Path | Authorization | Transaction scope | Moves `HEAD` |
| --- | --- | --- | --- | --- |
| `kamu pull <derivative>` | `PullCommand` → `PullDatasetUseCaseImpl` → planner, executor | per pull ([dataset-pull.md](dataset-pull.md#7-authorization)) | pull planning in one transaction per pull; per job, the rule of [§4.4](#44-transactions) ([dataset-pull.md](dataset-pull.md#62-per-job-runners)) | after each job |
| Transform flow | `DerivedDatasetFlowSensor` → `FlowControllerTransform` → `LogicalPlanDatasetUpdate` → `UpdateDatasetTaskPlanner` → `UpdateDatasetTaskRunner::run_transform_update` | system; the GraphQL mutations that configure or trigger the flow check access ([flow-system.md](flow-system.md#12-graphql-api)) | the rule of [§4.4](#44-transactions), when the task runs | once |
| `kamu verify <derivative>` | `VerifyDatasetUseCaseImpl` → `VerificationServiceImpl` → `build_transform_verification_plan` → `execute_verify_transform` | Read | the command's | never |

**`kamu pull`.** Planning, depth ordering, concurrency and authorization belong to pull
([dataset-pull.md](dataset-pull.md)). What matters for transforms: a transform at depth 1 sees
what depth 0 just committed, because elaboration re-reads input heads
([dataset-pull.md](dataset-pull.md#62-per-job-runners)).

**Transform flow.** The flow system decides when a transform runs
([flow-system.md](flow-system.md#8-sensors-and-propagation)). Transform-specific pieces that
live here:

- `TransformFlowEvaluatorImpl` calls `TransformPlanner::evaluate_transform_status`, which
  returns `InputBreakingChange` ([§7](#7-diverged-inputs)), checked first;
  `NewInputDataAvailable` for the inputs elaboration would consume something from (new data or
  a new watermark, or any new block while the derivative has no schema); or `UpToDate`, also when
  an input was never pulled. The evaluator reports changed inputs as up to date with a warning.
  The sensor uses it when it is activated.
- For a transform the task keeps only the target's handle; the runner plans, elaborates, executes
  and commits it ([task-system.md](task-system.md#71-update-dataset)). The runner never resets on
  its own: a diverged input fails the task, and an input without data ends it as up to date.

**Verify.** `build_transform_verification_plan` collects the `ExecuteTransform` blocks in the
requested range and rebuilds a request for each from the block's own inputs, offsets, checkpoint
and system time, using the `SetTransform`, `SetDataSchema` and `SetVocab` in effect at that block.
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
| `kamu pull` | elaboration fails with `InvalidInputIntervalError` | error, unless `--reset-derivatives-on-diverged-input`: then `PullDatasetUseCaseImpl` resets the derivative to metadata ([dataset-reset.md](dataset-reset.md#6-reset-to-metadata)), moves `HEAD` to the reset head through `DatasetHeadCommitter` (CAS), re-plans and elaborates once more. The pull reports one update from the head before the reset to the final head |
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

- `old_head` is the head elaboration saw, when the job or task runs. Another writer that moves
  the derivative's `HEAD` during execution (a manual `kamu pull`, a reset) makes the CAS fail, and
  the committed blocks and data file stay unreferenced.
- The CAS covers only the derivative. Inputs are read without locks: an input that moves after
  elaboration is picked up by the next run, and an input that is compacted between elaboration and
  execution fails inside the engine or at materialization.
- The schema is committed in its own block before `ExecuteTransform`, but `HEAD` moves only after
  both, so readers never see one without the other.

---

## 9. Errors

`TransformError` wraps `TransformPlanError` (`TransformNotDefined`), `TransformElaborateError`
(`TransformNotDefined`, `InputsChanged`, `InputNotPulled`, `InvalidInputInterval`),
`TransformExecuteError` (`EngineProvisioningError`, `EngineError`, `IncompatibleSchema`,
`CommitError`) and `Internal`. `EngineProvisioningError` includes `UnsupportedEngine` and
`ImagePull`. `EngineError` is `InvalidQuery` (the engine rejected the SQL), `ProcessError`,
`ContractError` or `InternalError`, and carries engine log paths. `rebuild_transform` has its own
`TransformRebuildError` ([§4.5](#45-rebuilding-a-stored-plan)), and `evaluate_transform_status`
its own `TransformStatusError`.

`TransformElaborateError::is_recoverable` and `TransformExecuteError::is_recoverable` say whether
a retry could succeed. A missing or changed transform, a diverged input (`InvalidInputInterval`),
an incompatible schema, an invalid query and an unsupported engine are not recoverable: they need a
change of metadata first. An input without data yet, commit, engine, provisioning and internal
errors are recoverable. The engine part comes from `EngineError::is_recoverable` and
`EngineProvisioningError::is_recoverable`, which ingest shares. The outcomes the update task
decides itself, such as `InputDatasetCompacted`, are in
[task-system.md](task-system.md#71-update-dataset).

---

## 10. Local files

| Directory | Holds | Cleaned |
| --- | --- | --- |
| `RunInfoDir/transform-<operation_id>/in/` | input schema files; with the remote proxy, downloaded slices and checkpoint | after a successful commit, or when the process starts |
| `RunInfoDir/transform-<operation_id>/out/` | `data` and `checkpoint` written by the engine | both move into the dataset's repos on commit; a data file with no offset interval is deleted |
| `RunInfoDir/transform-<operation_id>/logs/` | engine stdout and stderr; on failure, the demultiplexed adapter logs | after a successful commit, or when the process starts |

The executor removes the operation directory once a transform has committed; a failed run keeps
it, because the `EngineError` points at its log files. The executor's caller chooses
`operation_id`, one per execution attempt, so two attempts of one plan never share staged output;
it also names the engine container. The directory is created with `create_dir`, so an attempt that
reuses an ID fails instead of mixing its output with an earlier one.

---

## 11. Testing & gotchas

### Testing

- Service tests: `src/infra/core/tests/tests/test_transform_services_impl.rs`,
  `test_verification_service_impl.rs`; engine tests that run real containers:
  `src/infra/core/tests/tests/engine/` (`test_engine_transform.rs`, `test_engine_io.rs`,
  `test_engine_provisioner.rs`), and `test_engine_transform_output.rs` for the response contract
  checks, which need no container.
- Mocks in `src/domain/core/src/testing/`: `MockTransformPlanner`, `MockTransformExecutor`,
  `MockEngineProvisioner` (its engine stub can return a chosen response);
  `MockTransformFlowEvaluator` (flow-dataset `testing` feature) for sensor tests.
- CLI scenarios: `src/e2e/app/cli/repo-tests/src/commands/test_pull_command.rs`,
  `test_verify_command.rs`.
- Engine tests need Podman or Docker and pull the engine images; see
  [`DEVELOPER.md`](../../DEVELOPER.md).

### Behaviour by design

| Area | What happens |
| --- | --- |
| Engines | Every transform runs out of process in a pinned engine image; the in-process DataFusion engine is never used for transforms. Engine names ignore case |
| Containers | One container per transform; nothing is reused between runs |
| Schema-establishing runs | A derivative without a schema runs the engine even when no input has new data |
| Inputs never ingested | `kamu pull` fails with `InputNotPulled`; the update task ends as up to date |
| Diverged inputs | Never recovered automatically by the task; recovery is the CLI flag or the flow's `Recover` rule |
| Query evolution | The query and engine of a `SetTransform` may change; its inputs may not. A changed input list fails planning with `InputsChanged`, because ODF validates `ExecuteTransform` inputs by position |
| Verification | Replays every `ExecuteTransform` in the range with the `SetTransform` in effect at it, and compares the whole event except the data slice's physical hash and size and the checkpoint |

---

## 12. File/crate reference map

| Concern | File |
| --- | --- |
| Planner trait, its results and status | [`transform_planner.rs`](../../src/domain/core/src/services/transform/transform_planner.rs) |
| Planner errors | [`transform_planner_errors.rs`](../../src/domain/core/src/services/transform/transform_planner_errors.rs) |
| Executor trait | [`transform_executor.rs`](../../src/domain/core/src/services/transform/transform_executor.rs) |
| Options, results, shared errors | [`transform_types.rs`](../../src/domain/core/src/services/transform/transform_types.rs) |
| Preliminary request, input slicing | [`transform_helpers.rs`](../../src/infra/core/src/services/transform/transform_helpers.rs) |
| Planner: describe, elaborate, rebuild, status, verification plan | [`transform_planner_impl.rs`](../../src/infra/core/src/services/transform/transform_planner_impl.rs) |
| Compact stored plan | [`transform_plan_payload.rs`](../../src/domain/core/src/services/transform/transform_plan_payload.rs) |
| Reset on diverged input during `kamu pull` | [`pull_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/pull_dataset_use_case_impl.rs) |
| Executor, commit, verification replay | [`transform_executor_impl.rs`](../../src/infra/core/src/services/transform/transform_executor_impl.rs) |
| Engine trait and request/response types | [`engine.rs`](../../src/domain/core/src/entities/engine.rs) |
| Provisioner, concurrency, images | [`engine_provisioner_local.rs`](../../src/infra/core/src/engine/engine_provisioner_local.rs) |
| `ODFEngine` (host-side engine client) | [`engine_odf.rs`](../../src/infra/core/src/engine/engine_odf.rs) |
| Engine output checked against the response | [`engine_transform_output.rs`](../../src/infra/core/src/engine/engine_transform_output.rs) |
| Container lifecycle and logs | [`engine_container.rs`](../../src/infra/core/src/engine/engine_container.rs) |
| Input materialization | [`engine_io_strategy.rs`](../../src/infra/core/src/engine/engine_io_strategy.rs) |
| Chain validators | [`metadata_chain_validators.rs`](../../src/odf/dataset-impl/src/entities/metadata_chain_validators.rs) |
| Pull planning and execution | [`pull_request_planner_impl.rs`](../../src/infra/core/src/services/pull_request_planner_impl.rs), [`pull_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/pull_dataset_use_case_impl.rs) |
| Update task planner / runner | [`update_dataset_task_planner.rs`](../../src/adapter/task-dataset/src/planners/update_dataset_task_planner.rs), [`update_dataset_task_runner.rs`](../../src/adapter/task-dataset/src/runners/update_dataset_task_runner.rs) |
| Transform flow controller and sensor | [`flow_controller_transform.rs`](../../src/adapter/flow-dataset/src/flow_controllers/flow_controller_transform.rs), [`derived_dataset_flow_sensor.rs`](../../src/adapter/flow-dataset/src/flow_sensors/derived_dataset_flow_sensor.rs) |
| Status evaluator for flows | [`transform_flow_evaluator_impl.rs`](../../src/adapter/flow-dataset/src/services/transform_flow_evaluator_impl.rs) |
| Verification | [`verification_service_impl.rs`](../../src/infra/core/src/services/verification_service_impl.rs) |
