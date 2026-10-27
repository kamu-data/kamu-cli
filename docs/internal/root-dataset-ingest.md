# Root Dataset Ingest — Architecture

> **Status:** in production; covers polling and push ingest into root datasets. Behaviour that
> surprises newcomers is listed in [§11](#11-testing--gotchas).
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** A **root dataset** gets data from the outside world through a
**source** declared in its metadata chain: one `SetPollingSource` (Kamu goes and fetches the data)
or any number of named `AddPushSource` events (a client sends the data in). Both kinds share one
pipeline: read the input into a DataFusion data frame and run an optional preprocess query, then
hand it to the **data writer**, which **merges** it with the dataset's history according to the
source's merge strategy, adds the system columns, writes one Parquet slice, and appends
`SetDataSchema` (first time only) and `AddData` blocks to the metadata chain. The writer appends
blocks **without moving the `HEAD` reference**; the caller moves `HEAD` afterwards with a
compare-and-swap against the head it started from, and that ref update is what the dataset
projections (blocks, dependency graph, statistics, search) react to. Polling ingest adds a fetch
layer in front of the writer — fetch, prepare, read — with **source state** (an ETag, a
last-modified time, a last file name, a block number) stored in `AddData` so the next run can
resume, and a local **savepoint** so a failed run can retry without re-fetching.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Know the ODF concepts this builds on | [§2 ODF concepts](#2-odf-concepts) |
| Understand what gets written to the chain and data | [§4 The data writer](#4-the-data-writer) |
| Follow a polling ingest end to end | [§5 Polling ingest](#5-polling-ingest) |
| Follow a push ingest end to end | [§6 Push ingest](#6-push-ingest) |
| See which entry point does what | [§7 Entry points](#7-entry-points) |
| Understand `HEAD` updates, races and messages | [§8 Committing and concurrency](#8-committing-and-concurrency) |
| Know how failures map to task outcomes | [§9 Errors](#9-errors) |
| Find the file for X | [§12 Reference map](#12-filecrate-reference-map) |

---

## Table of contents

- [1. Purpose \& scope](#1-purpose--scope)
- [2. ODF concepts](#2-odf-concepts)
- [3. Layers and crates](#3-layers-and-crates)
- [4. The data writer](#4-the-data-writer)
- [5. Polling ingest](#5-polling-ingest)
- [6. Push ingest](#6-push-ingest)
- [7. Entry points](#7-entry-points)
- [8. Committing and concurrency](#8-committing-and-concurrency)
- [9. Errors](#9-errors)
- [10. Local files](#10-local-files)
- [11. Testing \& gotchas](#11-testing--gotchas)
- [12. File/crate reference map](#12-filecrate-reference-map)

---

## 1. Purpose & scope

Ingest is the only way new records enter the system; everything else (transform, sync) derives
from or copies records that were ingested somewhere. This page covers the path from a source
definition to a new `HEAD` of a root dataset: the domain interfaces, the DataFusion writer and
merge strategies, the polling fetch layer, the push planner and executor, and the entry points that
drive them.

Out of scope, each with its own owner:

| Topic | Owner |
| --- | --- |
| When an ingest flow runs: schedules, retries, batching, `fetch_next_iteration` | [flow-system.md](flow-system.md) |
| How `kamu pull` plans, orders and authorizes ingest jobs among others | [dataset-pull.md](dataset-pull.md) |
| How the update task is queued, planned and run | [task-system.md](task-system.md) |
| How `DatasetReferenceMessage` and other outbox messages are delivered | [outbox.md](outbox.md) |
| The ODF metadata model itself | [ODF spec](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/overview.md) |
| Derivative datasets (`SetTransform`, `ExecuteTransform`) | [derived-dataset-transform.md](derived-dataset-transform.md) |

---

## 2. ODF concepts

The [Open Data Fabric specification](https://github.com/open-data-fabric/open-data-fabric) owns
the metadata events, merge strategy semantics, system columns and watermarks. This page does not
restate them; read these first:

| Topic | Spec / RFC |
| --- | --- |
| Root datasets and ingestion | [Root Dataset](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/glossary.md#root-dataset), [Data Ingestion](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/datasets/ingestion.md) |
| `SetPollingSource` | [Polling Sources](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/datasets/ingestion.md#push-and-polling-sources) |
| `AddPushSource`, `DisablePushSource`, `DisablePollingSource` | [RFC-011](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/011-push-ingest-sources.md) |
| `AddData.newSourceState` | [RFC-009](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/009-ingest-source-state.md) |
| `SetDataSchema` | [RFC-010](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/010-data-schema-in-metadata.md), [RFC-016](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/016-odf-schema.md) |
| Offsets in `AddData` | [RFC-001](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/001-record-offsets.md), [RFC-014](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/014-minimize-offset-scanning.md) |
| Merge strategies | [Merge Strategies](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/datasets/ingestion.md#merge-strategies) |
| The `op` column, retractions and corrections | [RFC-015](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/015-unified-changelog-stream-schema.md) |
| Linked objects | [RFC-017](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/017-large-files-linking.md) |
| Watermarks | [Watermark](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/glossary.md#watermark) |

---

## 3. Layers and crates

| Layer | Crate / path | Contents |
| --- | --- | --- |
| Domain interfaces | `kamu-core` — [`src/domain/core/src/services/ingest/`](../../src/domain/core/src/services/ingest) | `PollingIngestPlanner`, `PollingIngestService`, `PushIngestPlanner`, `PushIngestExecutor`, `DataWriter`, `MergeStrategy`, `Reader`, `DataFormatRegistry`, error types |
| Writer state | `kamu-core` — [`writer_metadata_state.rs`](../../src/domain/core/src/entities/writer_metadata_state.rs) | `DataWriterMetadataState`: the projection of the chain the writer needs |
| Use cases | `kamu-core` — [`push_ingest_data_use_case.rs`](../../src/domain/core/src/use_cases/push_ingest_data_use_case.rs), `pull_dataset_use_case.rs` | `PushIngestDataUseCase`, `PullDatasetUseCase` |
| DataFusion writer | `kamu-ingest-datafusion` — [`src/infra/ingest-datafusion/`](../../src/infra/ingest-datafusion/src) | `DataWriterDataFusion`, merge strategies, readers |
| Polling and push services | `kamu` — [`src/infra/core/src/services/ingest/`](../../src/infra/core/src/services/ingest) | `PollingIngestPlannerImpl`, `PollingIngestServiceImpl`, `FetchService`, `PrepService`, `PushIngestPlannerImpl`, `PushIngestExecutorImpl`, `DataFormatRegistryImpl`, `ingest_common` |
| Use case impls | `kamu` — [`src/infra/core/src/use_cases/`](../../src/infra/core/src/use_cases) | `PushIngestDataUseCaseImpl`, `PullDatasetUseCaseImpl` |
| Task adapter | `kamu-adapter-task-dataset` | `UpdateDatasetTaskPlanner`, `UpdateDatasetTaskRunner` |
| Flow adapter | `kamu-adapter-flow-dataset` | `FlowControllerIngest`, `FlowConfigRuleIngest` |
| Entry points | `kamu-cli` (`kamu pull`, `kamu ingest`), `kamu-adapter-http` (`POST /{dataset}/ingest`) | See [§7](#7-entry-points) |

The writer and the merge strategies know nothing about where input came from; the polling and
push services know nothing about how records are merged. `ingest_common` holds the pieces both
services share: the session context factory, the preprocess step and the default preprocessing.

---

## 4. The data writer

### 4.1 Writer state

`DataWriterMetadataState::build(target, block_ref, source_name, head)` walks the chain once from
`head` (or from the resolved `block_ref` when `head` is `None`) with a set of visitors and
collects:

| Field | From |
| --- | --- |
| `head` | the starting block; every commit is chained onto it |
| `schema` | latest `SetDataSchema` |
| `source_event`, `merge_strategy` | the active source selected by `WriterSourceEventVisitor` for `source_name` |
| `vocab` | latest `SetVocab`, or the default vocabulary |
| `data_slices` | physical hashes of every data slice ever added, newest first |
| `prev_offset`, `prev_watermark`, `prev_checkpoint` | latest `AddData` |
| `prev_source_state` | source state of the latest `AddData` (none if that block has none) |

`build` asserts the seed's kind is `Root`; callers must reject derivative datasets before calling
it. The state is built once per operation and updated in memory by each commit, so a multi-step
operation (a polling loop, `execute_multi`) does not rescan the chain.

### 4.2 `stage` then `commit`

`DataWriter` splits writing in two so callers can inspect the result (for example, check a storage
quota) before anything is appended:

```text
stage(df?, opts)                                   commit(staged)
 ├─ validate_input        no system columns         ├─ commit_event(SetDataSchema)   if new_schema
 ├─ normalize_raw_result  timestamps → ms, UTC      ├─ commit_add_data(AddData, data_file)
 ├─ get_all_previous_data read every past slice      │     prev_block_hash = meta.head
 ├─ ensure_event_time_column                         │     update_block_ref = false
 ├─ merge_strategy.merge(prev, new)                  └─ advance meta: head, prev_offset,
 ├─ with_system_columns   offset, system_time,             data_slices, watermark,
 │                        event_time fallback              source_state, checkpoint
 ├─ coerce_schema / validate_schema_compatible
 ├─ write_output          single Parquet file
 └─ compute_offset_and_watermark, linked objects
```

`write` is `stage` followed by `commit`; `write_watermark` commits an `AddData` that carries no data,
only a watermark and optionally a source state (used by `SetWatermarkExecutorImpl` on behalf of
`SetWatermarkUseCase`, not by ingest). The executor moves `HEAD` through `DatasetHeadCommitter`; if
`HEAD` moved after planning it fails with `SetWatermarkExecutionError::RefCASFailed`.

### 4.3 What `stage` decides

| Situation | `SetDataSchema` | `AddData` | Result |
| --- | --- | --- | --- |
| Input produced records | yes, if the dataset has no schema yet | offsets, slice, watermark = max(prev, max `event_time`), source state | committed |
| Input was read but merge produced no records | yes, if the dataset has no schema yet | only if watermark or source state changed | committed or `EmptyCommit` |
| No data frame at all (`None`) | no | only if source state changed | committed or `EmptyCommit` |

`EmptyCommit` is not an error to callers: both polling and push map it to "up to date". A run that
only changes source state still produces an `AddData` block with no data — this is how a polling
source that saw a new ETag but produced no new rows records that it is caught up.

Points worth knowing before changing this code:

- **Offsets** are assigned with `row_number()` ordered by the merge strategy's `sort_order()`,
  or in the order of the merged records when `sort_order()` is empty, starting at
  `prev_offset + 1`, and the slice is sorted by offset.
- **Input order survives only in one partition.** A partitioned scan splits the input file into
  ranges and interleaves them. The ingest session defaults to `target_partitions = 1`
  (`EngineConfigDatafusionEmbeddedIngest`). For a strategy whose `requires_ordered_input()` is true
  (`ChangelogStream`, which numbers records in input order, and `UpsertStream`, which keeps the
  last upsert per key), the writer sets `target_partitions = 1` on the merged data frame whatever
  the configuration says. DataFusion reads the setting when it plans the frame for execution, so
  it applies to the whole plan, input scan included, as long as nothing executes the plan
  before the writer sets it.
- **Event time** missing from the input is filled with `source_event_time` (polling: the time the
  fetch step reported, otherwise system time; push: the caller's event time, otherwise system
  time).
- **Merge needs history.** `Ledger`, `Snapshot` and `UpsertStream` read every previous slice of the
  dataset on each ingest (`get_all_previous_data`); cost grows with the dataset.
- **Schema coercion** only adjusts nullability toward the dataset schema. The slice must then match
  the dataset schema column by column (names, order, metadata and types, with large and view
  variants of string, binary and list types treated as equal), or staging fails with
  `IncompatibleSchemaError`.
- **Linked objects** (`ObjectLink` columns) are checked against the data repo; a missing object is
  a `DanglingReferenceError`.

### 4.4 Merge strategies

The spec defines what each strategy means. The implementation
(`merge_strategy_for` picks one per source) differs only in cost and ordering:

| Strategy | Reads all previous slices | Offset order within a slice | Implementation |
| --- | --- | --- | --- |
| `Append` | no | `event_time` | [`append.rs`](../../src/infra/ingest-datafusion/src/merge_strategies/append.rs) |
| `Ledger` | yes — left anti-join on the primary key | `event_time` | [`ledger.rs`](../../src/infra/ingest-datafusion/src/merge_strategies/ledger.rs) |
| `Snapshot` | yes — projects history to latest-by-PK, then diffs | primary key, then operation type | [`snapshot.rs`](../../src/infra/ingest-datafusion/src/merge_strategies/snapshot.rs) |
| `ChangelogStream` | no | input order | [`changelog_stream.rs`](../../src/infra/ingest-datafusion/src/merge_strategies/changelog_stream.rs) |
| `UpsertStream` | yes — projects history to latest-by-PK | primary key, then operation type | [`upsert_stream.rs`](../../src/infra/ingest-datafusion/src/merge_strategies/upsert_stream.rs) |

The order comes from each strategy's `sort_order()`. `Snapshot` and `UpsertStream` emit at most one
change per key in a slice (a `-C`/`+C` pair sorts correctly by operation type), so sorting by key
loses nothing. A changelog can retract and re-append a key, or correct it several times, within
one chunk; projecting the state takes the last record per key by offset, so `ChangelogStream`
keeps the records in the order they arrived.

---

## 5. Polling ingest

### 5.1 Call sequence

A polling ingest has three steps, the same for every caller:

| Step | Who | Transaction |
| --- | --- | --- |
| **Plan** | `PollingIngestPlanner::plan_ingest(target)` builds the writer state at the current head into a `PollingIngestPlan`; `resolve_env_vars(dataset_id)` resolves the dataset's environment variables | the caller's, then the target is detached |
| **Execute** | `PollingIngestService::ingest(target, plan, options, env_vars, operation_id, listener)` runs one iteration and appends blocks without moving `HEAD` | none |
| **Commit** | the caller moves `HEAD` through `DatasetHeadCommitter` ([§8](#8-committing-and-concurrency)) | a new one |

Planning fails with `SourceNotDefined` when the dataset has no `SetPollingSource`.
`PollingIngestPlanPayload` is the plan's stored form: the dataset, the expected head, the
`SetPollingSource` block and the source state, offset and watermark it found.
`rebuild_ingest` turns it back into a plan, failing with `HeadMoved` when the head is no longer the
expected one and with `Mismatch` when the facts differ, which only a defect can cause.

Environment variable values are never part of a plan or a task definition. `resolve_env_vars` is
their only path: it returns an empty map unless secrets encryption is configured and enabled
(`SecretsEncryptionConfig::is_enabled`), and `IngestEnvVars` prints keys only.

`PollingIngestServiceImpl::ingest` runs one **iteration**:

1. **Cache check.** A source is *uncacheable* when the dataset already has data
   (`prev_offset.is_some()`) but no source state, and the fetch step is not MQTT. Without
   `fetch_uncacheable` such a source is skipped as `UpToDate { uncacheable: true }`.
2. **Fetch** ([§5.2](#52-fetch-steps)), honoring a savepoint ([§5.3](#53-savepoints)).
   `FetchResult::UpToDate` ends the iteration.
3. **Prepare** — optional `decompress` / `pipe` steps run on a blocking thread by `PrepService`,
   writing a new file into the cache directory.
4. **Read** — the `ReadStep` picks a reader from `DataFormatRegistry`. An empty or missing input
   file yields an empty frame when the read step has a schema, and no frame otherwise.
5. **Preprocess** — the source's `Transform` runs in DataFusion in-process or in a provisioned
   engine; without one, `preprocess_default` renames columns that clash with system columns and
   coerces an integer or string `event_time` to a timestamp, but only when the read step has no
   explicit schema.
6. **Stage and commit** through the writer with the fetch's new source state and event time.
7. Return `Updated { old_head, new_head, has_more, uncacheable, increment, new_source_state }`
   and the advanced writer state. `increment` counts the blocks, records and watermark between
   the two heads.

The working directory `RunInfoDir/ingest-<operation_id>/` is removed after a successful iteration
and kept after a failed one, whose errors may point at logs in it. The service never moves
`HEAD`; see [§8](#8-committing-and-concurrency).

### 5.2 Fetch steps

`FetchService::fetch` dispatches on `FetchStep`. URL, headers, the container image, args and env
values, the MQTT password and the EVM node URL pass through `${{ env.NAME || default }}`
templating. A name that is not set falls through to the next option; a lookup that fails, such as
a secret that cannot be decrypted, fails the fetch. With secrets encryption enabled (`DatasetKeyValueServiceImpl`), names resolve from the
dataset's env vars only; without it (`DatasetKeyValueServiceSysEnv`), any supplied dataset env vars
are checked first and the process environment is the fallback, which in practice means the process
environment, since no dataset env var service is wired in that mode.

| Fetch step | Source state written | `has_more` | Notes |
| --- | --- | --- | --- |
| `Url` `file://` | `LastModified` (file mtime) | never | Zero-copy: the savepoint references the original file |
| `Url` `http(s)://` | `ETag` or `LastModified` from response headers, else none | never | Sends `If-None-Match` / `If-Modified-Since`; `304` is up to date |
| `Url` `ftp(s)://` | none | never | Uncacheable after the first ingest; behind the `ingest-ftp` feature |
| `FilesGlob` | `ETag` = name of the file just ingested | when more files sort after it | One file per iteration, ordered by name; event time from system time or the file path |
| `Container` | whatever the container writes to `ODF_NEW_ETAG_PATH` / `ODF_NEW_LAST_MODIFIED_PATH` | when it creates `ODF_NEW_HAS_MORE_DATA_PATH` | Receives `ODF_ETAG`, `ODF_LAST_MODIFIED`, `ODF_BATCH_SIZE`; same state as before means up to date |
| `Mqtt` | none | never | Always fetched; never treated as uncacheable and savepoints are ignored; behind `ingest-mqtt` |
| `EthereumLogs` | `ETag` encoding the last scanned block | when the scan stopped before the chain head | Behind `ingest-evm` |

`PollingSourceState` understands only the `odf/etag` and `odf/last-modified` kinds; any other kind
is ignored by the fetch step, which then fetches as if there were no previous state. The source
state is read from the latest `AddData` only, so any `AddData` written without one (for example by
`set-watermark`) makes the source look stateless as well. The `cache` (`SourceCaching`) field of
`Url` and `FilesGlob` fetch steps is not consulted.

### 5.3 Savepoints

Fetching can be slow and expensive, while reading and merging can fail for reasons the user fixes
by editing the source (a bad preprocess query, a schema mismatch). After a successful fetch the
service writes a `FetchSavepoint` — creation time, new source state, event time, a reference to the
fetched data, `has_more` — to `cache/fetch-savepoint-<hash>`, where the hash covers the
flatbuffers form of the fetch step and the previous source state. The next iteration with the same
fetch step and the same committed state resumes from the savepoint instead of fetching again.

- A savepoint is ignored when the dataset has no source state and `fetch_uncacheable` is set, and
  always for MQTT.
- Changing the fetch step or committing a new source state changes the hash, so stale savepoints
  are never matched — they are left behind and removed by cache eviction ([§10](#10-local-files)).
- Prepared files are deleted after staging; the fetched file and savepoint are kept on purpose so a
  user iterating on a broken source can re-run without re-downloading.

### 5.4 Iterating: `has_more`

A source that yields data in batches (glob, container, EVM) reports `has_more`. Who loops:

| Driver | Behaviour |
| --- | --- |
| `kamu pull` | `PullOptions::exhaust_sources` is set; `PullIngestLoop` repeats the iteration, moving `HEAD` after each one. A failure after earlier iterations committed is `PullError::IngestStopped`, which reports what was committed ([dataset-pull.md](dataset-pull.md#62-per-job-runners)) |
| Ingest flow | one iteration per task; on success with `has_more` and `fetch_next_iteration` in the flow's ingest config, `FlowControllerIngest` schedules the next flow immediately |

---

## 6. Push ingest

### 6.1 Call sequence

`PushIngestDataUseCaseImpl::execute(target, data_source, options, listener)`:

1. **Add a push source** (uploads through the HTTP endpoint only) —
   `PushIngestPlannerImpl::plan_push_source_creation` returns an
   `AddPushSource { source_name: "auto", merge: Append }`, with a read step guessed from the media
   type, when the dataset has no source at all. The use case appends it on top of the current
   `HEAD` and moves `HEAD` through `DatasetHeadCommitter`, comparing against the head it read at
   that moment, separately from the data commit: the source stays even if the ingest that
   follows fails. Without a media type the push fails with `SourceNotFound`.
2. **Plan** — `PushIngestPlannerImpl::plan_ingest` builds the writer state at `HEAD` (or at
   `expected_head` when given) for `source_name`. The planner writes nothing. A missing source is
   `SourceNotFound`; with no `source_name` and several push sources, `WriterSourceEventVisitor`
   reports the ambiguity as `SourceNotFound` too.
3. **Execute** — `PushIngestExecutorImpl::execute_ingest`:
   materialize the `DataSource` into a file (a `file://` URL is used in place unless it is a FIFO
   or device, which is copied first; buffers and streams are written to the operation directory),
   read it — overriding the source's read step with a compatible one when the caller supplied a
   media type — preprocess, `stage`, check the account storage quota on the staged file size unless
   `skip_quota_check` (always skipped in single-tenant workspaces), then `commit`.
4. **Finalize** — move `HEAD` from `old_head` to `new_head` through `DatasetHeadCommitter` and post
   `DatasetExternallyChangedMessage::ingest_http`.

A lost CAS in step 1 or 4 is
`PushIngestError::CommitError(CommitError::MetadataAppendError(AppendError::RefCASFailed))` with
the expected and actual heads; nothing is committed.

`execute_multi` runs steps 1 to 3 for each data source in turn, threading the new head through
`expected_head`, and moves `HEAD` once at the end from the first `old_head` to the last `new_head`.

Push ingest stores no source state (`new_source_state: None`), so `AddPushSource`'s capacity for
exactly-once resume is unused.

### 6.2 Data sources and formats

| `DataSource` | Produced by |
| --- | --- |
| `Url` (`file://` only) | `kamu ingest <files>`, `kamu ingest --stdin` (as `/dev/fd/0`) |
| `Stream` | HTTP request body, or the upload store stream for an `uploadToken` |
| `Buffer` | in-process callers such as collections and versioned files |

`DataFormatRegistryImpl` maps media types and file extensions to `ReadStep`s:
`get_best_effort_config` builds a read step from a media type alone (used to auto-create a source),
and `get_compatible_read_config` returns the existing read step unchanged when its format matches
the media type, and otherwise builds a best-effort one for the media type that keeps only the
schema.

---

## 7. Entry points

| Entry point | Path | Authorization | Transaction scope | Moves `HEAD` | Messages |
| --- | --- | --- | --- | --- | --- |
| `kamu pull <root>` | `PullCommand` → `PullDatasetUseCaseImpl` → `PullIngestLoop` → `PollingIngestService` | write check per plan iteration | short transactions: pull planning; ingest planning with env-var lookup; each ref update | after each iteration | `DatasetReferenceMessage` from the ref update |
| Ingest flow | `FlowControllerIngest` → `LogicalPlanDatasetUpdate` → `UpdateDatasetTaskPlanner` → `UpdateDatasetTaskRunner` → `PollingIngestService` (one iteration) | system | ingest planning with env-var lookup in one transaction, when the task runs; fetch and write outside; ref update in its own | once | `DatasetReferenceMessage`; task and flow progress messages |
| `kamu ingest <root>` | `IngestCommand` → `PushIngestDataUseCase::execute` per file | none; the command rejects derivative datasets and datasets with a pull alias | the command's | once per file | `DatasetReferenceMessage`, `DatasetExternallyChangedMessage` |
| `POST /{dataset}/ingest` | `dataset_ingest_handler` → `PushIngestDataUseCase::execute` | `DatasetAction::Write` via ReBAC; upload token must belong to the caller | the whole request (`#[transactional_handler]`) | once; twice when a push source is auto-created | as above |
| Collections, versioned files | `UpdateCollectionEntriesUseCaseImpl`, `UpdateVersionedFileUseCaseImpl` → `PushIngestDataUseCase` | the calling use case | the caller's | once | as above |

Flow-side options reach ingest through `FlowConfigRuleIngest`: `fetch_uncacheable` becomes
`PollingIngestOptions::fetch_uncacheable`; `fetch_next_iteration` is read by the flow controller
only. Dataset env vars are resolved by the planner in both paths ([§5.1](#51-call-sequence)).

---

## 8. Committing and concurrency

Ingest separates **appending blocks** from **publishing them**:

1. The writer appends `SetDataSchema` / `AddData` with `prev_block_hash = meta.head` and
   `update_block_ref: false`. Data files and blocks land in storage; `HEAD` does not move, so no
   reader sees them yet.
2. The caller moves `HEAD` from `old_head` through `DatasetHeadCommitter`, a CAS in the caller's
   transaction. This goes through `DatasetReferenceServiceImpl::set_reference`, which writes the reference row and posts
   `DatasetReferenceMessage::Updated` in the same transaction; consumers write the storage-level
   reference, index the new blocks (which feeds the dependency graph), and update statistics and
   search ([outbox.md](outbox.md)).

Consequences:

- Two writers racing on one dataset both append blocks chained onto the same parent; the second
  CAS fails and its blocks and data file stay in storage unreferenced. Neither writer takes a lock.
  The polling path does not retry in-process; a flow-driven ingest is retried only as a new task
  under the flow's retry policy ([§9](#9-errors)).
- The writer state comes from the moment it was planned, which for both `kamu pull` and the
  update task is just before the fetch. A push that lands while the fetch runs makes the poll's
  CAS fail.
- `HEAD` moves only after all of an operation's blocks are written, so readers see the blocks of
  one operation together. That can still be a `SetDataSchema` alone, when a first ingest yields no
  rows ([§4.3](#43-what-stage-decides)), and an auto-created push source moves `HEAD` earlier
  ([§6.1](#61-call-sequence)).
- `DatasetExternallyChangedMessage` is posted by push ingest (and by the smart push server,
  [dataset-sync.md](dataset-sync.md#72-push-flow)); flows use it to tell user pushes from their
  own updates.

---

## 9. Errors

`PollingIngestError` and `PushIngestError` share the input errors (`ReadError`,
`BadInputSchema`, `IncompatibleSchema`, `MergeError`, `ExecutionError`, `DataValidation`),
`EngineError`, `CommitError` and `Internal`. Only polling has fetch errors (`NotFound`,
`Unreachable`, `ProcessError`, `ImagePull`, `PipeError`), `EngineProvisioningError`, and template
and parameter errors. Only push has `UnsupportedMediaType`, `QuotaExceeded` and `Access`. Push
planning fails separately with `PushIngestPlanningError` (`SourceNotFound`, `HeadNotFound`,
`UnsupportedMediaType`, `Internal`).

Each error type says whether a retry could succeed: `PollingIngestError::is_recoverable` and
`PollingIngestPlanError::is_recoverable`, built on the same methods of `EngineError` and
`EngineProvisioningError`. Errors caused by the source definition or by the fetched data are not
recoverable: a retry resumes from the same savepoint and fails the same way. That covers template
and parameter errors, read, schema, merge, execution and validation errors, an invalid preprocess
query, an unsupported engine, and a dataset without a polling source. Fetch, process, image, commit
and internal errors are recoverable. The update task maps the flag to its outcome
([task-system.md](task-system.md#71-update-dataset)).

The writer reports a value that does not parse or cast to its column's type as `ExecutionError`,
including values a CSV reader parses only when the data is written. A CAS conflict when moving `HEAD` after a
successful ingest becomes the recoverable task error `DatasetCommitError` → `HeadMoved`; any other
failure to move `HEAD` is returned from the runner as an internal error, which the task agent also
records as a recoverable failure. The retry plans from the new head. It reuses the fetch
savepoint only if the winning commit left the source state unchanged; if the winner was a push or
`set-watermark`, which write no source state, the source now looks uncacheable and the retry is
skipped as up to date unless `fetch_uncacheable` is set.

The HTTP handler maps push errors to status codes: `SourceNotFound` and `ReadError` → 400,
unsupported media type → 415, `QuotaExceeded` → 403, the rest → 500.

---

## 10. Local files

| Directory | Holds | Cleaned |
| --- | --- | --- |
| `RunInfoDir/ingest-<operation_id>/` | push input copies, reader temp files, `out/data.parquet` before commit | the staged Parquet file moves into the data repo on commit; a polling iteration removes the directory on success, and push leaves it; any left are cleared when the process starts |
| `RunInfoDir/fetch-<operation_id>/`, `RunInfoDir/raw-query-<operation_id>/` | container fetch output and logs; preprocess engine I/O and logs | cleared when the process starts |
| `CacheDir/fetch-*`, `prepare-*` | fetched and prepared input | prepared files after staging, or when reading or preprocessing fails; fetched files by eviction |
| `CacheDir/fetch-savepoint-<hash>` | savepoints | by eviction |

Eviction (`GcService::evict_cache`) runs at process start and removes cache entries older than 24
hours. A long-running server does not evict while it runs.

---

## 11. Testing & gotchas

### Testing

- Writer and merge strategy tests live in `src/infra/ingest-datafusion/tests/`; polling planner,
  polling and push service tests in `src/infra/core/tests/tests/ingest/`; CLI scenarios in
  `src/e2e/app/cli/repo-tests/src/commands/` (`test_ingest_command.rs`, `test_pull_command.rs`).
- The pull loop (`exhaust_sources`, `IngestStopped`, head moved) is tested in
  `test_pull_dataset_use_case.rs`; the update task runner in
  `src/adapter/task-dataset/tests/`, including a real planner and service over a `file://` source
  (`BaseRepoHarness::create_root_dataset_polling_csv`).
- `MockPollingIngestService` (helpers in `src/domain/core/src/testing/mock_polling_ingest_service.rs`)
  stands in for the polling service where a test needs no fetching;
  `make_expect_ingest_iterations` reports a sequence of `MockIngestIteration`s.
- Containerized fetch and preprocess tests need Podman or Docker and are grouped separately; see
  [`DEVELOPER.md`](../../DEVELOPER.md).
- Changing what `stage` considers an empty commit changes which runs produce blocks — and so which
  runs trigger downstream flows.
- Changing the flatbuffers form of `FetchStep` or `SourceState` invalidates every existing
  savepoint; that is harmless but forces a re-fetch.

### Behaviour by design

| Area | What happens |
| --- | --- |
| Concurrent writers | No lock is taken; the writer that loses the `HEAD` CAS leaves its blocks and data file in storage, unreferenced |
| Uncacheable sources | A source with data but no source state is skipped as up to date unless `fetch_uncacheable` is set ([§5.1](#51-call-sequence)) |
| Savepoints after success | Fetched data and savepoints stay in the cache so a user can re-run a broken source without re-downloading |
| Disabled sources | Any `DisablePollingSource` / `DisablePushSource` in the chain makes building the writer state panic (`unimplemented!`); disabling sources is not supported |
| History-reading merges | `Ledger`, `Snapshot` and `UpsertStream` read every previous slice on each ingest |
| Push source state | Push ingest writes no source state |
| Auto-created push source | Only an upload-token push creates one; a raw-body push or `kamu ingest` into a dataset without a push source fails with `SourceNotFound` |

---

## 12. File/crate reference map

| Concern | File |
| --- | --- |
| Polling planner trait, plan, env vars, planning errors | [`polling_ingest_planner.rs`](../../src/domain/core/src/services/ingest/polling_ingest_planner.rs) |
| Stored plan form | [`polling_ingest_plan_payload.rs`](../../src/domain/core/src/services/ingest/polling_ingest_plan_payload.rs) |
| Polling planner impl, env var gate | [`polling_ingest_planner_impl.rs`](../../src/infra/core/src/services/ingest/polling_ingest_planner_impl.rs) |
| Polling service trait, options, result, errors | [`polling_ingest_service.rs`](../../src/domain/core/src/services/ingest/polling_ingest_service.rs) |
| Polling service impl, savepoints | [`polling_ingest_service_impl.rs`](../../src/infra/core/src/services/ingest/polling_ingest_service_impl.rs) |
| Source state and savepoint types | [`polling_source_state.rs`](../../src/infra/core/src/services/ingest/polling_source_state.rs) |
| Fetch dispatch and templating | [`fetch_service/core.rs`](../../src/infra/core/src/services/ingest/fetch_service/core.rs), [`template.rs`](../../src/infra/core/src/services/ingest/fetch_service/template.rs) |
| Fetch step impls | [`fetch_service/`](../../src/infra/core/src/services/ingest/fetch_service) (`file.rs`, `http.rs`, `ftp.rs`, `container.rs`, `mqtt.rs`, `evm.rs`) |
| Prepare steps | [`prep_service.rs`](../../src/infra/core/src/services/ingest/prep_service.rs) |
| Shared preprocess and session context | [`ingest_common.rs`](../../src/infra/core/src/services/ingest/ingest_common.rs) |
| Push planner / executor traits | [`push_ingest_planner.rs`](../../src/domain/core/src/services/ingest/push_ingest_planner.rs), [`push_ingest_executor.rs`](../../src/domain/core/src/services/ingest/push_ingest_executor.rs) |
| Push planner / executor impls | [`push_ingest_planner_impl.rs`](../../src/infra/core/src/services/ingest/push_ingest_planner_impl.rs), [`push_ingest_executor_impl.rs`](../../src/infra/core/src/services/ingest/push_ingest_executor_impl.rs) |
| Push use case | [`push_ingest_data_use_case_impl.rs`](../../src/infra/core/src/use_cases/push_ingest_data_use_case_impl.rs) |
| Pull use case | [`pull_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/pull_dataset_use_case_impl.rs) |
| Pull polling loop, ref update | [`pull_ingest_loop.rs`](../../src/infra/core/src/use_cases/pull_ingest_loop.rs) |
| Writer trait and errors | [`data_writer.rs`](../../src/domain/core/src/services/ingest/data_writer.rs) |
| Writer state | [`writer_metadata_state.rs`](../../src/domain/core/src/entities/writer_metadata_state.rs) |
| DataFusion writer | [`writer.rs`](../../src/infra/ingest-datafusion/src/writer.rs) |
| Merge strategies | [`merge_strategies/`](../../src/infra/ingest-datafusion/src/merge_strategies) |
| Readers | [`readers/`](../../src/infra/ingest-datafusion/src/readers) |
| Format registry | [`data_format_registry_impl.rs`](../../src/infra/core/src/services/ingest/data_format_registry_impl.rs) |
| Update task planner / runner | [`update_dataset_task_planner.rs`](../../src/adapter/task-dataset/src/planners/update_dataset_task_planner.rs), [`update_dataset_task_runner.rs`](../../src/adapter/task-dataset/src/runners/update_dataset_task_runner.rs) |
| Ingest flow controller | [`flow_controller_ingest.rs`](../../src/adapter/flow-dataset/src/flow_controllers/flow_controller_ingest.rs) |
| HTTP push endpoint | [`ingest_handler.rs`](../../src/adapter/http/src/data/ingest_handler.rs) |
| CLI commands | [`pull_command.rs`](../../src/app/cli/src/commands/pull_command.rs), [`ingest_command.rs`](../../src/app/cli/src/commands/ingest_command.rs) |
