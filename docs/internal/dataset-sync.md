# Dataset Sync — Architecture

> **Status:** in production; covers copying datasets between a local workspace and remote
> repositories: `SyncService`, the Simple and Smart Transfer Protocols on both the client and
> the server side, `kamu push`, remote repositories and aliases, and IPFS. Behaviour that
> surprises newcomers is listed in [§12](#12-testing--gotchas).
> Type names and paths below are drawn from source — when they drift, treat the source as canonical
> and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** A dataset is a content-addressed metadata chain plus data and
checkpoint objects named by their hashes, so copying it means: find which blocks the destination
lacks, copy the objects those blocks reference, append the blocks, and move the destination's
`HEAD`. **`SyncService`** does exactly that between two **sync refs** — a local dataset, a local
dataset to be created, or a remote dataset behind a URL — and picks a protocol from the URLs. A
pull into a local dataset stops before `HEAD`: the transfer leaves the new blocks unreferenced, and
the caller commits them in its own transaction with **`LocalSyncCommitter`**. The
**Simple Transfer Protocol** needs nothing from the remote but readable files (`refs/head`,
`blocks/`, `data/`, `checkpoints/`), works over the local filesystem, S3 and HTTP, and does all
the comparison work on the client. The **Smart Transfer Protocol** (`odf+http(s)://` URLs) talks
to a Kamu node over a WebSocket: the server computes the block delta, ships the blocks as one
tarball, and hands out URLs (often presigned S3 URLs) for the objects, which the client transfers
in parallel; it is the only way to *write* to a remote node. `kamu pull` uses sync for datasets
with a pull alias, `kamu push` always syncs, and the Kamu node serves both protocols.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Know the ODF concepts this builds on | [§2 ODF concepts](#2-odf-concepts) |
| See which protocol a sync uses | [§5 Choosing a protocol](#5-choosing-a-protocol) |
| Follow a Simple Transfer Protocol sync | [§6 Simple Transfer Protocol](#6-simple-transfer-protocol) |
| Follow a smart pull or push, client and server | [§7 Smart Transfer Protocol](#7-smart-transfer-protocol) |
| Understand `kamu push` and remote aliases | [§8 Push](#8-push), [§9 Remote repositories and aliases](#9-remote-repositories-and-aliases) |
| Know who may read or write over the wire | [§10 Authorization](#10-authorization) |
| Find the file for X | [§13 Reference map](#13-filecrate-reference-map) |

---

## Table of contents

- [1. Purpose \& scope](#1-purpose--scope)
- [2. ODF concepts](#2-odf-concepts)
- [3. Layers and crates](#3-layers-and-crates)
- [4. Sync requests](#4-sync-requests)
- [5. Choosing a protocol](#5-choosing-a-protocol)
- [6. Simple Transfer Protocol](#6-simple-transfer-protocol)
- [7. Smart Transfer Protocol](#7-smart-transfer-protocol)
- [8. Push](#8-push)
- [9. Remote repositories and aliases](#9-remote-repositories-and-aliases)
- [10. Authorization](#10-authorization)
- [11. Committing, validation and errors](#11-committing-validation-and-errors)
- [12. Testing \& gotchas](#12-testing--gotchas)
- [13. File/crate reference map](#13-filecrate-reference-map)

---

## 1. Purpose & scope

This page covers how one dataset is copied from a source to a destination, whichever side is
remote, and the server endpoints that let other clients copy from and to a Kamu node.

Out of scope, each with its own owner:

| Topic | Owner |
| --- | --- |
| When `kamu pull` syncs a dataset, how pull requests and local aliases are resolved, how sync jobs are scheduled and authorized among other jobs | [dataset-pull.md](dataset-pull.md) |
| How the update task runs a sync job on the server | [task-system.md](task-system.md) |
| How the server reacts to a dataset changed by a push: flows, sensors | [flow-system.md](flow-system.md) |
| How `HEAD` updates post `DatasetReferenceMessage` | [root-dataset-ingest.md](root-dataset-ingest.md#8-committing-and-concurrency) |
| The wire format of the Smart Transfer Protocol messages | the ODF [OpenAPI](https://github.com/open-data-fabric/open-data-fabric/blob/master/contracts/transfer/smart-transfer-protocol.openapi.yaml) and [AsyncAPI](https://github.com/open-data-fabric/open-data-fabric/blob/master/contracts/transfer/smart-transfer-protocol.asyncapi.yaml) specs |

---

## 2. ODF concepts

The [Open Data Fabric specification](https://github.com/open-data-fabric/open-data-fabric) owns
the repository model and both protocols. This page does not restate them; read these first:

| Topic | Spec / RFC |
| --- | --- |
| Repositories, aliases and references | [Repository](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/glossary.md#repository), [Aliases and References](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/datasets/identity.md#aliases-and-references) |
| Content addressability of blocks and objects | [RFC-003](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/003-content-addressability.md), [RFC-006](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/006-checkpoints-as-files.md) |
| Simple Transfer Protocol | [Simple Transfer Protocol](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/exchange/transfer-protocols.md#simple-transfer-protocol), [RFC-007](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/007-simple-transfer-protocol.md) |
| Smart Transfer Protocol | [Smart Transfer Protocol](https://github.com/open-data-fabric/open-data-fabric/blob/master/specs/exchange/transfer-protocols.md#smart-transfer-protocol), [RFC-008](https://github.com/open-data-fabric/open-data-fabric/blob/master/rfcs/008-smart-transfer-protocol.md), [OpenAPI](https://github.com/open-data-fabric/open-data-fabric/blob/master/contracts/transfer/smart-transfer-protocol.openapi.yaml), [AsyncAPI](https://github.com/open-data-fabric/open-data-fabric/blob/master/contracts/transfer/smart-transfer-protocol.asyncapi.yaml) |

---

## 3. Layers and crates

| Layer | Crate / path | Contents |
| --- | --- | --- |
| Domain types | `kamu-core` — [`sync_service.rs`](../../src/domain/core/src/services/sync_service.rs) | `SyncService`, `SyncRequest`, `LocalSyncRequest`, `LocalSyncTarget`, `SyncRefRemote`, `SyncOptions`, `SyncResult`, `SyncError`, listeners and stats |
| Chain comparison | `kamu-core` — [`metadata_chain_comparator.rs`](../../src/domain/core/src/utils/metadata_chain_comparator.rs) | `MetadataChainComparator::compare_chains` → equal / ahead / behind / diverged |
| Sync service and request builder | `kamu` — [`src/infra/core/src/services/sync/`](../../src/infra/core/src/services/sync) | `SyncServiceImpl` (protocol dispatch, push to IPNS), `SyncRequestBuilder` |
| Local IPFS node | `kamu-core` — [`ipfs_service.rs`](../../src/domain/core/src/services/ipfs_service.rs); `kamu` — [`ipfs_service_impl.rs`](../../src/infra/core/src/services/ipfs_service_impl.rs) | `IpfsService` (add a dataset, find an IPNS key, resolve and publish its name), `IpfsServiceImpl`; `kamu system ipfs add` uses it directly |
| Simple protocol client | `kamu` — [`simple_transfer_protocol/`](../../src/infra/core/src/utils/simple_transfer_protocol) | `SimpleTransferProtocol` |
| Smart protocol client trait | `kamu` — [`smart_transfer_protocol.rs`](../../src/infra/core/src/utils/smart_transfer_protocol.rs) | `SmartTransferProtocolClient`, `TransferOptions` |
| Smart protocol client and server | `kamu-adapter-http` — [`src/adapter/http/src/smart_protocol/`](../../src/adapter/http/src/smart_protocol) | `WsSmartTransferProtocolClient`, `AxumServerPullProtocolInstance`, `AxumServerPushProtocolInstance`, messages, `protocol_dataset_helper` |
| Server HTTP endpoints | `kamu-adapter-http` — [`simple_protocol/handlers.rs`](../../src/adapter/http/src/simple_protocol/handlers.rs), [`http_server_dataset_router.rs`](../../src/adapter/http/src/http_server_dataset_router.rs) | `refs`, `blocks`, `data`, `checkpoints` (GET and PUT), `pull` and `push` WebSocket upgrades |
| Committing a pull | `kamu-core` — [`local_sync_committer.rs`](../../src/domain/core/src/services/local_sync_committer.rs); `kamu` — [`local_sync_committer_impl.rs`](../../src/infra/core/src/services/sync/local_sync_committer_impl.rs), [`local_sync_writes.rs`](../../src/infra/core/src/utils/local_sync_writes.rs) | `LocalSyncCommitter`: `HEAD` CAS through `DatasetHeadCommitter`, or creation through `CreateDatasetUseCase`; the `Seed` store and block append both protocols share |
| Appending incoming blocks | `kamu-datasets-services` — `AppendDatasetMetadataBatchUseCaseImpl`; `odf` — `MetadataChainExt::append_batch` | block validation, `HEAD` update on the smart push server |
| Push | `kamu` — [`push_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/push_dataset_use_case_impl.rs), [`push_request_planner_impl.rs`](../../src/infra/core/src/services/push_request_planner_impl.rs); CLI [`push_command.rs`](../../src/app/cli/src/commands/push_command.rs) | `kamu push` |
| Remotes | `kamu` — [`src/infra/core/src/services/remote/`](../../src/infra/core/src/services/remote) | `RemoteRepositoryRegistryImpl`, `RemoteAliasesRegistryImpl`, `RemoteDatasetUrlResolverImpl`, `RemoteAliasResolverImpl`, `DatasetPullSourceResolverImpl` |

---

## 4. Sync requests

A dataset behind a URL is a `SyncRefRemote`: opened through `DatasetFactory` as an `odf::Dataset`
over the URL's storage (file, S3, HTTP, IPFS gateway).

| Request | Source | Destination | Run by |
| --- | --- | --- | --- |
| `SyncRequest` (push) | `ResolvedDataset`: a dataset of this node | `SyncRefRemote`: a dataset that is not a dataset reference of this node | `SyncService::sync`, which also moves the destination's `HEAD` |
| `LocalSyncRequest` (pull) | `DatasetRefRemote`: the transfer resolves and opens it | `LocalSyncTarget`: `Existing { dataset, expected_head }`, or `New(DatasetAlias)` for a dataset the commit creates from the source's `Seed` | `SyncService::transfer_into_local`, then `LocalSyncCommitter::commit` |

A pull's destination is read from the database and its source from the network, so the two are
resolved at different steps. The destination is read as a `LocalSyncTarget`, inside a transaction,
because it reads `HEAD` as `expected_head`: a detached dataset reads the storage `HEAD`, which can
lag behind the database. A dataset entry left without `HEAD` by an interrupted creation is
`Existing` with no expected head. A missing destination becomes `New` when creation is allowed and
the reference carries an alias.

- `PullLocalTarget::resolve_sync_target` reads the destination of a planned pull; it is `None` for
  an existing dataset deleted since planning.
- `LocalSyncHelper::build_request` (`kamu::testing`) does the same from a local reference, for
  tests; it never opens the source.
- `SyncRequestBuilder::build_sync_request(src, dst, create_dst_if_not_exists)` builds a push and
  refuses a local destination. A reference counts as local when `DatasetRefAny::as_local_ref`
  accepts it (a repo-like prefix that is not a registered repository is read as an account);
  otherwise it is resolved to a URL by `RemoteDatasetUrlResolver`
  ([§9](#9-remote-repositories-and-aliases)). The source must have a `HEAD`.

**Pull into a local dataset** reads the target, transfers, then commits:

| Step | Transaction | Does |
| --- | --- | --- |
| Target | the caller's | `PullLocalTarget::resolve_sync_target`: read `expected_head`; no remote is contacted |
| Transfer | none held across it | `transfer_into_local`: resolve the source's URL with `RemoteDatasetUrlResolver`, open it and check it has a `HEAD` (an unknown repository is `RepositoryNotFound`, a remote named only by ID is `RemoteByIdUnsupported`, a source without `HEAD` is `DatasetNotFound`), compare the chains, store the `Seed` of a new dataset, copy objects, append blocks; no reference moves. Returns `SyncTransfer::UpToDate` or `Transferred(TransferredSync)` |
| Commit | the caller's | `LocalSyncCommitter::commit`: for an existing dataset, `DatasetHeadCommitter` moves `HEAD` from `expected_head` to the new head; for a new one, `CreateDatasetUseCase` creates the entry with `HEAD` already at the new head (`CreateDatasetUseCaseOptions::initial_head`) |

The `Seed` of a new dataset is stored through `DatasetStorageUnitWriter` in a short transaction that
writes nothing to the database: database-backed storage only needs one to build the dataset, which
is detached before the transaction ends. Storage takes over a unit left without `HEAD` by an
earlier failed pull, so the next pull continues into it.

`kamu pull` commits in a short transaction of its own after each transfer
([dataset-pull.md](dataset-pull.md#62-per-job-runners)); the update task does the same
([task-system.md](task-system.md#71-update-dataset)). Listeners follow the rule in
[dataset-pull.md](dataset-pull.md#63-listeners).

`SyncOptions`:

| Option | Effect |
| --- | --- |
| `trust_source` | skip block validation and hash recomputation; defaults to `true` for a push, whose source is local, and `false` for a pull |
| `create_if_not_exists` | allow creating the destination (default on) |
| `force` | accept a destination that is ahead or has diverged, and replace its history; never disables the CAS on `HEAD` |
| `dataset_visibility` | visibility of a dataset created by the sync (default private) |

`SyncResult` is `UpToDate` or `Updated { old_head, new_head, num_blocks, increment }`; `old_head:
None` means the destination was created. `increment` is the data between the two heads, read from
the chain that holds the new blocks (the destination on pull, the local source on push); when the
new head does not extend the old one, as after a forced sync over a divergence, it counts the whole
new chain.

---

## 5. Choosing a protocol

`SyncServiceImpl` dispatches on the remote URL: `sync_impl` on a push's destination,
`transfer_into_local_impl` on a pull's source:

| Direction and remote URL | Protocol |
| --- | --- |
| push to `ipfs://` | rejected: a CID changes with every update |
| push to `ipns://<key>` | IPFS add and IPNS publish ([§6.3](#63-ipfs)) |
| pull from `odf+http(s)://` | Smart Transfer Protocol, pull flow ([§7](#7-smart-transfer-protocol)) |
| push to `odf+http(s)://` | Smart Transfer Protocol, push flow |
| anything else (`file://`, `s3://`, `http(s)://`) | Simple Transfer Protocol ([§6](#6-simple-transfer-protocol)) |

The `odf+` prefix is stripped (`odf_to_transport_protocol`) before connecting. A plain `http(s)://`
URL is therefore read with the Simple protocol, and is read-only; a registered repository is turned
into an `odf+` URL when a push target is resolved ([§9](#9-remote-repositories-and-aliases)).

---

## 6. Simple Transfer Protocol

### 6.1 Client: `SimpleTransferProtocol`

The same code serves every non-smart direction: `transfer_into_local` for a pull into a local
dataset, `push` for a file or S3 repository. Both ends
are `odf::Dataset`s, so "remote" only changes which storage the object reads and writes go to.

```mermaid
sequenceDiagram
    participant C as SimpleTransferProtocol
    participant S as Source dataset
    participant D as Destination dataset
    C->>S: resolve refs/head
    Note over C,D: destination head: expected_head of a local target,<br/>refs/head of a remote one
    C->>S: walk blocks/{hash} via prevBlockHash
    C->>D: walk blocks for comparison
    Note over C: compare_chains: Equal → UpToDate<br/>Behind / Diverged → error unless force
    opt local destination does not exist (New)
        C->>D: store the source Seed block, no HEAD, no entry
    end
    par up to max_parallel_transfers
        C->>S: GET data/{physicalHash}, checkpoints/{physicalHash}
        C->>D: insert_stream (hash verified unless trusted)
    end
    C->>D: append blocks oldest first
    opt remote destination
        C->>D: set HEAD with CAS on the head read above
    end
```

1. **Heads and comparison.** `MetadataChainComparator::compare_chains` walks both chains and
   returns `Equal`, `LhsAhead` (the blocks to copy), `LhsBehind` or `Divergence`. A destination
   with no blocks yet, such as a dataset to create, is passed as no chain, so the whole source chain
   is ahead. Without `force`,
   behind is `DestinationAhead` and divergence is `DatasetsDiverged`. With `force`, the whole
   source chain, `Seed` included, is appended onto the destination; this protocol does not compare
   the two `Seed` blocks.
2. **Seed.** For a local destination to create, the source's `Seed` block is stored without
   `HEAD` or entry ([§4](#4-sync-requests)); its computed hash must equal the source's.
3. **Objects.** Every data slice and checkpoint referenced by the new blocks is streamed from the
   source's object repo to the destination's, `max_parallel_transfers` at a time (default 10,
   `SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS`). With a trusted source the declared hash is passed as
   precomputed; otherwise the local filesystem repo recomputes and checks it. The S3 repo requires
   a precomputed hash (it panics without one), so a sync into S3 storage works only from a trusted
   source, and then stores the stream without checking it.
4. **Blocks.** All new blocks are appended at once. A local destination's `HEAD` is left to the
   caller's commit ([§4](#4-sync-requests)), a CAS on `expected_head`. A remote destination is
   written with
   `MetadataChainExt::append_batch_and_set_head`, which appends the blocks and moves `HEAD` by a
   CAS on its storage, since its `HEAD` is not a dataset reference of this node. That CAS is the
   storage's conditional write (`NamedObjectRepository::compare_and_set`): a conditional `PUT` on
   S3; on the local file system, a `head.lock` file created exclusively, then renamed over `head`
   (a lock left by an interrupted writer fails later pushes until it is removed). Storage without
   conditional writes re-reads the head just before an unconditional write, so a change landing in
   between is lost. A destination that moved since step 1 fails with `HeadMoved`, also with
   `force`.

Objects land before the blocks that reference them, so a failure leaves at most unreferenced
objects and blocks, never a block with a missing object. A first pull that fails before its commit
leaves a stored unit without `HEAD` ([§4](#4-sync-requests)).

### 6.2 Server: read-only endpoints

A Kamu node serves every dataset under `/{dataset}` (single-tenant) or `/{account}/{dataset}`
(multi-tenant), so any Simple protocol client can read it:

| Endpoint | Handler | Action checked |
| --- | --- | --- |
| `GET refs/{reference}` | `dataset_refs_handler` | Read |
| `GET blocks/{block_hash}` | `dataset_blocks_handler` | Read |
| `GET data/{physical_hash}`, `GET checkpoints/{physical_hash}` | `dataset_data_get_handler`, `dataset_checkpoints_get_handler` | Read |
| `PUT data/{physical_hash}`, `PUT checkpoints/{physical_hash}` | `dataset_data_put_handler`, `dataset_checkpoints_put_handler` | Write |

The `PUT` endpoints are not part of the Simple protocol: they are the upload fallback of the Smart
protocol ([§7.3](#73-object-transfer)). Each streams the body into the object repo, verifying it
against the hash in the path. Authorization is done by the router's layers ([§10](#10-authorization)).

### 6.3 IPFS

Pushing to `ipns://<key-id>` adds the local dataset directory to the local IPFS node (`ipfs add`,
ignoring `config` and `info`) and publishes the root CID under that IPNS key. The previous head is
read through the IPFS HTTP gateway only if the key resolves locally; an equal head re-publishes the
old CID to keep the IPNS record alive. Only local filesystem datasets can be added. Reading from
IPFS goes through the gateway as an HTTP Simple protocol source. Every operation on the local
node goes through `IpfsService`; `SyncServiceImpl` keeps the push logic: head comparison, `force`
and the increment.

---

## 7. Smart Transfer Protocol

The client is `WsSmartTransferProtocolClient`; the server is `AxumServerPullProtocolInstance` /
`AxumServerPushProtocolInstance`, started by `GET {dataset}/pull` and `GET {dataset}/push` WebSocket
upgrades. The client sends `x-odf-smtp-version` on the upgrade request, and the server rejects a
missing or incompatible version before upgrading. The client sends `Authorization: Bearer` with a
token found by `OdfServerAccessTokenResolver` for the server URL (from `kamu login`). Messages are
JSON; a response that can fail is a `Result` of the success type and a typed error. Field names are
the Rust ones (snake_case), not the camelCase of the ODF AsyncAPI specification.

### 7.1 Pull flow

```mermaid
sequenceDiagram
    participant C as Client (pulling)
    participant S as Server
    participant O as Object storage
    C->>S: GET /pull (WebSocket upgrade, Read checked)
    C->>S: DatasetPullRequest { begin_after: expected_head, stop_at: None, force_update_if_diverged }
    S-->>C: TransferPlan { num_blocks, num_objects, … } or InvalidInterval
    alt num_blocks > 0
        C->>S: DatasetPullMetadataRequest
        S-->>C: blocks after begin_after as a tar.gz
        opt destination does not exist
            Note over C: store the Seed block, no HEAD, no entry
        end
        C->>S: DatasetPullObjectsTransferRequest { objects not present locally }
        S-->>C: one download URL per object (presigned, or the node's GET data/… endpoint)
        par max_parallel_transfers
            C->>O: GET object
        end
        Note over C: append blocks, no reference moves
    end
    Note over C: the caller commits: CAS on begin_after, with or without force
```

The server builds the plan from the chain interval after `begin_after` up to its head, and the
metadata batch ends at that same head even if it moves meanwhile. A `begin_after` that is not an
ancestor of the head is `InvalidInterval`, which the client reports as `DatasetsDiverged`, even
when the local copy is merely ahead of the server; with `force` the server sends the whole chain
instead. When the chain it receives starts with a `Seed` and the target exists (with `force`, or
without a `HEAD`), the client refuses a `Seed` of another dataset ID (`DatasetIdMismatch`) or of
another kind (`OverwriteSeedBlock`).

### 7.2 Push flow

```mermaid
sequenceDiagram
    participant C as Client (pushing)
    participant S as Server
    participant O as Object storage
    Note over C: read remote head through the Simple protocol
    C->>S: GET /push (WebSocket upgrade, logged in, Write checked if the dataset exists)
    C->>S: DatasetPushRequest { current_head, transfer_plan, force_update_if_diverged, visibility_for_created_dataset }
    S-->>C: Accepted, or InvalidHead if current_head is not the server's head
    C->>S: DatasetPushMetadataRequest { blocks as tar.gz }
    S-->>C: Accepted (or SeedBlockOverwriteRestricted)
    opt dataset does not exist on the server
        Note over S: create it from the Seed block, owner = caller, given visibility
    end
    C->>S: DatasetPushObjectsTransferRequest { objects }
    S-->>C: per object: SkipUpload (already present) or an upload URL
    par max_parallel_transfers
        C->>O: PUT object
    end
    loop while uploading
        S->>C: DatasetPushObjectsUploadProgressRequest
        C-->>S: DatasetPushObjectsUploadProgressResponse { Running }
    end
    C->>S: DatasetPushObjectsUploadProgressResponse { Complete }
    C->>S: DatasetPushComplete
    Note over S: append blocks, set HEAD (CAS),<br/>post DatasetExternallyChangedMessage
    S-->>C: DatasetPushCompleteResponse
```

`DatasetPushCompleteResponse` is `Ok(DatasetPushCompleteConfirmed)`, or an error sent as the reason
of a close with code 1013 ("try again later"): `Internal`, or `UpdatedConcurrently` when the CAS on
`HEAD` fails. The conflict carries no heads, since a close frame reason holds at most 123 bytes; the
server logs them. A client that predates the typed response takes any JSON object here, including
the reason of a close with the usual error code 1011, for a bare confirmation; it reads a close
with any other code as a disconnect, so with 1013 it fails the push instead of reporting success.
The current client reads the reason of both codes. The client accepts the bare `{}` confirmation of a server that predates it. Progress
requests carry `progress_requested: true`, so one still in flight is skipped rather than taken for
the confirmation; a server that predates the field sends `{}`, which stays ambiguous.

Server-side details:

- The upgrade requires a logged-in caller. For a dataset that does not exist yet, the account in
  the URL must be the caller's; the dataset is created when the first metadata arrives.
- Blocks are held in memory from the metadata request until completion, then appended in one
  `AppendDatasetMetadataBatchUseCase` call with full validation. Without `force` the batch must
  extend `HEAD`: the commit expects the block the batch is appended to
  (`SetRefCheckRefMode::ExtendsBase`), which initiation checked against
  `current_head`. With `force` the batch may replace a divergent history, so the commit expects
  the request's `current_head` instead, or the `Seed` when the push created the dataset
  (`SetRefCheckRefMode::Explicit`). Either way a head moved after initiation fails the push with
  `UpdatedConcurrently`. A server that predates this expects whatever `HEAD` is at commit time
  under `force`. A batch starting with a `Seed` into an existing dataset, with `force` or without a
  `HEAD`, is refused with `SeedBlockOverwriteRestricted` when that `Seed` is of another dataset.
- While objects upload, the server asks for progress every
  `protocol.smartTransferProtocol.uploadProgressInterval` of the node configuration
  ([config reference](../../resources/config-reference.md#smarttransferprotocolconfig)).
- Completion posts `DatasetExternallyChangedMessage::smart_transfer_protocol_sync` in the commit's
  transaction, which is how flows learn about a push
  ([flow-system.md](flow-system.md#changes-made-outside-flows)).
- Each step that touches the database opens its own short transaction; no transaction spans the
  WebSocket session.

### 7.3 Object transfer

Objects never travel over the WebSocket. For each object the server asks its object repo for an
external URL (`get_external_download_url` / `get_external_upload_url`):

| Storage | URL handed out | Headers |
| --- | --- | --- |
| S3 | a presigned `GET` or `PUT` URL with an expiry | those the presigner requires |
| Local filesystem, other | the node's own `{dataset}/data/{hash}` or `{dataset}/checkpoints/{hash}` endpoint ([§6.2](#62-server-read-only-endpoints)) | the protocol version and the caller's own bearer token |

The client downloads or uploads each object to that URL, at most `max_parallel_transfers` at a
time (default: the machine's available parallelism). On pull the client verifies each object's
hash as it inserts it. On push the server's `PUT` endpoint verifies the hash; a presigned S3
upload is not re-hashed by the server.

---

## 8. Push

`kamu push <dataset>…` builds `PushDatasetUseCase::execute_multi`:

1. **Authorization:** `SyncTo` on every dataset to push, which needs Read
   ([`DatasetOperationGuard`](../../src/domain/datasets/domain/src/services/dataset_operation_guard.rs), [§10](#10-authorization)); any failure
   returns only those failures.
2. **Plan:** `PushRequestPlanner::collect_plan` resolves each dataset's target with
   `RemoteAliasResolver::resolve_push_target` ([§9](#9-remote-repositories-and-aliases)).
3. **Sync requests:** source = the local dataset, destination = the target URL.
4. **Sync:** all requests run concurrently (`join_all`) through `SyncService` with the push's
   `SyncOptions`.
5. **Aliases:** if every push succeeded and `add_aliases` is on (the default; `--no-alias` turns it
   off), a push alias with the target URL is stored on each dataset.

`--recursive` and `--all` are accepted by the CLI but `unimplemented!` in the use case. `--to`
names the target (a URL, a `repo/name` alias, or a repository); `--force` and `--visibility` map
to `SyncOptions`.

---

## 9. Remote repositories and aliases

| Registry | Stores | Where |
| --- | --- | --- |
| `RemoteRepositoryRegistryImpl` | named repositories (`kamu repo add <name> <url>`), URL normalized to end with `/` | one YAML manifest per repository in the workspace `repos` directory |
| `RemoteAliasesRegistryImpl` | per-dataset pull and push aliases (`kamu repo alias add`) | the dataset's `info` storage |

`RemoteDatasetUrlResolverImpl` turns remote references into dataset URLs, and
`RemoteAliasResolverImpl` finds push targets. The URL resolver reads no database, so a transfer
calls it with no transaction held:

| Resolution | Rule |
| --- | --- |
| Pull URL of `repo/name` or `repo/account/name` | `odf+` repository: the repository's `odf+http(s)` URL plus the account (asked from the server's `info` and `accounts/me` when the reference has none and the server is multi-tenant) and the name; other repositories: the repository URL joined with the alias |
| Pull URL of a URL | the URL itself, with a trailing `/` |
| Pull URL of an ID | not supported |
| Push target | `--to` URL as given; otherwise the dataset's single push alias, if it is a URL alias (a `repo/name` push alias is skipped; several aliases are `AmbiguousAlias`); otherwise the only configured repository (none is `EmptyRepositoryList`, several are `AmbiguousRepository`). For a repository, the account comes from the server as above, and the remote dataset name from a lookup of the dataset ID on the server, falling back to the local name |

`DatasetPullSourceResolverImpl` owns what a pull alias means:

| Method | Answer |
| --- | --- |
| `has_pull_source` | whether the dataset has any pull alias, several included; reads the alias registry only. `kamu ingest` and setting a watermark refuse such a dataset, since its history comes from the remote; `kamu verify` replays its transforms only when all its inputs exist locally |
| `find_pull_alias` | the dataset's single pull alias, `None` without one, `AmbiguousPullSourceError` with several; reads the alias registry only |
| `resolve_remote_source` | a `DatasetPullSource { remote_ref, url }`, the URL from `RemoteDatasetUrlResolver::resolve_dataset_url`; an unknown repository is `RepositoryNotFound`, a remote named only by ID is `ByIdUnsupported` |
| `resolve_pull_source` | both of the above for one dataset |
| `find_dataset_by_pull_alias` | the current account's dataset whose pull aliases contain a remote ref: the dataset named as the remote one first, then a scan of the account's datasets |

`DatasetPullSource::to_sync_endpoint` gives the `SyncEndpoint` a sync task reads, without
credentials. `resolve_pull_source`, `resolve_remote_source` and `to_sync_endpoint` have no
production caller: a pull resolves its source URL with `RemoteDatasetUrlResolver` during the
transfer.

How `kamu pull` uses pull aliases to decide that a dataset is synced belongs to
[dataset-pull.md](dataset-pull.md#51-resolving-each-request).

---

## 10. Authorization

| Path | Check |
| --- | --- |
| Server, every `{dataset}/…` route | `DatasetResolverLayer` resolves the dataset from the path (optional only for `/push`); `DatasetAuthorizationLayer` requires **Write** for `/push` and every unsafe method (anything but `GET`, `HEAD`, `OPTIONS` and `TRACE`), **Read** otherwise. With `allowAnonymous=false`, `AuthPolicyLayer` rejects anonymous callers first |
| Server push to a new dataset | logged-in caller; the account in the path must be the caller's |
| `kamu pull` sync jobs | `SyncFrom`: Write on an existing destination ([dataset-pull.md](dataset-pull.md#7-authorization)) |
| `kamu push` | `SyncTo`: Read on each pushed dataset; the remote server decides about the destination |
| Update task sync jobs | none at run time ([dataset-pull.md](dataset-pull.md#8-server-use)) |

`SyncService` and `SyncRequestBuilder` resolve local datasets through the plain registry and check
nothing themselves; callers own authorization.

---

## 11. Committing, validation and errors

| Writer | Validation of incoming blocks | `HEAD` update |
| --- | --- | --- |
| Simple protocol, local source (push) | none (trusted), hashes not recomputed | CAS on the remote head read at the start |
| Simple protocol, remote source (pull) | full, hashes recomputed | CAS on `expected_head`, read just before the transfer |
| Smart pull client | full | CAS on `expected_head`, read just before the transfer, with or without `force` |
| Smart push server | full | CAS on the first new block's `prevBlockHash`; with `force`, on the request's `current_head` (the `Seed` for a dataset the push created) |

Every block except the `Seed` of a newly created dataset is appended by
`MetadataChainExt::append_batch` (IPFS publishing aside), which moves no reference; `HEAD` is set
once at the end, so readers see either the old or the new head. For a local destination
`LocalSyncCommitter` moves it through `DatasetHeadCommitter`, or creates the dataset at it; the
smart push server uses `AppendDatasetMetadataBatchUseCase`, which does both. Setting `HEAD` posts
`DatasetReferenceMessage`; the smart push server also posts `DatasetExternallyChangedMessage`.

The variants are in `SyncError` (`sync_service.rs`). A lost `HEAD` CAS is `HeadMoved` on every
destination, with a `SyncHeadMovedError` that carries the expected and actual heads when they are
known: always for a local destination and the Simple protocol. The Smart push server reports the
heads in `InvalidHead` when `HEAD` moved before the push began, and none in
`DatasetPushCompleteResponse` when it moved during the commit. Two mappings are
surprising: a Smart-protocol push onto a remote that is ahead fails with `DatasetsDiverged`, not
`DestinationAhead`; and server-side internal failures reach the client as `Internal` with the
generic message "Internal error", the cause staying in the server log (the one exception is a
failure while computing the pull transfer plan, whose message is forwarded as is). In a phase whose
reply has no `Internal` case, the client still reads that reply and reports the server's failed
phase. Of the typed
protocol errors, `InvalidInterval` (→ `DatasetsDiverged` on pull), `SeedBlockOverwriteRestricted`
(→ `OverwriteSeedBlock` on push; on pull the client makes the `Seed` check itself), `RefCollision`
and `NameCollision` keep their meaning. A metadata batch or an object download that cannot be read
fails the transfer with an error; a download answered with an error status is `Internal`, so it is
recoverable.

`SyncError::is_recoverable` says whether running the same sync again could succeed. A lost CAS
(`HeadMoved`) and `Internal`, which also covers network failures, are
recoverable; conflicts between the chains, a corrupted source, denied access and errors in the
request, such as an `InvalidDestination`, are not. `CreateDatasetFailed`
is recoverable for a lost CAS, an internal failure and a `RefCollision`, where a concurrent first
pull created the same dataset and the next attempt finds it. The update task runner turns the flag
into the recoverability of the failed task.

---

## 12. Testing & gotchas

### Testing

- Sync service tests: `src/infra/core/tests/tests/test_sync_service_impl.rs`; the transfer and
  commit split, first pull, forced pull and a `HEAD` moved during the transfer, on local FS and
  S3: `test_sync_service_local_commit.rs`. `kamu::testing::LocalSyncHelper` pulls the way the use
  case does.
- Smart protocol tests, client against an in-process server, single- and multi-tenant, local and
  S3 storage: `src/adapter/http/tests/tests/test_protocol_dataset_helpers.rs` and the
  `tests_pull` / `tests_push` scenario directories under `src/adapter/http/tests/tests/`. The
  local FS server harness runs on `InMemoryTransactionManager`, so a failed push rolls the dataset
  repositories back as a database does (files it wrote to storage stay); the racing doubles in
  `kamu_datasets_services::testing` move `HEAD` in a transaction of their own, as a concurrent
  writer does.
- CLI and node end-to-end scenarios: `src/e2e/app/cli/repo-tests/src/test_smart_transfer_protocol.rs`,
  `commands/test_pull_command.rs`, `commands/test_repo_command.rs`.

### Behaviour by design

| Area | What happens |
| --- | --- |
| Plain `http(s)://` URLs | Read-only through the Simple protocol; a push to a *registered* `http(s)` repository is rewritten to `odf+http(s)` by `resolve_push_target` and uses the Smart protocol |
| Created destination | Created by the commit, with its `HEAD` at the last transferred block; for a pull that fails before it, see [§4](#4-sync-requests) |
| Unreferenced objects | A failed or concurrent sync can leave uploaded objects that no block references |
| `force` | Replaces the destination's history from the source, but only from the destination head the sync observed: a head moved meanwhile fails with `HeadMoved`. The Smart protocol refuses a different `Seed` (`OverwriteSeedBlock`, `SeedBlockOverwriteRestricted`); the Simple protocol does not check |
| Trusted local source | A push from a local dataset skips validation and hash recomputation |

---

## 13. File/crate reference map

| Concern | File |
| --- | --- |
| Sync types and errors | [`sync_service.rs`](../../src/domain/core/src/services/sync_service.rs) |
| Committing a pull into a local dataset | [`local_sync_committer.rs`](../../src/domain/core/src/services/local_sync_committer.rs), [`local_sync_committer_impl.rs`](../../src/infra/core/src/services/sync/local_sync_committer_impl.rs) |
| `Seed` store and block append shared by both protocols | [`local_sync_writes.rs`](../../src/infra/core/src/utils/local_sync_writes.rs) |
| Protocol dispatch, push to IPNS, URL helpers | [`sync_service_impl.rs`](../../src/infra/core/src/services/sync/sync_service_impl.rs) |
| Local IPFS node | [`ipfs_service.rs`](../../src/domain/core/src/services/ipfs_service.rs), [`ipfs_service_impl.rs`](../../src/infra/core/src/services/ipfs_service_impl.rs) |
| Sync ref resolution | [`sync_request_builder.rs`](../../src/infra/core/src/services/sync/sync_request_builder.rs) |
| Chain comparison | [`metadata_chain_comparator.rs`](../../src/domain/core/src/utils/metadata_chain_comparator.rs) |
| Simple protocol client | [`simple_transfer_protocol/`](../../src/infra/core/src/utils/simple_transfer_protocol) |
| Smart protocol client | [`ws_tungstenite_client/`](../../src/adapter/http/src/smart_protocol/ws_tungstenite_client), [`smart_transfer_protocol.rs`](../../src/infra/core/src/utils/smart_transfer_protocol.rs) |
| Smart protocol server | [`axum_server_pull_protocol.rs`](../../src/adapter/http/src/smart_protocol/axum_server_pull_protocol.rs), [`axum_server_push_protocol.rs`](../../src/adapter/http/src/smart_protocol/axum_server_push_protocol.rs) |
| Messages, phases, transfer plans, tarballs, object URLs | [`messages.rs`](../../src/adapter/http/src/smart_protocol/messages.rs), [`phases.rs`](../../src/adapter/http/src/smart_protocol/phases.rs), [`protocol_dataset_helper/`](../../src/adapter/http/src/smart_protocol/protocol_dataset_helper) |
| Server endpoints and routing | [`handlers.rs`](../../src/adapter/http/src/simple_protocol/handlers.rs), [`http_server_dataset_router.rs`](../../src/adapter/http/src/http_server_dataset_router.rs), [`dataset_authorization_layer.rs`](../../src/adapter/http/src/middleware/dataset_authorization_layer.rs) |
| Push | [`push_dataset_use_case_impl.rs`](../../src/infra/core/src/use_cases/push_dataset_use_case_impl.rs), [`push_request_planner_impl.rs`](../../src/infra/core/src/services/push_request_planner_impl.rs), [`push_command.rs`](../../src/app/cli/src/commands/push_command.rs) |
| Remote repositories, aliases, URL resolution | [`src/infra/core/src/services/remote/`](../../src/infra/core/src/services/remote) |
