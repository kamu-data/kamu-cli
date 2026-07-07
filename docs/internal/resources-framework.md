# Resources Framework — Architecture

> **Status:** prototype, under active development on branch
> `feature/1609-managed-resources-prototype-stage1`.
> This document describes the framework *as it stands today*. Type names, paths, and field lists
> below are drawn from source — when they drift, treat the source as canonical and update this page.

---

## Agent / newcomer quick-start

**One-paragraph mental model.** The resources framework is a generic, event-sourced,
Kubernetes-inspired subsystem for *declarative* management of arbitrary "resource types". A user
authors a **manifest** (`$schema` + `headers` + `spec`); the framework stores it as an event-sourced
aggregate, fills in server-owned headers (id, timestamps, `generation`) and an **initial**
server-owned `status`, then **asynchronously reconciles** it toward the desired state (the status
progresses `Pending → Reconciling → Ready`/`Failed`). Every type (today: `VariableSet`, `SecretSet`)
implements a small set of traits and registers a **dispatcher** keyed by its canonical schema URL.
All callers — the CLI and the GraphQL API — go through a single seam, the **`ResourceFacade`** trait,
which has an in-process (`Local`) implementation and a `RemoteGraphql` implementation that talks to
a remote server.

**Where to start reading, by intent:**

| You want to… | Start at |
| --- | --- |
| Know the rules I must not break | [§3 Invariants](#3-invariants) |
| Understand the core abstractions | [§5 Domain model](#5-domain-model-kamu-resources) |
| Know what a user types vs what the server generates | [§5a Resource anatomy](#5a-resource-anatomy--input-vs-auto-generated) |
| Understand storage, uniqueness, soft-delete | [§6 Persistence model](#6-persistence-model) |
| Understand account scoping & permissions | [§7 Account resolution & authorization](#7-account-resolution--authorization) |
| Trace an `apply` / reconcile end-to-end | [§13 Data flow](#13-data-flow-walkthroughs) |
| Add a new resource type | [§14 Concrete types + recipe](#14-concrete-resource-types-kamu-configuration) |
| Find the file for X | [§16 Reference map](#16-filecrate-reference-map) |
| Avoid common traps | [§17 Gotchas](#17-extension-points--gotchas) |

**Build & test:**

```bash
cargo build
cargo nextest run -E 'test(test_apply_resource_use_case)'
make clippy
```

> `SQLX_OFFLINE=true` is needed only when there's no reachable Postgres — e.g. agents running with
> limited permissions. Human developers with the local Docker Postgres/Elasticsearch containers up
> can omit it (SQLx validates against the live DB instead of the offline cache).

---

## Table of contents

- [Resources Framework — Architecture](#resources-framework--architecture)
  - [Agent / newcomer quick-start](#agent--newcomer-quick-start)
  - [Table of contents](#table-of-contents)
  - [1. Purpose \& scope](#1-purpose--scope)
  - [2. Concept glossary](#2-concept-glossary)
  - [3. Invariants](#3-invariants)
  - [4. Layered architecture](#4-layered-architecture)
  - [5. Domain model (`kamu-resources`)](#5-domain-model-kamu-resources)
    - [Core traits](#core-traits)
    - [Events](#events)
    - [Repository](#repository)
    - [Dispatchers](#dispatchers)
    - [Use-case traits](#use-case-traits)
    - [5a. Resource anatomy — input vs auto-generated](#5a-resource-anatomy--input-vs-auto-generated)
  - [6. Persistence model](#6-persistence-model)
  - [7. Account resolution \& authorization](#7-account-resolution--authorization)
  - [8. Rename \& conflict rules](#8-rename--conflict-rules)
  - [9. Services (`kamu-resources-services`)](#9-services-kamu-resources-services)
  - [10. Facade (`kamu-resources-facade`)](#10-facade-kamu-resources-facade)
  - [11. GraphQL API](#11-graphql-api)
  - [12. CLI](#12-cli)
    - [CLI semantics matrix](#cli-semantics-matrix)
  - [13. Data flow walkthroughs](#13-data-flow-walkthroughs)
    - [(a) `kamu apply -f manifest.yaml`](#a-kamu-apply--f-manifestyaml)
    - [(b) Reconciliation](#b-reconciliation)
    - [Outbox connections](#outbox-connections)
    - [How reconciliation is scheduled](#how-reconciliation-is-scheduled)
    - [Lifecycle state machine](#lifecycle-state-machine)
  - [14. Concrete resource types (`kamu-configuration`)](#14-concrete-resource-types-kamu-configuration)
    - [Secret handling invariant](#secret-handling-invariant)
    - [Recipe: how to add a new resource type](#recipe-how-to-add-a-new-resource-type)
  - [15. Tests](#15-tests)
    - [Testing policy — what belongs where](#testing-policy--what-belongs-where)
  - [16. File/crate reference map](#16-filecrate-reference-map)
  - [17. Extension points \& gotchas](#17-extension-points--gotchas)

---

## 1. Purpose & scope

The framework provides a uniform way to **declaratively manage** typed resources:

- **CRUD + reconcile** — `apply` (create/update), `get`, `list`, `delete` (selectors support `%`
  name patterns), plus a `reconcile` step (outbox-driven, asynchronous — see
  [How reconciliation is scheduled](#how-reconciliation-is-scheduled)) that drives a resource toward
  its desired state.
- **Kubernetes-style model** — every resource carries user-authored `headers` + `spec`. The server
  maintains the remaining headers — including `generation` (the desired-state revision, bumped on
  each spec/headers change) — and a `status` with `phase`, `observedGeneration` (reconciliation
  progress), and `conditions`. Reconciliation is needed whenever `observedGeneration < generation`.
- **Pluggable types** — the generic machinery is type-parameterized over a resource type `R`;
  concrete types plug in via traits + registered dispatchers keyed by canonical schema URL.
- **Local & remote symmetry** — the same operations run in-process or against a remote server behind
  one trait (`ResourceFacade`).
- **Event-sourced & transactional** — mutations are recorded as immutable events; lifecycle changes
  are announced on the transactional **outbox**.

**Not in scope here:** the supported / production-ready types today are `VariableSet` and `SecretSet`
(below). A third type, `Storage`, is already **registered** in the CLI catalog and wired through the
same machinery but is **incomplete / WIP** — see [§14](#14-concrete-resource-types-kamu-configuration).
Datasets and flows are **not** resources today, though bringing them under the framework is a
long-term goal. This page documents what exists now.

---

## 2. Concept glossary

| Term | Meaning |
| --- | --- |
| **Resource** | A single managed object instance of a given type, identified by a `ResourceID`. |
| **Schema** | The canonical resource type identity URL, e.g. `https://opendatafabric.org/schemas/config/v1alpha1/VariableSet`. Carried in code as a `TypeUri` (opaque identity value); a parsed lens over it is `ResourceSchemaId` (see [§5a](#5a-resource-anatomy--input-vs-auto-generated)). Its last path segment is a `TypeName` (ODF RFC-018 schema type name, e.g. `VariableSet`), obtained via `ResourceSchemaId::type_name()`. |
| **Selector name / alias** | A resource *type's* user-facing name: either the canonical presentation name, e.g. `variablesets`, `secretsets`, or a short alias, e.g. `vs`, `ss`. Both are carried as `ResourceSelectorName` — the canonical/alias distinction is a matter of which field holds the value (`canonical_selector` vs. `selector_aliases`), not a separate type. Not an ODF `TypeName` and not persisted as the resource type identity. |
| **Resource type selector** | Raw user/API input identifying a resource *type* before resolution (matches either a selector name or alias); carried as `ResourceTypeSelectorRaw`. Not to be confused with **Selector** below, which identifies a resource *instance*. |
| **Descriptor** | The schema (`TypeUri`) plus selector name/aliases identifying a resource type for dispatcher routing and presentation; the domain type is `ResourceTypeDescriptor`, carried in the `dill` registry as `ResourceDispatcherMeta`. |
| **Manifest** | The user-authored wire document (`$schema`/`headers`/`spec`) in YAML or JSON. |
| **Spec** | The desired-state portion authored by the user; stored as `serde_json::Value`. |
| **Status** | Server-owned observed state (`phase`, `observedGeneration`, `conditions`). Note `generation` lives in **headers**, not status. |
| **Snapshot** | The persisted materialized form of a resource (`ResourceSnapshot`). |
| **Phase** | Lifecycle stage: `Pending`, `Reconciling`, `Ready`, `Failed` — matches ODF RFC-018's `ResourcePhase` exactly (see [§13 state machine](#lifecycle-state-machine)). |
| **Condition** | A K8s-style condition entry contributing to the overall phase. |
| **generation / observedGeneration** | `generation` bumps on each spec/headers change; `observedGeneration` records the last generation reconciliation observed. Drift ⇒ reconcile. |
| **Reconciliation** | The act of driving actual state toward the spec (e.g. `SecretSet` materializes its encrypted read-side projection). |
| **Selector** | Identifies one (`ResourceSelector`) or many (`ResourceBatchSelector`) resource *instances*, by name or UID, optionally scoped to an account. Distinct from **Resource type selector** above, which identifies a *type*, not an instance. |
| **SpecViewMode** | How sensitive spec fields are rendered. Two modes only: `Encrypted` (default — the stored ciphertext envelope is returned as-is) and `Revealed` (decrypted plaintext). There is no separate "redacted/placeholder" mode today. |
| **Dispatcher** | Per-type adapter (`ResourceCrudDispatcher`, …) registered in `dill` and looked up by schema or selector metadata. |
| **Facade** | The single API seam (`ResourceFacade`) used by all callers; local or remote-GraphQL impl. |
| **TypeRef** | A label/annotation *key*: either a short `TypeName` (e.g. `env`) or a full `TypeUri` schema URI (e.g. `https://opendatafabric.org/schemas/labels/v1/Team`), per ODF RFC-018. Carried as `odf::metadata::resource::TypeRef` (`Uri(TypeUri) \| Name(TypeName)`); `Ord`, so usable as a `BTreeMap` key; serializes as a plain string. |
| **Labels / Annotations** | `headers.labels`/`headers.annotations` are `BTreeMap<TypeRef, serde_json::Value>` (`ResourceLabels`/`ResourceAnnotations` — thin aliases over the ODF-generated containers), i.e. arbitrary hierarchical JSON values keyed by `TypeRef`, not flat `String → String` maps. Per RFC-018 the only semantic difference is that labels are meant to be indexed/queryable via selectors and annotations are not — **indexing is not yet implemented** (deferred). |

---

## 3. Invariants

The rules below hold across the framework. They are the contract a maintainer (or coding agent) must
not break; most are enforced in code and exercised by tests — pointers given where useful.

- **`ResourceID` is immutable and server-allocated.** A new resource's UID comes from
  `GenericResourceQueryService::allocate_uid()`; callers cannot choose it. Once assigned it never
  changes (it is the primary key — see [§6](#6-persistence-model)).
- **`(account_id, schema, name)` is unique.** Enforced by a DB unique constraint
  `UNIQUE (account_id, resource_schema, resource_name)`. Names are stored lowercased.
- **`$schema` is the resource type identity.** A schema URL is parsed into base/context/version/name
  for validation and display, but dispatch and persistence compare the full canonical schema — carried
  as a `TypeUri` and string-equal on the wire/in storage (the dill registry key is an equivalent
  `&'static str`, see [§9](#9-services-kamu-resources-services)).
- **Selectors are presentation names, not manifest identity.** CLI and GraphQL selectors still use
  friendly selector names and aliases (`variablesets`, `vs`, `secretsets`, `ss`), typed as
  `ResourceTypeSelectorRaw` before resolution, which are resolved to a `ResourceTypeDescriptor.schema`
  before repository or dispatcher access.
- **`headers.generation` changes only when desired state changes.** It starts at 1 on create and is
  bumped by the aggregate only when an apply produces a real headers/spec change (`Update`); an
  unchanged apply is `Untouched` and does not bump it.
- **`status.observedGeneration <= headers.generation`** always. Reconciliation sets
  `observedGeneration` to the generation it just processed; `needs_reconciliation()` is exactly
  `observedGeneration < generation`.
- **`status` is never accepted from manifests.** It is server-owned end to end; manifests use
  `deny_unknown_fields`, so a `status` key is rejected ([§5a](#5a-resource-anatomy--input-vs-auto-generated)).
- **`SecretSet` plaintext never crosses a durable boundary.** It is encrypted by the spec sanitizer
  *before the first event/snapshot write*; plaintext must not appear in events, snapshots, the
  read-side projection, logs, GraphQL responses, CLI output, diffs, or outbox payloads
  ([Secret handling](#secret-handling-invariant)).
- **Local and remote facade behavior must match for all contract-tested cases.** The `contract_test!`
  suite runs each case against both implementations; new facade behavior must be added there
  ([§15](#15-tests)).
- **Batch operations preserve the positional `request_index`.** `BatchResourceResponse` reports each
  success/problem tagged with the originating request index, so partial results map back to inputs.
- **A resource is always scoped to exactly one account**, fixed by the persistence key; cross-account
  visibility is denied by treating out-of-account UIDs as not-found ([§7](#7-account-resolution--authorization)).

---

## 4. Layered architecture

Dependency injection is handled by **`dill`** (a catalog/IoC container). Each crate exposes a
`register_dependencies(&mut CatalogBuilder)` that the application composes in order.

```mermaid
flowchart TD
    subgraph adapters["Entry points"]
      CLI["CLI commands<br/>(app/cli)"]
      GQL["GraphQL adapter<br/>(adapter/graphql)"]
    end

    CLISVC["CLI resource services<br/>(facade factory, discovery,<br/>selection, summary)"]
    FACADE{{"ResourceFacade<br/>(single API seam)"}}
    LOCAL["LocalResourceFacadeImpl"]
    REMOTE["RemoteGraphqlResourceFacadeImpl<br/>(cynic GraphQL client)"]

    subgraph domain["Domain + services"]
      REG["Dispatcher registry<br/>lookup by schema<br/>or selector metadata"]
      DISP["ResourceCrudDispatcher&lt;R&gt;<br/>per-type"]
      UC["Use cases&lt;R&gt;<br/>apply / reconcile / get / list / delete"]
    end

    STORE[("Event store + ResourceRepository<br/>(snapshots)")]
    OUTBOX[("Outbox<br/>ResourceLifecycleMessage")]

    CLI --> CLISVC --> FACADE
    GQL --> FACADE
    FACADE --> LOCAL
    FACADE --> REMOTE
    REMOTE -.HTTP GraphQL.-> GQL
    LOCAL --> REG --> DISP --> UC
    UC --> STORE
    UC --> OUTBOX
```

Note the loop: the **remote** facade implementation calls back into the GraphQL adapter of a remote
server, whose resolvers in turn use a **local** facade there. Local and remote thus share contract
behavior (verified by the same contract tests — see [§15](#15-tests)).

**Crates:**

| Crate | Path | Role |
| --- | --- | --- |
| `kamu-resources` | `src/domain/resources/domain` | Base domain model: traits, values, manifests, events, repo & dispatcher interfaces, use-case traits, messages. |
| `kamu-resources-services` | `src/domain/resources/services` | Implementations: loaders, persistence, query services, use-case macros, dispatcher macros + registry, message handlers. |
| `kamu-resources-facade` | `src/domain/resources/facade` | The `ResourceFacade` trait + `Local` and `RemoteGraphql` implementations. |
| `kamu-resources-facade-tests` | `src/domain/resources/facade-tests` | Cross-implementation contract tests enforcing local/remote symmetry for covered behavior. |
| `kamu-configuration` / `kamu-configuration-services` | `src/domain/configuration/{domain,services}` | Concrete types: `VariableSet`, `SecretSet`. |

---

## 5. Domain model (`kamu-resources`)

Module layout: `core/`, `state/`, `values/`, `validation/`, `views/`, `manifests/`, `services/`,
`repo/`, `dispatchers/`, `messages/`, `use_cases/`.

### Core traits

A resource is layered as a stack of traits. The base is **`DeclarativeResource`** — a resource has a
`Spec`, a `Status`, and a backing `ResourceState`
([`core/declarative_resource.rs`](/src/domain/resources/domain/src/core/declarative_resource.rs)):

```rust
pub trait DeclarativeResource:
    Sized + Send + Sync + std::fmt::Debug + AsRef<Self::ResourceState>
{
    type Spec: std::fmt::Debug + Send + Sync;
    type Status: ResourceStatusLike + std::fmt::Debug;
    type ResourceState: DeclarativeResourceState<Spec = Self::Spec, Status = Self::Status>
        + TryFrom<ResourceSnapshot, Error = InternalError>
        + From<Self>;

    fn id(&self) -> &ResourceID;
    fn headers(&self) -> &ResourceHeaders;
    fn spec(&self) -> &Self::Spec;
    fn status(&self) -> &Self::Status;
}
```

**`ReconcilableResource`** adds the lifecycle + reconciliation transitions
([`core/reconcilable_resource.rs`](/src/domain/resources/domain/src/core/reconcilable_resource.rs)).
Note that `needs_reconciliation()` is derived from `generation` vs the status:

```rust
pub trait ReconcilableResource: DeclarativeResource {
    type ReconcileSuccess;
    type ReconcileError: ResourceReconcileError;
    type ReconcileFailureDetails;
    type LifecycleError;

    fn needs_reconciliation(&self) -> bool { /* observed_generation vs generation */ }

    fn try_create(now, id, headers: ResourceHeadersInput, spec) -> Result<Self, LifecycleError>;
    fn try_update_headers(&mut self, now, new_headers: ResourceHeadersInput) -> ...;
    fn try_update_spec(&mut self, now, new_spec: Self::Spec) -> ...;
    fn try_delete(&mut self, now, tombstone_name: String) -> ...;
    fn try_mark_reconciliation_started(&mut self, now) -> ...;
    fn try_mark_reconciliation_succeeded(&mut self, now, expected_generation, success) -> ...;
    fn try_mark_reconciliation_failed(&mut self, now, expected_generation, error) -> ...;
}
```

**`ReconcilableEventSourcedResource`** binds the resource to the `event-sourcing` crate — its
`ResourceState` is an event `Projection` over `ReconcilableResourceEvent<Spec, Success, FailureDetails>`
and an aggregate. This is the bound that all generic use cases require.

**Presentation & descriptor traits:**

- **`ResourceSchemaProvider`** — exposes `fn schema() -> &'static TypeUri`, the canonical schema URL
  (as a `TypeUri`) for the resource, backed by a `'static` value (a `LazyLock<TypeUri>` from the ODF
  codegen) ([`core/resource_descriptor.rs`](/src/domain/resources/domain/src/core/resource_descriptor.rs)).
- **`ResourcePresentation`** — exposes a `ResourcePresentationDefinition` with `canonical_selector:
  ResourceSelectorName`, `selector_aliases: &'static [ResourceSelectorName]`, and per-type list
  columns for table/`list` rendering.

  > Routing to the right dispatcher no longer goes through a `ResourceDescriptor`/`DESCRIPTOR`
  > const — the registry keys dispatchers on `dill` metadata (`ResourceDispatcherMeta`, carrying the
  > schema as a `&'static str` plus selector name/aliases as raw `&'static str`/`&'static [&'static
  > str]`, since dill's `#[meta]` requires const-evaluable values) and compares it against the
  > target `TypeUri` / selector. See [§9](#9-services-kamu-resources-services).

### Events

`ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>` is the event-sourcing alphabet:
`Created`, `HeadersUpdated`, `SpecUpdated`, `Deleted`, `ReconciliationStarted`,
`ReconciliationSucceeded`, `ReconciliationFailed`. The `ResourceState` projection folds these into
current state.

### Repository

`ResourceRepository` (`repo/`) is the persistence seam: allocate UID, create/update snapshot
(with optimistic `expected_last_event_id`), find by name/id, search identities, and stream UIDs /
snapshots by schema.

### Dispatchers

The generic code can't name a concrete `R` at the API boundary, so dynamic dispatch is keyed by
descriptor. `ResourceCrudDispatcher` is the main one (also `ResourcePresentationDispatcher`,
`ResourceLifecycleEventDispatcher`, and a spec-view dispatcher that reveals/decrypts sensitive
spec fields on request). Each carries
schema plus presentation metadata as `dill` metadata for registry lookup
(see [§9](#9-services-kamu-resources-services)).

### Use-case traits

Generic, `R`-parameterized contracts in `use_cases/`: `ApplyResourceUseCase<R>` (two-phase, below),
`ReconcileResourceUseCase<R>`, `GetResourceByUidUseCase<R>`, `ListResourcesByTypeUseCase<R>`,
`DeleteResourcesUseCase<R>`, plus the non-generic `ListAllResourcesUseCase` and
`DeleteAccountResourcesUseCase`.

### 5a. Resource anatomy — input vs auto-generated

> **This is the part to get right when authoring or generating manifests.** Only a subset of a
> resource is user-authored; everything else is owned and produced by the framework.

**(1) User-authored — the manifest.** `ResourceManifest`
([`manifests/resource_manifest.rs`](/src/domain/resources/domain/src/manifests/resource_manifest.rs)):

```rust
pub struct ResourceManifest {
    #[serde(rename = "$schema")]
    pub schema: ResourceSchemaId,            // required — canonical schema URL
    pub headers: ResourceManifestHeaders,
    pub spec: serde_json::Value,             // desired state; type-specific shape
}

#[serde(deny_unknown_fields)]                // ← unknown fields (e.g. `status`) are rejected
pub struct ResourceManifestHeaders {
    pub id: Option<ResourceID>,              // optional — NOT assignable; an exact pointer to an
                                             // existing resource for updates (e.g. when renaming)
    pub account: Option<ResourceAccountRef>, // optional — by name, id, or both; defaults to caller
    pub name: ResourceName,                  // required
    pub description: Option<String>,
    pub labels: Vec<(TypeRef, serde_json::Value)>,
    pub annotations: Vec<(TypeRef, serde_json::Value)>,
}
```

A user may write **only**: `$schema`, `headers.{id?, account?, name, description?, labels,
annotations}`, and `spec`. `deny_unknown_fields` means a manifest **cannot** carry `status`,
timestamps, or `generation` — those are server-owned.

> **`TypeUri` vs `ResourceSchemaId`.** Both model the *same* `$schema` attribute at two levels.
> `TypeUri` is the opaque identity value carried through fields, storage, and the wire
> (`ResourceSnapshot.schema`, dispatcher lookup, outbox payloads). `ResourceSchemaId` is a parsed
> *lens* over it — it wraps a `TypeUri` (`typ`) and exposes the decomposed `base`/`context`/`version`/
> `name` segments used for validation and display. The manifest deserializes `$schema` as
> `ResourceSchemaId` (so a malformed URL is rejected at parse time and the segments are available);
> everything downstream carries the plain `TypeUri`. Both serialize byte-identically to the schema
> URL string, so there is no wire or storage difference between them.

> **`ResourceAccountRef`** is a re-export of ODF's generated `auth::AccountRef` enum
> (`Id(AccountID) | Name(AccountName) | Both { id, name }`), reused verbatim rather than
> hand-rolled — the same "adopt the codegen type directly" pattern as `ResourceID`/`ResourceName`.
> A manifest's `account` field, and every facade selector's `account` field, share this one type.
> Because the enum has no empty variant, `headers.account: {}` is rejected at manifest
> deserialization time (via the ODF codegen's own YAML/JSON serde proxy for this type) with the
> message "AccountRef must specify id or name or both" — a manifest can no longer represent the
> nonsensical "account block present but empty" state that the old hand-rolled
> `{name: Option<String>, id: Option<AccountID>}` struct allowed.

The `id` is **not** something the user assigns — a new resource's UID is always allocated by the
server. It may only be *supplied* on a manifest to point at an already-existing resource for an
update; this is what lets a resource be renamed (the `id` keeps the identity stable while `name`
changes). Omit it for normal create/update-by-name.

**(2) Framework-generated — the rest of headers + all of status.**
`ResourceHeaders` ([`values/resource_headers.rs`](/src/domain/resources/domain/src/values/resource_headers.rs))
and `ResourceStatus` ([`state/resource_status.rs`](/src/domain/resources/domain/src/state/resource_status.rs)):

```rust
pub struct ResourceHeaders {
    pub account: odf::AccountID,             // resolved from manifest account / caller
    pub name: ResourceName,                  // (authored)
    pub description: Option<String>,         // (authored)
    pub labels: ResourceLabels,              // (authored) BTreeMap<TypeRef, serde_json::Value>
    pub annotations: ResourceAnnotations,    // (authored) BTreeMap<TypeRef, serde_json::Value>
    pub generation: u64,                     // generated — bumps on spec/headers change
    pub created_at: DateTime<Utc>,           // generated
    pub updated_at: DateTime<Utc>,           // generated
    pub deleted_at: Option<DateTime<Utc>>,   // generated (soft-delete tombstone)
}

pub struct ResourceStatus {                  // entirely server-owned
    pub phase: ResourcePhase,                // Pending|Reconciling|Ready|Failed
    pub observed_generation: u64,
    pub conditions: Vec<ResourceCondition>,
}
```

> **`ResourcePhase`** is a `pub type ResourcePhase = odf::metadata::resource::ResourcePhase;` alias —
> the same "adopt the codegen type directly" pattern as `ResourceID`/`TypeUri`/`ResourceAccountRef`
> above. Because the ODF dto has no direct `Serialize`/`Deserialize` (only the YAML-manifest layer
> does, via its own codegen'd shadow proxy), every struct with a `ResourcePhase`-typed field annotates
> that field with `#[serde_with::serde_as] ... #[serde_as(as =
> "odf::metadata::serde::yaml::resource::ResourcePhase")]` rather than deriving serde for free — this
> is a deliberate per-field compromise, not a bug; `Display`/`FromStr` are provided directly on the
> ODF dto (`src/odf/metadata/src/serde/yaml/derivations_extra.rs`) so CLI/string round-tripping needs
> no per-crate workaround.

> **`ResourceLabels`/`ResourceAnnotations`** follow the same `#[serde_as]` pattern as `ResourcePhase`
> above — `pub type ResourceLabels = odf::metadata::resource::ResourceLabels;` (and the annotations
> equivalent), each a thin `{ entries: BTreeMap<TypeRef, serde_json::Value> }` container with no direct
> `Serialize`/`Deserialize` of its own, only via the YAML shadow proxy
> (`odf::metadata::serde::yaml::resource::{ResourceLabels, ResourceAnnotations}`). Every struct with a
> `ResourceLabels`/`ResourceAnnotations`-typed field (`ResourceHeaders`, `ResourceHeadersInput`,
> `ResourceViewHeaders`, and the local CLI render structs) annotates that field with
> `#[serde_as(as = "odf::metadata::serde::yaml::resource::ResourceLabels")]` (or the annotations
> variant). Repositories that round-trip through raw `serde_json::Value` columns (Postgres/SQLite) go
> through the free functions `resource_labels_from_json`/`resource_labels_to_json` (and the annotations
> equivalents) in `values/resource_labels_annotations.rs` rather than repeating the proxy dance inline.
> The Cynic (remote-facade) client mirrors the same idea with its own `scalars::{ResourceLabels,
> ResourceAnnotations}` newtypes implementing `Serialize`/`Deserialize` by hand against the proxy, since
> `cynic::impl_scalar!` needs a concrete local type to bind to the GraphQL scalar.
>
> **Manifest-layer duplicate-key detection cannot reuse the ODF proxy as-is.** The proxy's
> `#[serde(flatten)] #[serde(with = "map_value_limited_precision")]` deserializes straight into a
> `BTreeMap`, which silently drops duplicate keys (last write wins) rather than erroring — exactly the
> hazard the manifest layer's hand-rolled `EntriesVisitor` exists to catch. So
> `ResourceManifestHeaders.labels`/`.annotations` stay `Vec<(TypeRef, serde_json::Value)>` with a custom
> visitor that walks map/seq entries and rejects a repeated key before it can collapse, only widened
> from `Vec<(String, String)>` to swap in `TypeRef` keys and arbitrary JSON values.
>
> **A bad label/annotation key is now a parse-time failure, not a headers-validation failure.** Because
> `TypeRef`'s own `FromStr`/`Deserialize` runs *during* `serde_json`/`serde_yaml::from_str` on the whole
> manifest, an invalid key (fails both the `https:` URI check and the `TypeName` grammar) surfaces as
> `ParseResourceManifestError` → `ApplyManifestError::ParseManifest`, the same category as malformed
> YAML/JSON — not as a `ResourceHeadersValidationError`/`InvalidHeaders` problem. The
> `InvalidLabelKey`/`InvalidAnnotationKey` problem-code variants (facade errors, the GraphQL
> `ResourceHeaderValidationProblemCode` enum, the Cynic mirror, and `resources/schema.gql`) were removed
> end to end as now-unreachable, the same treatment `ResourcePhase::Degraded` got. There is also no
> longer a per-value size limit on labels/annotations (`LabelValueTooLong`/`AnnotationValueTooLong` were
> dropped too) — values are arbitrary JSON and may be arbitrarily large/nested; only entry *counts*
> (`MAX_LABELS`/`MAX_ANNOTATIONS`) are still enforced.

Also generated: `id` (allocated if the manifest omitted it) and `last_reconciled_at`.

**(3) Persisted form — the snapshot.** `ResourceSnapshot`
([`core/resource_snapshot.rs`](/src/domain/resources/domain/src/core/resource_snapshot.rs))
combines authored + generated + event-sourcing bookkeeping:

```rust
pub struct ResourceSnapshot {
    pub id: ResourceID,
    pub schema: TypeUri,                      // canonical schema URL (identity value)
    pub headers: ResourceHeaders,            // authored fields + generated fields
    pub spec: serde_json::Value,             // authored (may be transformed — see SecretSet)
    pub status: Option<serde_json::Value>,   // generated
    pub last_reconciled_at: Option<DateTime<Utc>>, // generated
    pub last_event_id: Option<EventID>,      // event-sourcing cursor (optimistic concurrency)
}
```

```mermaid
flowchart LR
    M["Manifest (authored)<br/>$schema<br/>headers: name, account?, id?,<br/>description?, labels, annotations<br/>spec"]
    A["apply use case<br/>(resolve account, allocate id,<br/>bump generation, set timestamps)"]
    S["Snapshot / State<br/>= authored fields<br/>+ <b>generated</b>: account(ID), id,<br/>generation, created/updated/deleted_at,<br/>status{phase, observedGeneration, conditions}"]
    M --> A --> S
```

**Worked example — `VariableSet`:** the user authors `spec.variables`; the framework generates the
entire `status` after reconciliation.

**`SecretSet` goes further — and its security invariant is documented separately below.** Authored
plaintext secrets are converted to an encrypted canonical form *before the first durable write*, so
the persisted `spec` is itself server-derived. See [Secret handling](#secret-handling-invariant).

---

## 6. Persistence model

Resources are **event-sourced with a materialized snapshot per resource**. Storage backends live in
`src/infra/resources/` (`postgres`, `sqlite`, `inmem`), all implementing the domain
`ResourceRepository` + raw event-store traits and sharing the cross-backend test suite in
`src/infra/resources/repo-tests/`. Schema is defined by
[`migrations/postgres/20260323155948_resources.sql`](/migrations/postgres/20260323155948_resources.sql)
(SQLite mirror alongside).

**Two tables:**

- **`resources`** — one row per resource (the snapshot): `resource_id` (UUID, **PK**),
  `account_id`, `resource_schema`, `resource_name`, `description`, `labels`/`annotations`
  (JSONB), `spec` (JSONB), `status` (JSONB, nullable), `generation`, `created_at`/`updated_at`,
  `deleted_at` (nullable), `last_reconciled_at`, `last_event_id`. **Uniqueness:**
  `UNIQUE (account_id, resource_schema, resource_name)`.
  A partial index on `(account_id, resource_schema, status->>'phase') WHERE deleted_at IS NULL`
  backs the summary projection. The `labels`/`annotations` columns are untyped JSONB with **no
  index** — the migration to the canonical `TypeRef`-keyed shape (see [§5a](#5a-resource-anatomy--input-vs-auto-generated))
  needed no schema change, since an empty map (`{}`) is valid under both the old and new
  representations and the columns were never more specifically typed than "some JSON object". Label
  indexing for selector queries is a deferred future addition, not yet implemented.
- **`resource_events`** — append-only log: `event_id` (BIGINT from a sequence, PK), `resource_id`
  (FK → `resources`), `resource_schema`, `event_time`, `event_type`, `event_payload` (JSONB).

**Source of truth.** The event log is authoritative — aggregates are rebuilt by projecting events
(`ResourceAggregateLoader`). The `resources` row is a **derived snapshot** maintained in the same
transaction as the event append; it exists for efficient queries/listing/uniqueness and should never
diverge. If they ever disagree, the events win and the snapshot is the bug.

**Optimistic concurrency.** `update_resource` is a compare-and-set on `last_event_id`: the update
only applies if the stored `last_event_id` equals the caller's `expected_last_event_id`; otherwise it
returns `UpdateResourceError::concurrent_modification()`. A unique-constraint violation surfaces as
`UpdateResourceError::Duplicate`.

**Soft-delete / tombstone.** Delete is a soft-delete: the row stays with `deleted_at` set and a
`Deleted` event is appended. Because the unique constraint still covers deleted rows, delete also
**renames the resource to a tombstone name** (`try_delete(now, tombstone_name)` → `Deleted` event
carries `tombstone_name`) so the original `(account, schema, name)` is freed for reuse. All query/list
methods filter `WHERE deleted_at IS NULL`, so tombstones are invisible to normal reads.

**Migration / backfill.** Schema changes are ordinary SQLx migrations under `migrations/{postgres,sqlite}`.
There is a precedent for data backfill into resources —
`20260513120000_backfill_env_var_resources.sql` migrates legacy dataset env-vars into `VariableSet`
resources; new types that supersede existing data should follow that pattern (additive migration +
backfill, never rewriting the event log in place).

**Schema/version upgrades.** The schema URL is part of resource identity, including its version
segment. Supporting a new schema version therefore requires an explicit compatibility/migration
story for that resource type: update manifests, projections, dispatcher registration, and any
existing rows/events that should move to the new schema. Do not assume the pre-`$schema` version
conversion rules still apply.

---

## 7. Account resolution & authorization

Every resource belongs to exactly one account, and that scoping is also the authorization boundary.
Resolution + permission checks live in `ResourceAccountResolverImpl`
([`facade/local/resource_account_resolver_impl.rs`](/src/domain/resources/facade/src/facade/local/resource_account_resolver_impl.rs)).

- **Who may specify `headers.account`.** The manifest `account` field is optional and **defaults to
  the calling subject's own account**. To target *another* account, the resolver requires the caller
  to be an **admin** (`rebac_service.is_account_admin`); otherwise it returns
  `AccessError::Unauthorized`. An **anonymous** subject cannot resolve any account (rejected).
- **Account selector forms.** `account` (`ResourceAccountRef`) may be given by name, by id, or both
  (agreement checked when both are given — mismatch → error). Resolution maps the selector to a
  concrete `(AccountID, AccountName)`. An empty selector (`{}`) is rejected while the manifest is
  being deserialized — the enum has no empty variant, so this is a parse-time failure, not a
  resolution-time one; the resolver itself never sees an empty selector.
- **Remote calls.** For a remote context the CLI authenticates with an access token; the server then
  resolves the *authenticated principal* into the current account subject exactly as above — the
  client never asserts its own account id, the server derives it.
- **Local workspace context.** The local facade uses the workspace's current account subject
  (`CurrentAccountSubject`) as the caller; the same admin rule applies for cross-account targeting.
- **UID belonging to a different account.** Lookups are account-scoped (`find_account_snapshot`
  filters by `account_id`). A UID that exists but belongs to another account is reported as
  **not-found** (`ResourceIDNotFoundError`), not "forbidden" — so existence is not leaked across
  accounts. A UID of the wrong schema yields `ResourceTypeMismatchError` in the domain and
  `ResourceLookupProblem::SchemaMismatch` at the facade boundary.
- **Account-deletion cascade.** When an account is deleted, `DeleteAccountResourcesUseCase` deletes
  that account's resources (see [§13](#13-data-flow-walkthroughs)). It operates on live resources;
  already-tombstoned resources are simply skipped (they are already `deleted_at`-marked), so the
  cascade is idempotent with respect to prior deletions.

---

## 8. Rename & conflict rules

The apply planner ([`services/apply_resource_planner.rs`](/src/domain/resources/services/src/services/apply_resource_planner.rs))
decides create vs update by resolving the target resource first:

- **No `id` in manifest** → resolve by `(account, schema, name)`. Found → update; not found → create
  (new UID allocated).
- **`id` in manifest** → load that exact resource (the "exact pointer"). This is what enables a
  **rename**: supply the `id` and a new `headers.name`; identity stays stable while the name
  changes.

Concrete conflict cases:

| Case | Outcome |
| --- | --- |
| `id` + changed `headers.name` | **Rename** — name updated on the same resource (`Update`). |
| `id` whose resource has a different schema | **Reject** — `ResourceTypeMismatchError` in domain code, mapped to `SchemaMismatch` in the facade. |
| `id` resolving to a resource in a different account | **Not found** — `ResourceIDNotFoundError` (account-scoped lookup; existence not leaked). |
| `headers.account` targeting another account without admin | **Reject** — `AccessError::Unauthorized` ([§7](#7-account-resolution--authorization)). |
| Rename target name already taken (same account+schema) | **Reject** — unique-constraint `Duplicate` at persistence. |
| Update by name where another account has the same name | **No conflict** — the account disambiguates; each `(account, schema, name)` is independent. |
| Apply re-using a tombstoned (deleted) name | **Allowed** — the deleted resource was renamed to a tombstone, freeing the name for a fresh create. |
| Apply with no real change | **`Untouched`** — no generation bump, no events, no reconcile. |

Note names are normalized to lowercase on write, so case-only differences are not distinct names.

---

## 9. Services (`kamu-resources-services`)

This crate turns the domain traits into running code. Module layout: `services/`, `use_cases/`,
`crud_dispatchers/`, `message_handlers/`, `event_stores/`, `resources/`, `testing/`.

**Supporting services:**

- **`ResourceAggregateLoader<R>`** — replays the event stream and projects a resource aggregate.
- **`ResourcePersistenceService<R>`** — commits aggregate changes as events + snapshot (create /
  save / delete, with `delete_many`).
- **`GenericResourceQueryService`** (impl `GenericResourceQueryServiceImpl`) — descriptor-agnostic
  queries (allocate UID, find by name, search identities) delegating to `ResourceRepository`.
- **`TypedResourceQueryService<R>`** — type-safe queries for a single type.
- **`Reconciler<R>`** — the per-type reconcile engine (implemented in the type's crate).

**Two-phase apply.** `ApplyResourceUseCase<R>` separates *planning* (validate, diff, decide
create/update/untouched, without writing) from *application* (persist + publish)
([`use_cases/apply_resource_use_case.rs`](/src/domain/resources/domain/src/use_cases/apply_resource_use_case.rs)):

```rust
pub trait ApplyResourceUseCase<R: ReconcilableEventSourcedResource>: Send + Sync {
    async fn plan(&self, params: ApplyResourceParams<R>)
        -> Result<ApplyResourcePlanningDecision<R>, ApplyResourceUseCaseError<R>>;
    async fn apply(&self, params: ApplyResourceParams<R>)
        -> Result<ApplyResourceApplicationDecision<R>, ApplyResourceUseCaseError<R>>;
}

pub enum ApplyResourcePlanningDecision<R> { Planned(ApplyResourcePlan<R>), Rejected(ApplyResourceRejection) }
pub enum ApplyResourceApplicationDecision<R> { Applied(ApplyResourceResult<R>), Rejected(ApplyResourceRejection) }
pub enum ApplyResourceAction { Create, Update, Untouched }
pub enum ApplyResourceOutcome { Created, Updated, Untouched }
```

The `--dry-run` path uses `plan`; a live apply uses `apply`. Use-case implementations are generated
by **`declare_*_use_case!`** macros so each type gets a fully-wired instance without boilerplate.

**Dispatchers + registry.** Each type registers a `ResourceCrudDispatcher` via
`declare_resource_crud_dispatcher!` (and a presentation dispatcher). Lookup is by schema or selector
metadata through `dill` — the registry key is `ResourceDispatcherMeta` (schema `&'static str` +
selector name/aliases). There are **three** lookup entry points, differing only in how a missing
dispatcher is reported
([`crud_dispatchers/resource_crud_dispatcher_registry.rs`](/src/domain/resources/services/src/crud_dispatchers/resource_crud_dispatcher_registry.rs)):

```rust
// (1) By schema (from a parsed manifest `$schema`) — a miss is a user error.
pub fn get_resource_crud_dispatcher<E>(target_catalog, schema: &str) -> Result<Arc<dyn …>, E>
    where E: From<UnsupportedResourceDescriptorError> + From<InternalError>;

// (2) By selector name/alias (from a CLI/GraphQL selector) — a miss is a user error.
pub fn get_resource_crud_dispatcher_by_raw_selector<E>(target_catalog, raw_selector: &ResourceTypeSelectorRaw) -> Result<…, E>
    where E: From<UnsupportedResourceSelectorError> + From<InternalError>;

// (3) By a schema already known valid (stored snapshot, or an already-resolved selector) —
//     a miss is a data-integrity catastrophe, so it is an InternalError (→ 500), never a user error.
pub fn get_resource_crud_dispatcher_for_trusted_schema(target_catalog, schema: &str)
    -> Result<Arc<dyn …>, InternalError>;
```

Each finds exactly one dispatcher; zero → `NotFound`, more than one → `Duplicate`. The
**descriptor** path (1) is reachable only where a fresh unvalidated schema arrives — the apply
manifest `$schema`. The **selector** path (2) serves every CLI/GraphQL selector. The **trusted**
path (3) is for schemas the system itself produced (a stored snapshot's `schema`, or a schema
already resolved from a valid selector), where a lookup miss means storage/registration is corrupt,
not a bad request.

**Message handlers** (outbox consumers — see [§13](#13-data-flow-walkthroughs)):
`ResourceLifecycleMessageConsumer` and `AccountLifecycleMessageConsumer`.

**DI registration** — [`dependencies.rs`](/src/domain/resources/services/src/dependencies.rs) is the
single place where this crate's `dill` catalog components (base query services, the cross-type use
cases, and the outbox message consumers) are registered. Per-type use cases and dispatchers are
registered separately by the type's own crate (see [§14](#14-concrete-resource-types-kamu-configuration)).

---

## 10. Facade (`kamu-resources-facade`)

`ResourceFacade` is the **single API seam** every caller uses. It hides the per-type generics behind
descriptor-keyed dispatch and gives local/remote symmetry. Selected signature
([`facade/resource_facade.rs`](/src/domain/resources/facade/src/facade/resource_facade.rs)):

```rust
#[async_trait]
pub trait ResourceFacade: Send + Sync {
    async fn list_supported_resource_types(&self) -> Result<Vec<ResourceTypeDescriptor>, ...>;
    async fn summary(&self, request: ResourcesSummaryRequest) -> Result<ResourcesSummary, ...>;

    async fn get(&self, selector: ResourceSelector, spec_view_mode: SpecViewMode) -> Result<ResourceView, ...>;
    async fn get_many(&self, selector: ResourceBatchSelector, spec_view_mode: SpecViewMode)
        -> Result<BatchResourceResponse<ResourceView, ResourceLookupProblem>, ...>;
    async fn get_identity(&self, selector: ResourceSelector) -> Result<ResourceIdentityView, ...>;
    async fn render_manifest(&self, selector, format: ResourceManifestFormat, spec_view_mode) -> ...;

    async fn list(&self, request: ListResourcesRequest) -> Result<Vec<ResourceSummaryView>, ...>;
    async fn list_identities(&self, request: ListResourceIdentitiesRequest) -> ...;
    async fn search_identities(&self, request: SearchResourceIdentitiesRequest) -> ...;
    async fn list_all(&self, request: ListAllResourcesRequest) -> ...;

    async fn plan_apply_manifest(&self, request: ApplyManifestRequest) -> Result<ApplyManifestPlanningDecision, ...>;
    async fn apply_manifest(&self, request: ApplyManifestRequest) -> Result<ApplyManifestApplicationDecision, ...>;
    async fn delete(&self, selector: ResourceSelector) -> Result<ResourceID, ...>;
    async fn delete_many(&self, selector: ResourceBatchSelector) -> ...;
}
```

**Selectors & view modes:**

```rust
pub struct ResourceSelector { pub account: Option<ResourceAccountRef>,
                              pub resource_type: ResourceTypeSelectorRaw,
                              pub resource_ref: ResourceRef }
pub enum   ResourceRef { ById(ResourceID), ByName(ResourceName) }
pub enum   ResourceManifestFormat { Json, Yaml }
pub enum   SpecViewMode { Encrypted /* default */, Revealed }
```

Batch operations return `BatchResourceResponse<T, E>` with positional `successes` / `problems`
(each tagged by `request_index`) — so a partial batch reports per-item outcomes.

**Implementations:**

- **`LocalResourceFacadeImpl`** — resolves account → resolves selector name/alias to a
  schema and UID/snapshot → looks up the per-type dispatcher via `get_resource_crud_dispatcher` →
  calls it. Holds the `dill::Catalog`,
  a `ResourceAccountResolver`, and `GenericResourceQueryService`.
- **`RemoteGraphqlResourceFacadeImpl`** — a `cynic`-based GraphQL client that issues the queries /
  mutations of a remote server (whose resolvers use a *local* facade there). Operations live under
  `facade/graphql/cynic_api/operations/`; responses are mapped back to domain views/errors in
  `facade/graphql/outcome_mapper/`.

**Registration** ([`dependencies.rs`](/src/domain/resources/facade/src/dependencies.rs)) adds
`ResourceAccountResolverImpl` and `LocalResourceFacadeImpl`. (The remote impl is constructed
on demand by the CLI for remote contexts — see [§12](#12-cli).)

---

## 11. GraphQL API

Files: [`adapter/graphql/src/queries/resources/`](/src/adapter/graphql/src/queries/resources)
and [`adapter/graphql/src/mutations/resources_mut/`](/src/adapter/graphql/src/mutations/resources_mut).
Every resolver delegates to `ResourceFacade`.

**Queries (`Resources`):** `supported_resource_types`, `summary`, `resource` / `resources`,
`resource_identity` / `resource_identities`, `list_by_resource_type` /
`list_identities_by_resource_type`, `search_identities`, `list_all` / `list_all_identities`,
`render_manifest` / `render_manifests`. The `revealed: bool` argument maps to `SpecViewMode`.

**Mutations (`ResourcesMut`):** `apply_manifest(manifest, format, dry_run?)`, `delete(selector)`,
`delete_many(selector)`. `dry_run` routes to `plan_apply_manifest`, otherwise `apply_manifest`.

**Outcome-union pattern.** *Domain/application outcomes* are modeled as unions: a resolver returns a
union of `Success` + typed `Problem` variants (bad account, unsupported descriptor, validation
failures, …) so clients handle each expected case structurally rather than by parsing error strings.
This does **not** cover everything — authentication failures, authorization failures, server bugs,
and infrastructure failures still surface as ordinary GraphQL `errors`. Clients must handle both: the
typed `Problem` variants *and* transport-level GraphQL errors. The apply outcome
(`resource_apply_outcome_model.rs`) is the richest example of the union:

- `Success` → operation (`Created`/`Updated`/`Untouched`) + `changes` (each: kind
  `Generation`/`Headers`/`Spec`, JSON path, `before`/`after`) + `warnings`.
- `Rejection` → category (`ImmutableFieldChanged`, `BusinessValidationFailed`,
  `ReferencedObjectMissing`, `LifecycleRuleConflict`) + message.
- `ParseManifest`, `UnsupportedDescriptor`, `BadAccount`, `InvalidHeaders`, `InvalidSpec` →
  structured validation/parse problems.

These map directly from the domain views in
[`views/apply_manifest_views.rs`](/src/domain/resources/domain/src/views/apply_manifest_views.rs)
(`ApplyManifestPlan` / `ApplyManifestResult` / `ApplyManifestRejection` /
`ApplyManifestChange` / `ApplyManifestChangeKind`).

---

## 12. CLI

**Resource commands** (defined in [`/src/app/cli/src/cli.rs`](/src/app/cli/src/cli.rs), implemented
in [`/src/app/cli/src/commands/`](/src/app/cli/src/commands)). Note the user-facing subcommands are
the short, *unified* forms — `delete`/`get`/`list` serve both datasets and resources — while the
implementation files carry `_resource(s)_` names:

| Subcommand | Implementation file | Purpose |
| --- | --- | --- |
| `kamu apply` | `apply_command.rs` | Discover manifests (files/dir/stdin) and apply/plan them; `--dry-run`, `--recursive`, `--stdin`, `--continue-on-error`. |
| `kamu list` | `list_resources_command.rs` | List resources by type or all; renders Table/CSV/JSON/Parquet. |
| `kamu get` | `get_resource_command.rs` | Get resource(s) by selector(s); names or full manifest; `--spec`, `--revealed`. |
| `kamu delete` | `delete_resources_command.rs` | Delete by selector(s); `--force`, `--ignore-not-found`, `--dry-run`. |

**Context commands** (manage which server/workspace resources target): `kamu context add` / `list` /
`check` / `use` / `delete`, and `kamu context api-resources` (list supported resource types for a
context).

**CLI-side services** ([`app/cli/src/services/resources/`](/src/app/cli/src/services/resources),
implementations under `impl/`):

| Service | Role |
| --- | --- |
| `ResourceFacadeFactory` | Returns the right `ResourceFacade` for a context — local for the workspace, or a `RemoteGraphqlResourceFacadeImpl` (with access token) for a remote context. |
| `ResourceManifestDiscoveryService` | Finds `.yaml`/`.yml`/`.json` manifests from paths/stdin (recursive optional). |
| `ResourceManifestExecutionService` | Reads a discovered manifest and calls `plan_apply_manifest` / `apply_manifest`. |
| `ResourceTypeLookupService` | Resolves a resource type by selector name/alias/raw string; caches `list_supported_resource_types` per context. |
| `ResourceSelectionSyntaxService` | Parses the `get`/`delete` selector grammar. |
| `ResourceSelectionResolutionService` | Expands parsed selectors into concrete targets via facade queries. |
| `ResourceSelectorResolutionService` | Resolves a single selector string to a `ResourceRef`. |
| `ResourceSummaryService` | Produces the dashboard summary (context info + per-type counts). |

**Selector grammar** (used by `get`/`delete`): `all`; `type all` or `type/all`; same-type list
`type name1 name2 …` (no slash); slash form `type/name …` (exactly one `/` each); plus name patterns.

Local-vs-remote is chosen entirely by `ResourceFacadeFactory` + the context resolver — commands
themselves are agnostic.

### CLI semantics matrix

| Aspect | `apply` | `get` | `list` | `delete` |
| --- | --- | --- | --- | --- |
| Input | manifest(s): `-f <file>`, dir + `--recursive`, or `--stdin` | selector(s) | positional `target` (a type or `all`) | selector(s) |
| Selector / target examples | n/a (identity from manifest) | `vs my-vars`, `vs/my-vars`, `secretset/db%`, `vs all` | `kamu list variablesets` (or `vs`, `secretsets`, `ss`, `all`) | `vs my-vars`, `vs/my%`, `vs all` |
| `%` name patterns | n/a | **yes** | n/a (lists whole type) | **yes** |
| May return / act on multiple | yes (per manifest) | **yes, but bounded** — selector-driven, capped by `max_results` (default), `--unbounded` to lift | yes (bounded by `--max-results`/`--unbounded`) | yes |
| `get` ≠ `list` | — | `get` resolves explicit selectors to specific resources and is **bounded by design** so it doesn't degrade into a type-wide listing; use `list` to enumerate a type | enumerates a type/all | — |
| Output modes | summary + changes (`--dry-run`)/warnings; verbose | `-o name` \| `-o json` \| `-o yaml`; `--spec` for apply-compatible spec | Table/CSV/JSON/Parquet (via `OutputConfig`), `-w` for wider detail | summary / dry-run preview |
| Default secret visibility | n/a | **`Encrypted`** (ciphertext); `--revealed` to decrypt | secrets not expanded in list columns | n/a |
| Relevant flags | `--dry-run`, `--recursive`, `--stdin`, `--continue-on-error` | `--ignore-not-found`, `--spec`, `--revealed`, `--max-results`/`--unbounded` | `--max-results`/`--unbounded`, `-w`, `-o` | `--force`, `--ignore-not-found`, `--dry-run` |
| Flag semantics | `--continue-on-error`: keep going past a failing manifest; `--dry-run`: plan only, no writes | `--ignore-not-found`: skip missing selectors instead of erroring | — | `--force`: skip confirmation prompt; `--ignore-not-found`: exit OK if absent; `--dry-run`: preview resolved deletions |
| Local vs remote | identical behavior; chosen by context (`--context` to override) | identical | identical | identical |

> The `get` vs `list` boundary is intentional: `get` is for *named/selected* resources (bounded),
> `list` is for *enumeration* (paginated). Keep `get` from growing into a second `list`.

**Selector grammar — accepted forms** (parsed by `ResourceSelectionSyntaxParser`,
[`resource_selection_syntax_parser.rs`](/src/app/cli/src/services/resources/impl/resource_selection_syntax_parser.rs)):
`all`; same-type list `type name1 name2 …` (no slash); slash form `type/name …` (each arg exactly one
`/`); `type all` or `type/all`; names may use `%` patterns.

**Intentionally rejected** (documented as a contract, not just left implicit):

- `kamu get vs/foo bar` — **mixing** slash and same-type list forms in one command is rejected
  ("Cannot mix positional `type name` and slash `type/name` syntax"). The one exception is a leading
  `all` (e.g. `all vs/foo` is accepted, with the rest treated as shadowed).
- `kamu get vs/foo/extra` — the slash form must contain **exactly one** `/` (rejected: "Invalid
  resource reference").
- `kamu get vs` — a bare type with **no selector** is rejected ("Expected `type/name`"); use
  `kamu get vs all` / `kamu get vs/all`, or `kamu list vs`, to enumerate the type.

> Note: `kamu get all` *is* accepted by the parser (resolves all resources, **bounded** by
> `--max-results`/`--unbounded`). For unbounded enumeration prefer `kamu list all` — this is a
> guidance boundary, not a parser rejection.

---

## 13. Data flow walkthroughs

### (a) `kamu apply -f manifest.yaml`

```mermaid
sequenceDiagram
    participant U as User / CLI
    participant D as ManifestDiscovery+Execution
    participant F as ResourceFacade (Local)
    participant R as Dispatcher registry
    participant UC as ApplyResourceUseCase<R>
    participant ST as Event store + repo
    participant OB as Outbox

    U->>D: apply -f manifest.yaml [--dry-run]
    D->>F: apply_manifest(ApplyManifestRequest) (or plan_apply_manifest)
    F->>F: resolve account, parse manifest ($schema, spec)
    F->>R: get_resource_crud_dispatcher(schema)
    R-->>F: ResourceCrudDispatcher<R>
    F->>UC: plan(params)  %% validate + diff (create/update/untouched)
    alt dry-run
        UC-->>F: ApplyResourcePlanningDecision (Planned | Rejected)
    else live apply
        UC->>ST: append events + write snapshot (optimistic last_event_id)
        UC->>OB: produce ResourceLifecycleMessage::Applied
        UC-->>F: ApplyResourceApplicationDecision (Applied | Rejected)
    end
    F-->>U: outcome + changes + warnings
```

### (b) Reconciliation

```mermaid
sequenceDiagram
    participant UC as ReconcileResourceUseCase<R>
    participant L as AggregateLoader<R>
    participant Rec as Reconciler<R>
    participant ST as Event store + repo
    participant OB as Outbox

    UC->>L: load(id)  %% needs_reconciliation()? observed_generation < generation
    UC->>ST: try_mark_reconciliation_started → append event
    UC->>Rec: reconcile(resource)  %% e.g. SecretSet builds encrypted projection
    alt success
        UC->>ST: try_mark_reconciliation_succeeded(generation, success)
        UC->>OB: produce ResourceLifecycleMessage::ReconciliationSucceeded
    else failure
        UC->>ST: try_mark_reconciliation_failed(generation, error)
        UC->>OB: produce ResourceLifecycleMessage::ReconciliationFailed
    end
```

Reconciliation is internally **two separately-committed phases**
([`use_cases/reconcile.rs`](/src/domain/resources/services/src/use_cases/reconcile.rs)): phase 1
marks the resource `Reconciling` and **commits that transition** (a stable persisted hand-off point);
phase 2 then runs the `Reconciler<R>` and persists the outcome (`Ready`/`Failed`) in a *separate*
transaction. Because the two phases commit independently, concurrent changes between them are
expected and handled via optimistic concurrency ([§6](#6-persistence-model)) — a resource can be left
in `Reconciling` if phase 2 never completes, and a later reconcile will pick it up.

### Outbox connections

Producer: **`MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE`** (`dev.kamu.domain.resources.ResourceService`).
The use cases publish `ResourceLifecycleMessage` variants:

```mermaid
flowchart LR
    APPLY["apply.rs"] -->|Applied| MSG[["ResourceLifecycleMessage"]]
    REC["reconcile.rs"] -->|ReconciliationSucceeded / ReconciliationFailed| MSG
    DEL["delete.rs"] -->|Deleted| MSG

    MSG --> OB[("Outbox")]

    OB --> BRIDGE["ResourceLifecycleMessageConsumer<br/>(MESSAGE_CONSUMER_..._RESOURCE_LIFECYCLE_EVENT_BRIDGE)<br/>→ ResourceLifecycleEventDispatcher (per-type)"]
    OB --> CFG["configuration crate:<br/>ConfigurationResourceLifecycleMessageConsumer"]

    ACC[["AccountLifecycleMessage::Deleted"]] --> OB2[("Outbox")]
    OB2 --> ACCH["AccountLifecycleMessageConsumer<br/>(MESSAGE_CONSUMER_..._RESOURCE_ACCOUNT_LIFECYCLE_HANDLER)<br/>→ DeleteAccountResourcesUseCase (cascade delete)"]
```

- **Resource lifecycle** — `Applied` (from `use_cases/apply.rs`), `ReconciliationSucceeded` /
  `ReconciliationFailed` (`use_cases/reconcile.rs`), `Deleted` (`use_cases/delete.rs`). Consumed by
  the **event bridge** (routes to the per-type `ResourceLifecycleEventDispatcher` — this is what
  schedules reconciliation) and by the configuration crate's projection-maintenance consumer. See
  [How reconciliation is scheduled](#how-reconciliation-is-scheduled) for the split of duties.
- **Account cascade** — when an account is deleted, `AccountLifecycleMessage::Deleted` is consumed by
  `AccountLifecycleMessageConsumer` (`MESSAGE_CONSUMER_KAMU_RESOURCE_ACCOUNT_LIFECYCLE_HANDLER`),
  which invokes `DeleteAccountResourcesUseCase` to remove all of that account's resources.

### How reconciliation is scheduled

Reconciliation is **not** synchronous within `apply` — it is driven by the outbox. The chain is:

```mermaid
flowchart LR
    A["apply use case<br/>(persist + produce Applied)"] --> OB[("Outbox")]
    OB --> C["ResourceLifecycleMessageConsumer<br/>(event bridge)"]
    C -->|lookup by schema| D["per-type ResourceLifecycleEventDispatcher<br/>handle_applied()"]
    D --> R["ReconcileResourceUseCase::execute(id)"]
    R --> Rec["Reconciler&lt;R&gt;"]
```

1. `apply` persists the resource and produces `ResourceLifecycleMessage::Applied` to the outbox.
2. The generic **`ResourceLifecycleMessageConsumer`** (the "event bridge",
   `MESSAGE_CONSUMER_KAMU_RESOURCE_LIFECYCLE_EVENT_BRIDGE`,
   [`message_handlers/resource_lifecycle_message_consumer.rs`](/src/domain/resources/services/src/message_handlers/resource_lifecycle_message_consumer.rs))
   consumes it and looks up the per-type `ResourceLifecycleEventDispatcher` by descriptor.
3. That dispatcher's `handle_applied` calls **`ReconcileResourceUseCase::execute(id)`**, which loads
   the aggregate, marks reconciliation started, invokes the type's `Reconciler<R>`, and records the
   outcome (producing `ReconciliationSucceeded` / `ReconciliationFailed`).

The per-type dispatcher + reconcile use case are generated and registered together by the
`declare_resource_service_layer!` umbrella macro
([`resources/mod.rs`](/src/domain/resources/services/src/resources/mod.rs)); its
`register_<type>_resource_service_layer(builder)` is called from the type's crate
(e.g. `configuration/services/src/dependencies.rs`). In the current dispatcher
([`resource_lifecycle_reconcile_dispatcher.rs`](/src/domain/resources/services/src/message_handlers/resource_lifecycle_reconcile_dispatcher.rs))
only `handle_applied` triggers work; `handle_reconciliation_succeeded/failed/deleted` are no-ops.

There is a **second, separate consumer** in the configuration crate
(`ConfigurationResourceLifecycleMessageConsumer`) that reacts to `ReconciliationSucceeded` / `Deleted`
to **maintain the read-side projections** (cleanup superseded generations, delete projection rows on
resource deletion) — it does *not* trigger reconciliation. So the two `ResourceLifecycleMessage`
consumers have distinct jobs: the bridge **schedules reconciliation**, the configuration consumer
**garbage-collects projections**.

> There is **no periodic/background reconcile loop** today — reconciliation fires once per `Applied`
> message (i.e. per generation-changing apply). A failed reconcile is not automatically retried; a
> subsequent re-apply that bumps the generation produces a new `Applied` and re-triggers the chain.

### Lifecycle state machine

Below is the machine **as actually implemented today** for `VariableSet` / `SecretSet`. The
transitions come from `ResourceStatus` in
[`state/resource_status.rs`](/src/domain/resources/domain/src/state/resource_status.rs)
(`mark_reconciling` → `Reconciling`, `mark_ready` → `Ready`, `mark_failed` → `Failed`,
`mark_pending_for_new_generation` → `Pending`).

```mermaid
stateDiagram-v2
    [*] --> Pending: apply (Created)
    Pending --> Reconciling: reconcile starts<br/>(observedGeneration < generation)
    Reconciling --> Ready: reconciliation succeeded
    Reconciling --> Failed: reconciliation failed
    Ready --> Pending: spec/headers changed<br/>(generation bumps, conditions cleared)
    Failed --> Pending: re-apply (generation bumps)
    Ready --> [*]: delete
    Failed --> [*]: delete
    Pending --> [*]: delete
```

**Notes on current behavior (stage-1):**

- **`Failed` is the only unhealthy phase.** `ResourcePhase` is adopted directly from ODF RFC-018's
  codegen'd `resource::ResourcePhase` (`Pending`, `Reconciling`, `Ready`, `Failed`) — same "adopt the
  codegen type directly" pattern as `ResourceID`/`TypeUri`/`ResourceAccountRef` (see
  [§5a](#5a-resource-anatomy--input-vs-auto-generated)). A previously-reserved, never-produced
  `Degraded` variant (kamu-side only, not part of the RFC) was dropped in this migration. `Failed` is
  set by `mark_failed` when the reconciler returns an error, alongside a `Ready=false` condition
  carrying the reason/message.
- **`Failed` is status, not terminal and not auto-retried.** There is no background reconcile
  worker or scheduler yet. Recovery is driven by the **user re-applying** the manifest: a changed
  spec bumps `generation`, which moves the resource back to `Pending` (clearing conditions) and makes
  it eligible for reconciliation again.
- An unchanged re-apply (`Untouched`) does not bump `generation` and so does not re-trigger
  reconciliation.

---

## 14. Concrete resource types (`kamu-configuration`)

Two types are functional today, both under the config `v1alpha1` schema namespace. (A third, `Storage`
(`st`/`storage`), lives under `src/domain/storage/` and is wired through the same machinery but is
**work in progress / not yet complete** — treat it as an in-flight example, not a supported type.)

| Schema | Selector name | Short name | Spec | Reconciliation |
| --- | --- | --- | --- | --- |
| `https://opendatafabric.org/schemas/config/v1alpha1/VariableSet` | `variablesets` | `vs` | `spec.variables` (name → value, scalar or `{ value }`) | Projects status; lint warnings (e.g. reserved `KAMU_` prefix). |
| `https://opendatafabric.org/schemas/config/v1alpha1/SecretSet` | `secretsets` | `ss` | `spec.secrets` (name → plaintext / `{ value }` / encrypted) | Materializes an **encrypted** read-side projection (`SecretSetEntry`) for consumers (see [Secret handling](#secret-handling-invariant) for where encryption actually happens). |

### Secret handling invariant

> **Invariant:** plaintext secret material must never be written to resource events, snapshots, the
> read-side projection, logs, GraphQL responses, CLI output, diffs, or outbox payloads. `SecretSet`
> input is converted to an encrypted canonical representation **before the first durable write.**

Concretely, encryption happens in **two distinct places — and *not* (primarily) in the reconciler**,
which is the easy thing to assume:

1. **Spec sanitizer — the pre-persistence boundary** ([`sanitizers/secret_set.rs`](/src/domain/configuration/services/src/sanitizers/secret_set.rs)).
   `SecretSetSpecSanitizer` implements `ResourceSpecSanitizer<SecretSetResource>::sanitize_new_spec`.
   The apply use case calls `sanitize_params(...)` as the **very first step** of both `plan` and
   `apply` — *before* the planner runs and *before* any event/snapshot is written
   ([`use_cases/apply.rs`](/src/domain/resources/services/src/use_cases/apply.rs), lines 121 & 140).
   The sanitizer walks each secret, and for any non-encrypted value encrypts it (AES-GCM via
   `crypto_utils::AesGcmEncryptor`) into `SecretSpec::Encrypted { encrypted, nonce }` (base64). So
   the `spec` that gets persisted in `Created`/`SpecUpdated` events and snapshots **already holds
   ciphertext, never plaintext.** (As an optimization, if the new plaintext decrypts-equal to the
   current stored secret, the existing ciphertext is reused to avoid a spurious change.)
2. **Reconciler — encrypted read-side projection** ([`reconcilers/secret_set.rs`](/src/domain/configuration/services/src/reconcilers/secret_set.rs)).
   `SecretSetReconcilerImpl` re-encrypts into a *separate* materialized projection
   (`SecretSetEntry` rows with `value` + `secret_nonce`) in `SecretSetProjectionRepository`, which is
   what downstream consumers read. This is also ciphertext-only; it does **not** rewrite the resource
   `spec`.

**Reading back:** the secret-set spec-view dispatcher
([`resource_crud_dispatchers/secret_set_spec_view.rs`](/src/domain/configuration/services/src/resource_crud_dispatchers/secret_set_spec_view.rs))
exposes `reveal_spec`, used only when `SpecViewMode::Revealed` is requested — it decrypts each
`SecretSpec::Encrypted` back to a literal. With the default `SpecViewMode::Encrypted`, the stored
ciphertext envelope is returned unchanged (no decryption).

Domain types live in `src/domain/configuration/domain/src/resources/<type>/`
(`resource.rs`, `spec.rs`, `state.rs`, `status.rs`, `event.rs`, `reconciliation.rs`, …). Each
resource declares its identity and implements the core traits, e.g.:

```rust
// variable_set/resource.rs
impl VariableSetResource {
    // Const `&'static str`, reused straight from the ODF codegen (no re-declared URL literal).
    // Used only as the const dill-registry key (dill `#[meta]` requires a const).
    pub const SCHEMA_STR: &'static str = odf::metadata::config::VariableSet::schema_str();
    // Raw consts feed the dill-registry `ResourceDispatcherMeta` (const-only); the typed
    // `CANONICAL_SELECTOR_NAME`/`SELECTOR_ALIASES` feed `ResourcePresentationDefinition`. A unit test
    // (`selector_constants_stay_in_sync`) guards the two staying identical.
    pub const CANONICAL_SELECTOR_NAME_STR: &'static str = "variablesets";
    pub const CANONICAL_SELECTOR_NAME: ResourceSelectorName =
        ResourceSelectorName::new_unchecked_static(Self::CANONICAL_SELECTOR_NAME_STR);
    pub const SELECTOR_ALIAS_STRS: &'static [&'static str] = &["vs"];
    pub const SELECTOR_ALIASES: &'static [ResourceSelectorName] =
        &[ResourceSelectorName::new_unchecked_static("vs")];
}
// The typed schema identity is a `TypeUri` accessor (LazyLock-backed in the codegen), not a const;
// it and SCHEMA_STR derive from the same generated static, so they cannot drift.
impl ResourceSchemaProvider for VariableSetResource {
    fn schema() -> &'static TypeUri { odf::metadata::config::VariableSet::schema() }
}
impl DeclarativeResource for VariableSetResource { /* … */ }
impl ResourcePresentation for VariableSetResource { /* selector name/aliases + list columns */ }
```

Registration is in `src/domain/configuration/services/src/resource_crud_dispatchers/<type>.rs`
using the framework macros:

```rust
kamu_resources_services::declare_resource_crud_dispatcher!(
    dispatcher = VariableSetResourceCrudDispatcher, resource = VariableSetResource);
kamu_resources_services::declare_resource_presentation_dispatcher!(
    dispatcher = VariableSetResourcePresentationDispatcher, resource = VariableSetResource);

pub fn register_variable_set_resource_crud_dispatcher(catalog_builder: &mut dill::CatalogBuilder) {
    catalog_builder.add::<VariableSetResourceCrudDispatcher>();
    catalog_builder.add::<VariableSetResourcePresentationDispatcher>();
}
```

### Recipe: how to add a new resource type

1. **Define the domain types** under `configuration/domain/src/resources/<type>/`: `spec.rs`
   (`#[serde(deny_unknown_fields)]`, with validation + lint), `status.rs`, `state.rs`, `event.rs`,
   and `resource.rs` implementing `ResourceSchemaProvider`, `DeclarativeResource`,
   `ReconcilableResource`/`ReconcilableEventSourcedResource`, and `ResourcePresentation`
   (implement `schema() -> &'static TypeUri`, and set `SCHEMA_STR`, `CANONICAL_SELECTOR_NAME_STR`,
   `SELECTOR_ALIAS_STRS`, `CANONICAL_SELECTOR_NAME`, `SELECTOR_ALIASES`).
2. **Implement a `Reconciler<R>`** in `configuration/services/src/reconcilers/` (no-op projection is
   fine if there's nothing to do; transform the spec here if needed — see `SecretSet`).
3. **Declare dispatchers** with `declare_resource_crud_dispatcher!` /
   `declare_resource_presentation_dispatcher!` (and a spec-view dispatcher if the type has sensitive
   fields) and add a `register_<type>_resource_crud_dispatcher(builder)` function.
4. **Wire it into DI** — generate the per-type service layer (event-store bridge, loader,
   persistence, typed query, all use cases, and the lifecycle reconcile dispatcher) with the
   `declare_resource_service_layer!` umbrella macro, then call its
   `register_<type>_resource_service_layer(builder)` plus your CRUD/presentation/spec-view
   `register_*` from the crate's `dependencies.rs`, and `add` the `Reconciler<R>` impl.
5. **Test all three tiers** (see [§15](#15-tests)): a reconciler/service unit test, a facade contract
   case, and an E2E lifecycle test (apply → list → get → update → delete) plus a golden-view test.

The dispatcher registry then resolves your resource by schema or selector metadata, so the generic CRUD
operations work everywhere without changing the CLI command or GraphQL resolver code. **But "no
changes" is only true for the generic path** — in practice a complete type still needs:

- **DI registration** and **presentation metadata** (selector aliases + list columns) for it to appear
  and render at all;
- **facade contract coverage** (the `contract_test!` suite) so local/remote behavior is exercised;
- **schema / codegen validation** if the remote GraphQL client's generated `cynic` types depend on
  type-specific shapes;
- **CLI golden-output coverage** (the per-type golden-view E2E test) so output formatting is pinned;
- any **type-specific spec-view / sanitizer** logic (e.g. encryption) and its tests.

---

## 15. Tests

Three tiers, all exercising the real `VariableSet` / `SecretSet` types:

**Service-level (unit/integration)** —
[`src/domain/resources/services/tests`](/src/domain/resources/services/tests) and
[`src/domain/configuration/services/tests`](/src/domain/configuration/services/tests). Cover the
apply planner/executor, reconcile transitions, CRUD dispatch, persistence, the event-store bridge,
and the message consumers. A `TestResource` mock and `BaseResourceServiceHarness`
(`services/src/testing/`) provide a type-agnostic harness; configuration tests cover the real
reconcilers (incl. the `SecretSet` spec sanitizer, encrypted projection, and `reveal_spec`).

**Facade contract tests** —
[`src/domain/resources/facade-tests`](/src/domain/resources/facade-tests). The key idea: a
`contract_test!` macro runs **each** case against **both** a `LocalFacadeHarness` and a
`RemoteGraphqlFacadeHarness`, so the suite **enforces local/remote symmetry for the behavior it
covers** (it doesn't guarantee parity for untested paths). Suites cover apply, batch ops, account scoping,
list/search, supported resource types, get-identity, error taxonomy, delete, render-manifest,
list-all, summary, and spec view modes.

**E2E (CLI)** —
[`src/e2e/app/cli/repo-tests/src/commands/resources`](/src/e2e/app/cli/repo-tests/src/commands/resources/).
Drive the real CLI binary via `KamuCliPuppet`. A `ResourceCtx` abstraction
(`repo-tests/src/resources/context.rs`) runs every scenario against **both** an implicit local
context and a registered remote context. The `get_view.rs` helper parses `get -o json` into a
queryable `ResourceView` (`ident()`, `variable()`, `has_secret()`, `id()`, …) so assertions are
targeted rather than brittle; a golden-view test pins the whole-document shape once per type.

### Testing policy — what belongs where

Each tier has a distinct job; put a test at the lowest tier that can express it:

- **Service tests** validate internal state transitions and persistence edge cases — reconcile
  transitions, generation/observed-generation bookkeeping, optimistic-concurrency conflicts,
  tombstone/uniqueness behavior, event-store projection. Fast, no transport.
- **Facade contract tests** validate **semantic API parity** between the local and remote facade:
  one `contract_test!` body runs against both, so behavior (including error taxonomy and batch
  positional results) stays identical. This is the canonical place to lock in facade behavior.
- **CLI E2E tests** validate **black-box user behavior** and local/remote **context symmetry** —
  argument parsing, selector grammar, output formatting (golden views), confirmation/flag behavior.

**Avoid redundant pure-GraphQL or pure-remote tests.** Once the facade contract suite is strong, a
separate remote-only or GraphQL-only test is justified *only* when it covers something the contract
suite structurally cannot: transport/error mapping, GraphQL **schema compatibility**, authentication/
authorization at the transport edge, or **`cynic` response-deserialization** in the remote client.
Otherwise the behavior is already guaranteed for both implementations by the contract tests.

---

## 16. File/crate reference map

| Layer | Crate | Directory | Key files |
| --- | --- | --- | --- |
| Domain model | `kamu-resources` | `src/domain/resources/domain/src` | `core/`, `state/`, `values/`, `manifests/`, `repo/`, `dispatchers/`, `messages/`, `use_cases/`, `views/` |
| Services | `kamu-resources-services` | `src/domain/resources/services/src` | `use_cases/{apply,reconcile,delete}.rs`, `crud_dispatchers/resource_crud_dispatcher_registry.rs`, `message_handlers/`, `event_stores/`, `dependencies.rs` |
| Facade | `kamu-resources-facade` | `src/domain/resources/facade/src/facade` | `resource_facade.rs`, `local/`, `graphql/` |
| GraphQL | (adapter) | `src/adapter/graphql/src` | `queries/resources/`, `mutations/resources_mut/` |
| CLI commands | (app/cli) | `src/app/cli/src/commands` | `apply_command.rs`, `list_resources_command.rs`, `get_resource_command.rs`, `delete_resources_command.rs`, `context_*_command.rs` |
| CLI services | (app/cli) | `src/app/cli/src/services/resources` | `resource_facade_factory.rs`, `resource_manifest_{discovery,execution}_service.rs`, `resource_type_lookup_service.rs`, `resource_selection_*_service.rs`, `resource_summary_service.rs`, `impl/` |
| Concrete types | `kamu-configuration` / `-services` | `src/domain/configuration/{domain,services}/src` | `resources/{variable_set,secret_set}/`, `reconcilers/`, `resource_crud_dispatchers/`, `dependencies.rs` |
| Concrete type (WIP) | `kamu-storage` / `-services` | `src/domain/storage/{domain,services}/src` | `Storage` type (`st`/`storage`) — registered in the CLI catalog but incomplete; same machinery as above |
| Tests | several | see [§15](#15-tests) | `resources/services/tests`, `resources/facade-tests`, `e2e/app/cli/repo-tests/src/commands/resources` |

---

## 17. Extension points & gotchas

- **`SQLX_OFFLINE=true`** makes `cargo` build/check/test/clippy validate against the offline SQLx
  cache instead of a live database — needed for agent/CI-style runs without a reachable Postgres.
  Developers with the local Docker Postgres/Elasticsearch services up may omit it. (Never needed for
  `fmt`/`doc`.)
- **Dispatch is by schema.** A missing schema yields
  `UnsupportedResourceDescriptorError::NotFound`; two matching registrations yield `Duplicate`.
  Selector-based lookup (`variablesets`, `vs`, etc.) is a separate metadata path and yields
  selector-specific not-found/duplicate errors.
- **Manifests are strict** — `#[serde(deny_unknown_fields)]` means a manifest cannot carry
  `status`, timestamps, or `generation`. Those are server-owned
  ([§5a](#5a-resource-anatomy--input-vs-auto-generated)).
- **`spec` is stored as `serde_json::Value`** — the framework is schema-agnostic; per-type structure
  and validation live in the type's `spec.rs`.
- **Schema strings are validated, not normalized** — `ResourceSchemaId::parse` rejects malformed
  URLs, whitespace, query strings, fragments, trailing slashes, and too-few path segments, but it
  preserves the accepted string (stored as the inner `TypeUri`) and allows non-`opendatafabric.org`
  hosts for future custom schema namespaces.
- **Secrets are encrypted before the first durable write** (by the spec sanitizer, not the
  reconciler) and only decrypted on read with `SpecViewMode::Revealed` (CLI `--revealed`, GraphQL
  `revealed: true`); the default `Encrypted` returns the stored ciphertext envelope. See
  [Secret handling](#secret-handling-invariant) — never assume plaintext is safe to log or emit.
- **Two-phase apply** — `--dry-run` / `plan_apply_manifest` never writes; only `apply_manifest`
  persists and publishes the outbox message.
- **Local ≡ remote is a contract-test invariant for covered behavior** — the `contract_test!` suite
  enforces parity only for the cases it exercises (not untested paths); when changing facade
  behavior, extend that suite so both implementations stay in lockstep.
- **Test convention:** use `assert_matches!` directly (never `assert!(matches!(...))`).
