# Contract Test Coverage Matrix

Each `RF-*` contract scenario is listed with its status. Test code and comments may cite these IDs; keep this map in sync when adding or renumbering scenarios.

| RF ID | File | Status | Description | Notes |
|---|---|---|---|---|
| RF-001 | supported_resource_types.rs | Active | Lists supported resource types |  |
| RF-002 | supported_resource_types.rs | Active | Supported selector aliases resolve to usable facade selectors |  |
| RF-003 | supported_resource_types.rs | Active | Unsupported type is rejected consistently across all APIs |  |
| RF-010 | apply_manifest.rs | Active | Plan create from JSON manifest |  |
| RF-011 | apply_manifest.rs | Active | Plan create from YAML manifest |  |
| RF-012 | apply_manifest.rs | Active | Plan update of existing resource |  |
| RF-013 | apply_manifest.rs | Active | Plan unchanged manifest reports Untouched |  |
| RF-014 | apply_manifest.rs | Active | Plan rejects malformed manifest |  |
| RF-015 | apply_manifest.rs | Active | Plan rejects schema-invalid manifest |  |
| RF-016 | apply_manifest.rs | Active | Plan rejects business-invalid manifest | Combined with RF-025 in one test |
| RF-020 | apply_manifest.rs | Active | Apply create from JSON manifest |  |
| RF-021 | apply_manifest.rs | Active | Apply create from YAML manifest |  |
| RF-022 | apply_manifest.rs | Active | Apply update changes headers/spec |  |
| RF-023 | apply_manifest.rs | Active | Apply unchanged manifest is idempotent |  |
| RF-024 | immutable_specs.rs | Active | Plan and apply reject a changed immutable spec | On `ImmutableTestResource`; an unchanged manifest stays a no-op |
| RF-025 | apply_manifest.rs | Active | Apply rejects invalid spec | Combined with RF-016 in one test |
| RF-026 | apply_manifest.rs | Active | Apply preserves duplicate header-key validation |  |
| RF-026A | apply_manifest.rs | Active | Apply rejects invalid label/annotation header keys at parse time | Extension beyond original plan |
| RF-026B | apply_manifest.rs | Active | Apply round-trips populated labels and annotations | Extension beyond original plan |
| RF-026C | apply_manifest.rs | Active | Plan/apply report extension header warnings | Covers free-form labels/annotations and non-indexable label values |
| RF-026D | apply_manifest.rs | Active | Extension header canonicalization precedes diffing | Short-name after URI reapply reports `Untouched` |
| RF-026E | apply_manifest.rs | Active | Plan/apply reject invalid registered extension values as headers | Surfaces as `InvalidHeaders` locally and remotely |
| RF-026F | apply_manifest.rs | Active | Plan/apply reject over-long description via annotation schema | Uses `ResourceExtensionSchema`, not legacy `DescriptionTooLong` |
| RF-027 | apply_manifest.rs | Active | Apply documents never expose SecretSet plaintext | The pre-apply hook runs before planning, so `before`/`after` carry ciphertext |
| RF-028 | apply_manifest.rs | Active | Apply documents match the rendered manifest | Shared canonicalization with `render_manifests` |
| RF-029 | apply_manifest.rs | Active | Apply document `before` is absent only for creates | Unchanged apply still carries `before`; guards placeholder regressions |
| RF-030 | get_handle.rs | Active | Get by name returns full resource view |  |
| RF-031 | get_handle.rs | Active | Get by UID returns same full resource view |  |
| RF-032 | get_handle.rs | Active | Get handle by name returns lightweight handle |  |
| RF-033 | get_handle.rs | Active | Get handle by UID returns same handle |  |
| RF-034 | get_handle.rs | Active | Get missing name returns NameNotFound |  |
| RF-035 | get_handle.rs | Active | Get missing UID returns IDNotFound |  |
| RF-036 | get_handle.rs | Active | Get with wrong schema returns SchemaMismatch |  |
| RF-037 | get_handle.rs | Active | Get by UID with wrong schema returns SchemaMismatch |  |
| RF-040 | spec_view_mode.rs | Active | Encrypted/default spec view hides secret material |  |
| RF-041 | spec_view_mode.rs | Active | Revealed spec view exposes revealable material |  |
| RF-042 | spec_view_mode.rs | Active | Spec view mode applies consistently to batch get |  |
| RF-043 | spec_view_mode.rs | Active | Spec view mode applies to manifest rendering |  |
| RF-050 | batch_ops.rs | Active | get all successes by mixed refs |  |
| RF-051 | batch_ops.rs | Active | get mixed successes and lookup problems |  |
| RF-052 | batch_ops.rs | Active | get duplicate refs preserve request indexes |  |
| RF-053 | batch_ops.rs | Active | Empty refs returns an empty result across all batch ops | RF-053A/B/C retired: the batch wrapper that carried a type and account to validate is gone |
| RF-054 | batch_ops.rs | Active | get wrong schema produces per-item mismatch problems |  |
| RF-055 | batch_ops.rs | Active | get_handles mirrors get lookup behavior |  |
| RF-056 | batch_ops.rs | Active | render_manifests all successes |  |
| RF-057 | batch_ops.rs | Active | render_manifests mixed successes and problems |  |
| RF-058 | batch_ops.rs | Active | delete all successes |  |
| RF-059 | batch_ops.rs | Active | delete mixed successes and problems |  |
| RF-060 | batch_ops.rs | Active | delete duplicate refs is deterministic |  |
| RF-061 | batch_ops.rs | Active | Batch APIs reject unsupported type at batch level |  |
| RF-070 | render_manifest.rs | Active | Render JSON manifest by name |  |
| RF-071 | render_manifest.rs | Active | Render YAML manifest by UID |  |
| RF-072 | render_manifest.rs | Active | Rendered manifest can be reapplied unchanged |  |
| RF-073 | render_manifest.rs | Active | Render missing resource returns lookup problem |  |
| RF-074 | render_manifest.rs | Active | Render wrong schema returns mismatch problem |  |
| RF-080 | list_search.rs | Active | List by type returns summaries for account |  |
| RF-081 | list_search.rs | Active | search_handles by type returns handles for account | Was `list_handles` before the listing collapse |
| RF-082 | list_search.rs | Active | List supports pagination limit |  |
| RF-083 | list_search.rs | Active | List supports pagination offset |  |
| RF-084 | list_search.rs | Active | search_handles pagination mirrors search pagination | Was `list_handles`/`list` before the listing collapse |
| RF-085 | list_search.rs | Active | List empty account/type returns empty result |  |
| RF-086 | list_search.rs | Active | List unsupported type returns unsupported descriptor error |  |
| RF-087 | list_search.rs | Active | Search narrowed by selectors (name pattern / id) | An empty selector list is vacuous, not an error |
| RF-088 | list_search.rs | Active | search_handles honours selectors | Pins the stage-4 behaviour change: `list_handles` used to hardcode an unnarrowed scope while `list` accepted a query; the collapse kept it |
| RF-090 | list_search.rs | Active | Search by exact names | An exact name is a wildcard-free `LIKE` pattern: a selector's `name` is a pattern by ODF definition, so listing has no separate exact-name mode |
| RF-091 | list_search.rs | Active | Search by exact names with missing names | Same wildcard-free-pattern form as RF-090 |
| RF-091A | list_search.rs | Active | Search by exact ids | Extension beyond original plan |
| RF-091B | list_search.rs | Active | Search by exact ids with missing ids | Extension beyond original plan |
| RF-091C | list_search.rs | Active | Search by exact ids is account-scoped | Extension beyond original plan |
| RF-092 | list_search.rs | Active | Search by name pattern |  |
| RF-093 | list_search.rs | Active | Search by multiple types |  |
| RF-094 | list_search.rs | Active | Search with an empty selector list is vacuous, not rejected | Empty is "match nothing", *not* "match everything" — the latter needs an explicit type-less, unnarrowed selector |
| RF-095 | list_search.rs | Active | Search pagination and total_count |  |
| RF-096 | list_search.rs | Active | Search account scoping |  |
| RF-097 | list_search.rs | Active | List filter by canonical label URI is accepted | Runs local and remote; the transport carries labels per selector |
| RF-098 | list_search.rs | Active | List filter by short label name is accepted | Runs local and remote, same as RF-097 |
| RF-099 | list_search.rs | Active | List filter by free-form label is accepted | Runs local and remote, same as RF-097 |
| RF-099A | list_search.rs | Active | List filter invalid key is rejected | Runs local and remote, same as RF-097 |
| RF-099B | list_search.rs | Active | List filter unknown URI is rejected | Runs local and remote, same as RF-097 |
| RF-099C | list_search.rs | Active | List filter non-string value is rejected | Runs local and remote, same as RF-097 |
| RF-099D | list_search.rs | Active | List filter duplicate-after-canonicalization is rejected | Runs local and remote, same as RF-097 |
| RF-099E | list_search.rs | Active | List filter `$not` operator is rejected | Recognized (ODF `LabelFilter` schema shape) but not evaluated yet |
| RF-099F | list_search.rs | Active | List filter `$or` operator is rejected | Recognized but not evaluated yet, same reason as RF-099E |
| RF-099G | list_search.rs | Active | List filter malformed `$not` operator is rejected | Parse failure shares the same code as a well-formed but unevaluated `$not` |
| RF-099H | list_search.rs | Active | search_handles label filter narrows candidates | Extension beyond original plan |
| RF-100 | search_any_type.rs | Active | search returns summaries across supported types | Was `list_all` before the listing collapse |
| RF-101 | search_any_type.rs | Active | search_handles returns handles across supported types | Was `list_all_handles`, which had no scope at all; now a type-less unnarrowed selector |
| RF-102 | search_any_type.rs | Active | search pagination across types | Pagination is global across types, not per type |
| RF-103 | search_any_type.rs | Active | search empty account returns empty result |  |
| RF-104 | search_any_type.rs | Active | search narrowed by selectors: type subset + per-selector pattern | Pins per-selector type/pattern pairing, so a cross-wired selector list fails |
| RF-105 | list_search.rs | Active | Per-selector `account` is authorized, and denial fails the whole call | Denial leaks nothing about the named account; one denied selector fails the call. Covers both `search_handles` and `search` |
| RF-105B | list_search.rs | Active | An admin may search another account's resources | Local only: the admin is a predefined account outside `TestAccount` |
| RF-105C | list_search.rs | Active | `Principal::Node` cannot search, with or without an account selector | Local only: the remote transport cannot express `Principal::Node`. The node has no account to default unscoped selectors to, and search always resolves that default; both cases are reported as an account selector problem |
| RF-106 | list_search.rs | Active | Type-less selector scope limits (`UnrepresentableScopeError`) | `AnyType` limits: mixed with typed selectors, multiple query modes, and two type-less selectors naming different accounts. Asserted on message, not variant |
| RF-107 | list_search.rs | Active | Typed list columns are rendered, across several types in one search | The schema-specific columns `kamu list` shows (`variables`, `secrets`), rendered across several types in one search |
| RF-108 | batch_ops.rs | Active | One batch spans several types, and one account spelled several ways | Asserts positional indexes, so a fan-out that lost ordering fails |
| RF-110 | summary.rs | Active | Summary for empty account |  |
| RF-111 | summary.rs | Active | Summary counts resources by type |  |
| RF-112 | summary.rs | Active | Summary phase counts (pending → ready transition) | Reconciling is an internal transient not observable at facade granularity |
| RF-113 | summary.rs | Active | Summary account scoping |  |
| RF-114 | summary.rs | Active | Summary counts resources whose deletion was requested | `Deleting` until the controllers clean up, then `Deleted`; the total includes both |
| RF-120 | account_scoping.rs | Active | Default account selector resolves to current account |  |
| RF-121 | account_scoping.rs | Active | Account by name resolves correctly |  |
| RF-122 | account_scoping.rs | Active | Account by id resolves correctly |  |
| RF-122B | account_scoping.rs | Active | Agreeing account name and id resolve correctly | Extension beyond original plan; the positive counterpart to RF-123 |
| RF-123 | account_scoping.rs | Active | Account name/id mismatch is rejected |  |
| RF-124 | account_scoping.rs | Active | Unknown account name/id is rejected |  |
| RF-125 | account_scoping.rs | Active | Account isolation across all read APIs |  |
| RF-126 | account_scoping.rs | Active | Explicit empty account selector `{}` is rejected | Distinct from omitting `account` entirely (RF-120); rejected by account resolution, not parsing |
| RF-130 | delete.rs | Active | Delete by name removes resource | Reports `Deleting`: the controllers clean up after the request. Then hidden from names and listings, and found by id as `Deleted` |
| RF-131 | delete.rs | Active | Delete by UID removes resource |  |
| RF-132 | delete.rs | Active | Delete missing name returns lookup problem |  |
| RF-133 | delete.rs | Active | Delete missing UID returns lookup problem |  |
| RF-134 | delete.rs | Active | Delete wrong schema returns mismatch problem |  |
| RF-135 | delete.rs | Active | Delete is account-scoped |  |
| RF-136 | delete.rs | Active | Repeated delete of a deleted resource returns not found |  |
| RF-137 | delete.rs | Active | Delete reports `Deleting` until controllers clean up | While deleting: found by id, hidden from names and from both `search` and `search_handles` unless `IncludeDeleting` (a `search` summary then reports `Deleting`); once cleaned up: still found by id, hidden from the others unless `IncludeDeleted` or `DeletedOnly` |
| RF-138 | delete.rs | Active | A resource being deleted gives up its name | Re-apply creates a new resource with a `replaces_resource_being_deleted` warning; with `IncludeDeleting`, `get_handles`, `get` and `render_manifests` by name return both under one request index, live first; re-apply over a deleted resource creates one without the warning; with `IncludeDeleted` the name finds the live resource and then the deleted ones, newest deletion first |
| RF-139 | delete.rs | Active | Repeated delete of a resource being deleted reports `Deleting` | By id only: `delete` resolves names to live resources; the controller runtime completes the deletion |
| RF-140 | error_taxonomy.rs | Active | One-element-batch lookup taxonomy is consistent across get/get_handles/render_manifests/delete | Full 3x4 matrix (NameNotFound/IDNotFound/SchemaMismatch x 4 methods); RF-141 covers only a subset, so this was converted to batch form rather than retired |
| RF-141 | error_taxonomy.rs | Active | Multi-ref batch lookup problem taxonomy mirrors the one-element case |  |
| RF-142 | error_taxonomy.rs | Active | Batch-level error taxonomy (unsupported type, bad account) |  |
| RF-143 | error_taxonomy.rs | Active | Apply rejection taxonomy (InvalidHeaders, InvalidSpec, Rejected) | Also split into apply_manifest.rs for apply-specific rejection cases, and immutable_specs.rs for `ImmutableFieldChanged` and `ReferencedObjectMissing` |
| RF-150 | cross_impl.rs | Active | Local and remote expose same supported type descriptors | Verified via `contract_test!` macro, not side-by-side comparison |
| RF-151 | cross_impl.rs | Active | Local-created resource is readable remotely | True cross-facade test |
| RF-152 | cross_impl.rs | Active | Remote-created resource is readable locally | True cross-facade test |
| RF-153 | cross_impl.rs | Active | Local and remote render equivalent manifests | True cross-facade test |
| RF-154 | cross_impl.rs | Active | Local and remote produce equivalent batch responses | Verified via `contract_test!` macro, not side-by-side comparison |
| RF-155 | cross_impl.rs | Active | Local and remote produce equivalent apply decisions | Verified via `contract_test!` macro, not side-by-side comparison |
| RF-160 | apply_manifest_batch.rs | Active | Batch apply all successes preserve order and persist |  |
| RF-161 | apply_manifest_batch.rs | Active | Batch apply stops on business rejection and reports rollback metadata | Physical rollback is covered by CLI E2E against real storage |
| RF-162 | apply_manifest_batch.rs | Active | Batch apply stops on hard failure with typed error reconstruction | Uses ParseManifest as the hard failure |
| RF-163 | apply_manifest_batch.rs | Active | Batch rollback reconstructs IDNotFound as typed error | Covers remote `extensions.batch` decode taxonomy |
| RF-164 | apply_manifest_batch.rs | Active | Batch rollback reconstructs TypeMismatch as typed error | Covers remote `extensions.batch` decode taxonomy |
| RF-165 | apply_manifest_batch.rs | Active | Batch dry-run stops on business rejection and persists nothing |  |
| RF-166 | apply_manifest_batch.rs | Active | Batch dry-run same-name create/update plans both as create | Pins no same-batch write visibility during planning |
| RF-167 | apply_manifest_batch.rs | Active | Live batch same-name create/update reads own writes |  |
| RF-168 | apply_manifest_batch.rs | Active | Raw GraphQL batch rejection returns rollback extensions | Verifies `extensions.batch` envelope |
| RF-169 | list_search.rs | Active | Search with a type-less selector spans every schema, still respects account | Covers the type-less (`type: None`) selector, which resolves to `ResourceScope::AnyType` |
| RF-170 | batch_ops.rs | Active | A ref supplying both `id` and `name` fails if they disagree — and a case-only variant *agrees* | ODF allows the pair as a consistency assertion, so an unchecked `name` would let a ref read, render and delete the wrong resource. The case-only half pins `ResourceName`'s case-insensitive equality |
| RF-171 | batch_ops.rs | Active | A ref naming a resource but no type is refused on every batch path | RFC-018 § References allows only ID, DID, or type+name: `(account, type, name)` is the uniqueness key. Refusal lives in the shared front half, so all four paths are asserted together |
| RF-172 | — | Retired | A type-less ref whose name exists in several types is ambiguous, not a multi-match | Superseded by RF-171: a ref can no longer address by name without a type, so `AmbiguousType` is unreachable. Contrast RF-169, where several matches remain expected for a *selector* |
| RF-173 | spec_view_mode.rs | Active | Revealed spec view resolves its dispatcher per schema, not once for the whole batch | The spec-view dispatcher is schema-specific, so a batch spanning schemas must resolve one per item. Both orders asserted: they fail differently |
| RF-174 | list_search.rs | Active | A selector narrowed *only* by a field the facade cannot resolve is rejected, not widened | Dropping an unresolvable field would widen a selector into the whole account. Exercises `did`; asserted on both read APIs since the facade trait is public |
| RF-175 | list_search.rs | Active | Two selectors in one call filter by *different* labels | The capability per-selector labels exist for. Asserts the union of two independently-filtered selectors, and that `total_count` spans both |
| RF-176 | list_search.rs | Active | A labelled selector beside an unlabelled one does not leak its filter | What the coalescer's `(schema, account, labels)` grouping key guards: merging would make the unfiltered selector inherit the other's labels |
| RF-177 | list_search.rs | Active | A non-string label value on *one* selector fails the whole call | Only top-level string-valued labels are indexed, so such a predicate is unsatisfiable by construction. Pins the blast radius as the whole call |
| RF-178 | apply_manifest_batch.rs | Active | Batch rollback reconstructs AccountResolution as typed error | Covers remote `extensions.batch` decode taxonomy, like RF-163/164 |
| RF-179 | error_taxonomy.rs | Active | A name-without-type ref is refused, and distinguishably from `EmptyRef` | RF-171 pins the refusal; this pins its taxonomy, distinguishing it from `EmptyRef` and `NameNotFound` |
| RF-180 | list_search.rs | Active | A type-less selector may name an account, spanning every type under it | The only spelling for an all-types listing scoped to one account, there being no call-level `account`. RF-105 covers a typed selector's account |
| RF-181 | generations.rs | Active | `generation` counts header and spec changes, `statusGeneration` counts status updates | Walks apply → reconcile → spec update → labels-only update → unchanged re-apply. Between an update and its reconciliation the phase is derived as `Pending` without a status update |
| RF-182 | delete.rs | Active | Applying a manifest whose `headers.id` names a resource being deleted is rejected | Plan and apply both reject with `LifecycleRuleConflict`; the resource stays `Deleting` with its spec unchanged |
| RF-183 | delete.rs | Active | Applying a manifest whose `headers.id` names a deleted resource is not found | Plan and apply alike |
| RF-184 | delete.rs | Active | `DeletedOnly` hides live resources, even from their ids | Also pins the listing order: newest deletion first, and with `IncludeDeleted` the live ones before it |
| RF-185 | delete.rs | Active | Label selectors find deleted resources | Deleted resources keep their label projection |
| RF-186 | spec_view_mode.rs | Active | The spec of a deleted resource is not revealed | A per-item problem from `get` and `render_manifests`; a live resource sharing the name is revealed beside it |
| RF-187 | generated_names.rs | Active | Applying a manifest without a name creates a resource each time and returns its generated name | On `TestResource`, the type that generates names |
| RF-188 | generated_names.rs | Active | Planning a manifest without a name reports the name as pending | A placeholder in `resource`, no name in the `after` document |
| RF-189 | generated_names.rs | Active | A manifest with an `id` and without a name keeps the name of the resource | Plan and apply alike |
| RF-190 | generated_names.rs | Active | A manifest without a name is refused for a type that does not generate names | Omitted, `null`, and with an `id`; `InvalidHeaders` from plan, apply and batch apply |
| RF-191 | owner_references.rs | Active | Owners are stored as handles and rendered in the manifest | Short form and id; the rendered manifest re-applies as `Untouched` |
| RF-192 | owner_references.rs | Active | Plan and apply reject a reference that does not name an owner | Missing, in another account, being deleted, mismatching type or name, repeated, the resource itself |
| RF-193 | owner_references.rs | Active | Owner references without a resolvable form are `InvalidHeaders` | Also too many of them |
| RF-194 | owner_references.rs | Active | A resource keeps the reference to its deleted owner through updates | Omitting `ownerReferences` drops it |
| RF-195 | owner_references.rs | Active | Search by owner narrows the selectors, with any visibility | By id, by name, and by both; `search` and `search_handles` agree, account-scoped |
| RF-196 | owner_references.rs | Active | The owner to search by is found by id even when gone, by name only when live | Lookup problems and unresolvable forms are typed errors; an unresolvable selector fails before the owner is looked up |
| RF-197 | owner_references.rs | Active | Owners carry the current name of a renamed account | In plan, apply and get |
| RF-198 | creation_provenance.rs | Active | A created resource records its creator's principal and origin in a `CreationProvenance` condition | Local records `Cli`, remote `Api` (`GraphQl`); an update keeps it |
| RF-199 | operation_guard.rs | Active | A type's placement puts a resource applied by a non-owner in the placed account | On `TestResource` with its access rules; without an account and naming the placed one. Applied again, the manifest finds the resource there: `Untouched`, then `Updated` with a changed value |
| RF-200 | operation_guard.rs | Active | A manifest naming another account than the placement is rejected | `BusinessValidationFailed` |
| RF-201 | operation_guard.rs | Active | A type's guard decides a create in another account | Without a grant it is `AccountAccess`, as under the default rule. A `headers.id` naming nothing, or a resource of another type, gets the same denial from plan and apply: the guard decides before the lookup |
| RF-202 | operation_guard.rs | Active | Get in another account is decided per resource | A resource the caller may not read is a not-found problem beside the one it may; one resource named twice is checked once and answered for both refs. A hidden ID, and an ID of another type, are `IDNotFound` with the text of a missing ID. A type-less ref keeps the default rule: the whole call is `AccountAccess` |
| RF-203 | operation_guard.rs | Active | Delete in another account is decided per resource | A readable resource the caller may not delete is an `Access` problem, an unreadable one not found; the permitted one is deleted. A resource named twice gets the same problem for both refs. The denial keeps its kind and detail remotely. Deleting again a deleted resource the caller may read is `IDNotFound`, not `Access` |
| RF-204 | operation_guard.rs | Active | Listing another account needs the filter the type's guard asks for | A selector without a type keeps the default rule; a pattern that may match what the caller may not read is refused |
| RF-205 | operation_guard.rs | Active | A name matching resources the caller may read and ones it may not answers with the readable ones only | Under `IncludeDeleting`, by `get` and `get_handles`; a name matching only hidden resources is one `NameNotFound` |
| RF-206 | operation_guard.rs | Active | An owner the caller may not read is rejected as a missing one | By id or name, with a mismatching name or type, deleted, or repeated; plan and apply agree. A readable owner is accepted and checked against the reference. An owner of a type whose guard refuses the whole account (`VariableSet`) is rejected the same way |
| RF-207 | account_scoping.rs | Active | An apply addressing a resource by `headers.id` finds it in the apply's own account only | Another account's ID is `IDNotFound` from plan and apply; the resource there keeps its spec |
| RF-208 | account_scoping.rs | Active | Under the default rule, batch refs naming another account fail the whole call | `get`, `get_handles`, `render_manifests` and `delete`, beside a ref the caller may use; `AccountAccess`, naming none of the resources. RF-105 covers search |
| RF-208B | account_scoping.rs | Active | An admin may use batch refs naming another account | Local only: the admin is a predefined account outside `TestAccount` |
| RF-209 | operation_guard.rs | Active | A type's guard decides an update by the current resource | On `TestResource`: an update the new spec alone would allow is `AccountAccess` when the current spec does not place the resource in the account |
| RF-210 | operation_guard.rs | Active | A placement rejecting the spec rejects plan and apply | On `TestResource`: a `place:` token naming a missing account is `ReferencedObjectMissing`, and nothing is created |
| RF-211 | owner_references.rs | Active | Owners named alike in several types resolve to their own types | `VariableSet:x` and `SecretSet:x` in one manifest are stored with their own ids and types |
| RF-212 | operation_guard.rs | Active | A guard that finds a create's object hidden denies it as an apply in an account the caller may not use | On `TestResource`, a `hide:` token: `AccountAccess` from plan and apply, as RF-201 |
