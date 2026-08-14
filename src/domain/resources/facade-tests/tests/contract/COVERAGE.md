# Contract Test Coverage Matrix

Each RF scenario from the original plan is listed with its status.

| RF ID   | File                | Status   | Description                                                           | Notes                                                                  |
|---------|---------------------|----------|-----------------------------------------------------------------------|------------------------------------------------------------------------|
| RF-001  | supported_resource_types.rs  | Active   | Lists supported resource types                                       |                                                                        |
| RF-002  | supported_resource_types.rs  | Active   | Supported selector aliases resolve to usable facade selectors         |                                                                        |
| RF-003  | supported_resource_types.rs  | Active   | Unsupported type is rejected consistently across all APIs             |                                                                        |
| RF-010  | apply_manifest.rs   | Active   | Plan create from JSON manifest                                        |                                                                        |
| RF-011  | apply_manifest.rs   | Active   | Plan create from YAML manifest                                        |                                                                        |
| RF-012  | apply_manifest.rs   | Active   | Plan update of existing resource                                      |                                                                        |
| RF-013  | apply_manifest.rs   | Active   | Plan unchanged manifest reports Untouched                             |                                                                        |
| RF-014  | apply_manifest.rs   | Active   | Plan rejects malformed manifest                                       |                                                                        |
| RF-015  | apply_manifest.rs   | Active   | Plan rejects schema-invalid manifest                                  |                                                                        |
| RF-016  | apply_manifest.rs   | Active   | Plan rejects business-invalid manifest                                | Combined with RF-025 in one test                                       |
| RF-020  | apply_manifest.rs   | Active   | Apply create from JSON manifest                                       |                                                                        |
| RF-021  | apply_manifest.rs   | Active   | Apply create from YAML manifest                                       |                                                                        |
| RF-022  | apply_manifest.rs   | Active   | Apply update changes headers/spec                                    |                                                                        |
| RF-023  | apply_manifest.rs   | Active   | Apply unchanged manifest is idempotent                                |                                                                        |
| RF-024  | apply_manifest.rs   | Deferred | Apply rejects immutable field change                                  | Requires a resource type with an immutable field                      |
| RF-025  | apply_manifest.rs   | Active   | Apply rejects invalid spec                                            | Combined with RF-016 in one test                                       |
| RF-026  | apply_manifest.rs   | Active   | Apply preserves duplicate header-key validation                     |                                                                        |
| RF-026A | apply_manifest.rs   | Active   | Apply rejects invalid label/annotation header keys at parse time       | Extension beyond original plan                                         |
| RF-026B | apply_manifest.rs   | Active   | Apply round-trips populated labels and annotations                     | Extension beyond original plan                                         |
| RF-026C | apply_manifest.rs   | Active   | Plan/apply report extension header warnings                           | Covers free-form labels/annotations and non-indexable label values     |
| RF-026D | apply_manifest.rs   | Active   | Extension header canonicalization precedes diffing                    | Short-name after URI reapply reports `Untouched`                       |
| RF-026E | apply_manifest.rs   | Active   | Plan/apply reject invalid registered extension values as headers       | Surfaces as `InvalidHeaders` locally and remotely                      |
| RF-026F | apply_manifest.rs   | Active   | Plan/apply reject over-long description via annotation schema          | Uses `ResourceExtensionSchema`, not legacy `DescriptionTooLong`        |
| RF-030  | get_handle.rs     | Active   | Get by name returns full resource view                                |                                                                        |
| RF-031  | get_handle.rs     | Active   | Get by UID returns same full resource view                            |                                                                        |
| RF-032  | get_handle.rs     | Active   | Get handle by name returns lightweight handle                     |                                                                        |
| RF-033  | get_handle.rs     | Active   | Get handle by UID returns same handle                             |                                                                        |
| RF-034  | get_handle.rs     | Active   | Get missing name returns NameNotFound                                 |                                                                        |
| RF-035  | get_handle.rs     | Active   | Get missing UID returns IDNotFound                                   |                                                                        |
| RF-036  | get_handle.rs     | Active   | Get with wrong schema returns SchemaMismatch                 |                                                                        |
| RF-037  | get_handle.rs     | Active   | Get by UID with wrong schema returns SchemaMismatch                       |                                                                        |
| RF-040  | spec_view_mode.rs   | Active   | Encrypted/default spec view hides secret material                     |                                                                        |
| RF-041  | spec_view_mode.rs   | Active   | Revealed spec view exposes revealable material                        |                                                                        |
| RF-042  | spec_view_mode.rs   | Active   | Spec view mode applies consistently to batch get                      |                                                                        |
| RF-043  | spec_view_mode.rs   | Active   | Spec view mode applies to manifest rendering                          |                                                                        |
| RF-050  | batch_ops.rs        | Active   | get_many all successes by mixed refs                                  |                                                                        |
| RF-051  | batch_ops.rs        | Active   | get_many mixed successes and lookup problems                          |                                                                        |
| RF-052  | batch_ops.rs        | Active   | get_many duplicate refs preserve request indexes                      |                                                                        |
| RF-053  | batch_ops.rs        | Active   | Empty refs returns an empty result across all batch ops               | RF-053A/B/C retired: the batch wrapper that carried a type and account to validate is gone |
| RF-054  | batch_ops.rs        | Active   | get_many wrong schema produces per-item mismatch problems        |                                                                        |
| RF-055  | batch_ops.rs        | Active   | get_handles mirrors get_many lookup behavior                       |                                                                        |
| RF-056  | batch_ops.rs        | Active   | render_manifests all successes                                        |                                                                        |
| RF-057  | batch_ops.rs        | Active   | render_manifests mixed successes and problems                         |                                                                        |
| RF-058  | batch_ops.rs        | Active   | delete_many all successes                                             |                                                                        |
| RF-059  | batch_ops.rs        | Active   | delete_many mixed successes and problems                              |                                                                        |
| RF-060  | batch_ops.rs        | Active   | delete_many duplicate refs is deterministic                           |                                                                        |
| RF-061  | batch_ops.rs        | Active   | Batch APIs reject unsupported type at batch level                     |                                                                        |
| RF-070  | render_manifest.rs  | Active   | Render JSON manifest by name                                          |                                                                        |
| RF-071  | render_manifest.rs  | Active   | Render YAML manifest by UID                                           |                                                                        |
| RF-072  | render_manifest.rs  | Active   | Rendered manifest can be reapplied unchanged                          |                                                                        |
| RF-073  | render_manifest.rs  | Active   | Render missing resource returns lookup problem                        |                                                                        |
| RF-074  | render_manifest.rs  | Active   | Render wrong schema returns mismatch problem                |                                                                        |
| RF-080  | list_search.rs      | Active   | List by type returns summaries for account                            |                                                                        |
| RF-081  | list_search.rs      | Active   | list_handles by type returns handles for account                |                                                                        |
| RF-082  | list_search.rs      | Active   | List supports pagination limit                                        |                                                                        |
| RF-083  | list_search.rs      | Active   | List supports pagination offset                                       |                                                                        |
| RF-084  | list_search.rs      | Active   | list_handles pagination mirrors list pagination                    |                                                                        |
| RF-085  | list_search.rs      | Active   | List empty account/type returns empty result                          |                                                                        |
| RF-086  | list_search.rs      | Active   | List unsupported type returns unsupported descriptor error            |                                                                        |
| RF-087  | list_search.rs      | Active   | List narrowed by query (pattern / exact names / exact ids)            | `list` keeps summary views while narrowing; vacuous empty list mirrors RF-094 |
| RF-090  | list_search.rs      | Active   | Search by exact names                                                 |                                                                        |
| RF-091  | list_search.rs      | Active   | Search by exact names with missing names                              |                                                                        |
| RF-091A | list_search.rs      | Active   | Search by exact ids                                                   | Extension beyond original plan                                         |
| RF-091B | list_search.rs      | Active   | Search by exact ids with missing ids                                  | Extension beyond original plan                                         |
| RF-091C | list_search.rs      | Active   | Search by exact ids is account-scoped                                 | Extension beyond original plan                                         |
| RF-092  | list_search.rs      | Active   | Search by name pattern                                                |                                                                        |
| RF-093  | list_search.rs      | Active   | Search by multiple types                                              |                                                                        |
| RF-094  | list_search.rs      | Active   | Search with an empty exact-names/ids list is vacuous, not rejected    | `ResourceQuery` makes "no query mode" unrepresentable                   |
| RF-095  | list_search.rs      | Active   | Search pagination and total_count                                     |                                                                        |
| RF-096  | list_search.rs      | Active   | Search account scoping                                                |                                                                        |
| RF-097  | list_search.rs      | Active   | List filter by canonical label URI is accepted                        | Local-only: repo matching is Phase 9, remote transport is Phase 10   |
| RF-098  | list_search.rs      | Active   | List filter by short label name is accepted                           | Local-only, same reason as RF-097                                     |
| RF-099  | list_search.rs      | Active   | List filter by free-form label is accepted                            | Local-only, same reason as RF-097                                     |
| RF-099A | list_search.rs      | Active   | List filter invalid key is rejected                                   | Local-only, same reason as RF-097                                     |
| RF-099B | list_search.rs      | Active   | List filter unknown URI is rejected                                   | Local-only, same reason as RF-097                                     |
| RF-099C | list_search.rs      | Active   | List filter non-string value is rejected                              | Local-only, same reason as RF-097                                     |
| RF-099D | list_search.rs      | Active   | List filter duplicate-after-canonicalization is rejected              | Local-only, same reason as RF-097                                     |
| RF-099E | list_search.rs      | Active   | List filter `$not` operator is rejected                               | Recognized (ODF `LabelFilter` schema shape) but not evaluated yet     |
| RF-099F | list_search.rs      | Active   | List filter `$or` operator is rejected                                | Recognized but not evaluated yet, same reason as RF-099E              |
| RF-099G | list_search.rs      | Active   | List filter malformed `$not` operator is rejected                     | Parse failure shares the same code as a well-formed but unevaluated `$not` |
| RF-100  | list_all.rs         | Active   | list_all returns summaries across supported types                     |                                                                        |
| RF-101  | list_all.rs         | Active   | list_all_handles returns handles across supported types         |                                                                        |
| RF-102  | list_all.rs         | Active   | list_all pagination                                                   |                                                                        |
| RF-103  | list_all.rs         | Active   | list_all empty account returns empty result                           |                                                                        |
| RF-104  | list_all.rs         | Active   | list_all narrowed by scope: type subset + per-type query              | Pins positional type/query pairing, so a cross-wired scope fails       |
| RF-110  | summary.rs          | Active   | Summary for empty account                                             |                                                                        |
| RF-111  | summary.rs          | Active   | Summary counts resources by type                                      |                                                                        |
| RF-112  | summary.rs          | Active   | Summary phase counts (pending → ready transition)                     | Reconciling is an internal transient not observable at facade granularity |
| RF-113  | summary.rs          | Active   | Summary account scoping                                               |                                                                        |
| RF-120  | account_scoping.rs  | Active   | Default account selector resolves to current account                  |                                                                        |
| RF-121  | account_scoping.rs  | Active   | Account by name resolves correctly                                    |                                                                        |
| RF-122  | account_scoping.rs  | Active   | Account by id resolves correctly                                      |                                                                        |
| RF-123  | account_scoping.rs  | Active   | Account name/id mismatch is rejected                                  |                                                                        |
| RF-124  | account_scoping.rs  | Active   | Unknown account name/id is rejected                                   |                                                                        |
| RF-125  | account_scoping.rs  | Active   | Account isolation across all read APIs                                |                                                                        |
| RF-130  | delete.rs           | Active   | Delete by name removes resource                                       |                                                                        |
| RF-131  | delete.rs           | Active   | Delete by UID removes resource                                        |                                                                        |
| RF-132  | delete.rs           | Active   | Delete missing name returns lookup problem                            |                                                                        |
| RF-133  | delete.rs           | Active   | Delete missing UID returns lookup problem                             |                                                                        |
| RF-134  | delete.rs           | Active   | Delete wrong schema returns mismatch problem                |                                                                        |
| RF-135  | delete.rs           | Active   | Delete is account-scoped                                              |                                                                        |
| RF-136  | delete.rs           | Active   | Repeated delete returns not found                                     |                                                                        |
| RF-140  | error_taxonomy.rs   | Active   | Single-resource lookup error taxonomy is consistent across get/render/delete |                                                                 |
| RF-141  | error_taxonomy.rs   | Active   | Batch lookup problem taxonomy mirrors single-resource taxonomy        |                                                                        |
| RF-142  | error_taxonomy.rs   | Active   | Batch-level error taxonomy (unsupported type, bad account)            |                                                                        |
| RF-143  | error_taxonomy.rs   | Active   | Apply rejection taxonomy (InvalidHeaders, InvalidSpec, Rejected)     | Also split into apply_manifest.rs for apply-specific rejection cases   |
| RF-150  | cross_impl.rs       | Active   | Local and remote expose same supported type descriptors               | Verified via `contract_test!` macro, not side-by-side comparison       |
| RF-151  | cross_impl.rs       | Active   | Local-created resource is readable remotely                           | True cross-facade test                                                 |
| RF-152  | cross_impl.rs       | Active   | Remote-created resource is readable locally                           | True cross-facade test                                                 |
| RF-153  | cross_impl.rs       | Active   | Local and remote render equivalent manifests                          | True cross-facade test                                                 |
| RF-154  | cross_impl.rs       | Active   | Local and remote produce equivalent batch responses                   | Verified via `contract_test!` macro, not side-by-side comparison       |
| RF-155  | cross_impl.rs       | Active   | Local and remote produce equivalent apply decisions                   | Verified via `contract_test!` macro, not side-by-side comparison       |
| RF-160  | apply_manifest_batch.rs | Active | Batch apply all successes preserve order and persist                  |                                                                        |
| RF-161  | apply_manifest_batch.rs | Active | Batch apply stops on business rejection and reports rollback metadata | Physical rollback is covered by CLI E2E against real storage           |
| RF-162  | apply_manifest_batch.rs | Active | Batch apply stops on hard failure with typed error reconstruction     | Uses ParseManifest as the hard failure                                 |
| RF-163  | apply_manifest_batch.rs | Active | Batch rollback reconstructs IDNotFound as typed error                 | Covers remote `extensions.batch` decode taxonomy                       |
| RF-164  | apply_manifest_batch.rs | Active | Batch rollback reconstructs TypeMismatch as typed error               | Covers remote `extensions.batch` decode taxonomy                       |
| RF-165  | apply_manifest_batch.rs | Active | Batch dry-run stops on business rejection and persists nothing        |                                                                        |
| RF-166  | apply_manifest_batch.rs | Active | Batch dry-run same-name create/update plans both as create            | Pins no same-batch write visibility during planning                    |
| RF-167  | apply_manifest_batch.rs | Active | Live batch same-name create/update reads own writes                   |                                                                        |
| RF-168  | apply_manifest_batch.rs | Active | Raw GraphQL batch rejection returns rollback extensions               | Verifies `extensions.batch` envelope                                   |
| RF-169  | list_search.rs      | Active   | Search with AnyType scope spans every schema, still respects account  | Covers `RawResourceScope::AnyType`                                     |
