# Contract Test Coverage Matrix

Each RF scenario from the original plan is listed with its status.

| RF ID   | File                | Status   | Description                                                           | Notes                                                                  |
|---------|---------------------|----------|-----------------------------------------------------------------------|------------------------------------------------------------------------|
| RF-001  | supported_kinds.rs  | Active   | Lists supported resource kinds                                        |                                                                        |
| RF-002  | supported_kinds.rs  | Active   | Supported kind aliases resolve to usable facade selectors             |                                                                        |
| RF-003  | supported_kinds.rs  | Active   | Unsupported kind is rejected consistently across all APIs             |                                                                        |
| RF-010  | apply_manifest.rs   | Active   | Plan create from JSON manifest                                        |                                                                        |
| RF-011  | apply_manifest.rs   | Active   | Plan create from YAML manifest                                        |                                                                        |
| RF-012  | apply_manifest.rs   | Active   | Plan update of existing resource                                      |                                                                        |
| RF-013  | apply_manifest.rs   | Active   | Plan unchanged manifest reports Untouched                             |                                                                        |
| RF-014  | apply_manifest.rs   | Active   | Plan rejects malformed manifest                                       |                                                                        |
| RF-015  | apply_manifest.rs   | Active   | Plan rejects schema-invalid manifest                                  |                                                                        |
| RF-016  | apply_manifest.rs   | Active   | Plan rejects business-invalid manifest                                | Combined with RF-025 in one test                                       |
| RF-020  | apply_manifest.rs   | Active   | Apply create from JSON manifest                                       |                                                                        |
| RF-021  | apply_manifest.rs   | Active   | Apply create from YAML manifest                                       |                                                                        |
| RF-022  | apply_manifest.rs   | Active   | Apply update changes metadata/spec                                    |                                                                        |
| RF-023  | apply_manifest.rs   | Active   | Apply unchanged manifest is idempotent                                |                                                                        |
| RF-024  | apply_manifest.rs   | Deferred | Apply rejects immutable field change                                  | Requires a resource kind with an immutable field                       |
| RF-025  | apply_manifest.rs   | Active   | Apply rejects invalid spec                                            | Combined with RF-016 in one test                                       |
| RF-026  | apply_manifest.rs   | Active   | Apply preserves duplicate metadata-key validation                     |                                                                        |
| RF-030  | get_identity.rs     | Active   | Get by name returns full resource view                                |                                                                        |
| RF-031  | get_identity.rs     | Active   | Get by UID returns same full resource view                            |                                                                        |
| RF-032  | get_identity.rs     | Active   | Get identity by name returns lightweight identity                     |                                                                        |
| RF-033  | get_identity.rs     | Active   | Get identity by UID returns same identity                             |                                                                        |
| RF-034  | get_identity.rs     | Active   | Get missing name returns NameNotFound                                 |                                                                        |
| RF-035  | get_identity.rs     | Active   | Get missing UID returns UIDNotFound                                   |                                                                        |
| RF-036  | get_identity.rs     | Active   | Get with wrong api_version returns ApiVersionMismatch                 |                                                                        |
| RF-037  | get_identity.rs     | Active   | Get by UID with wrong kind returns KindMismatch                       |                                                                        |
| RF-040  | spec_view_mode.rs   | Active   | Encrypted/default spec view hides secret material                     |                                                                        |
| RF-041  | spec_view_mode.rs   | Active   | Revealed spec view exposes revealable material                        |                                                                        |
| RF-042  | spec_view_mode.rs   | Active   | Spec view mode applies consistently to batch get                      |                                                                        |
| RF-043  | spec_view_mode.rs   | Active   | Spec view mode applies to manifest rendering                          |                                                                        |
| RF-050  | batch_ops.rs        | Active   | get_many all successes by mixed refs                                  |                                                                        |
| RF-051  | batch_ops.rs        | Active   | get_many mixed successes and lookup problems                          |                                                                        |
| RF-052  | batch_ops.rs        | Active   | get_many duplicate refs preserve request indexes                      |                                                                        |
| RF-053  | batch_ops.rs        | Active   | get_many empty refs returns empty result                              |                                                                        |
| RF-053A | batch_ops.rs        | Active   | Empty batch still validates unsupported kind                          | Extension beyond original plan                                         |
| RF-053B | batch_ops.rs        | Active   | Empty batch still validates bad account                               | Extension beyond original plan                                         |
| RF-053C | batch_ops.rs        | Active   | Empty-batch validation mirrors RF-053A/B across all batch ops         | Extension beyond original plan                                         |
| RF-054  | batch_ops.rs        | Active   | get_many wrong api_version produces per-item mismatch problems        |                                                                        |
| RF-055  | batch_ops.rs        | Active   | get_identities mirrors get_many lookup behavior                       |                                                                        |
| RF-056  | batch_ops.rs        | Active   | render_manifests all successes                                        |                                                                        |
| RF-057  | batch_ops.rs        | Active   | render_manifests mixed successes and problems                         |                                                                        |
| RF-058  | batch_ops.rs        | Active   | delete_many all successes                                             |                                                                        |
| RF-059  | batch_ops.rs        | Active   | delete_many mixed successes and problems                              |                                                                        |
| RF-060  | batch_ops.rs        | Active   | delete_many duplicate refs is deterministic                           |                                                                        |
| RF-061  | batch_ops.rs        | Active   | Batch APIs reject unsupported kind at batch level                     |                                                                        |
| RF-070  | render_manifest.rs  | Active   | Render JSON manifest by name                                          |                                                                        |
| RF-071  | render_manifest.rs  | Active   | Render YAML manifest by UID                                           |                                                                        |
| RF-072  | render_manifest.rs  | Active   | Rendered manifest can be reapplied unchanged                          |                                                                        |
| RF-073  | render_manifest.rs  | Active   | Render missing resource returns lookup problem                        |                                                                        |
| RF-074  | render_manifest.rs  | Active   | Render wrong api_version/kind returns mismatch problem                |                                                                        |
| RF-080  | list_search.rs      | Active   | List by kind returns summaries for account                            |                                                                        |
| RF-081  | list_search.rs      | Active   | list_identities by kind returns identities for account                |                                                                        |
| RF-082  | list_search.rs      | Active   | List supports pagination limit                                        |                                                                        |
| RF-083  | list_search.rs      | Active   | List supports pagination offset                                       |                                                                        |
| RF-084  | list_search.rs      | Active   | list_identities pagination mirrors list pagination                    |                                                                        |
| RF-085  | list_search.rs      | Active   | List empty account/kind returns empty result                          |                                                                        |
| RF-086  | list_search.rs      | Active   | List unsupported kind returns unsupported descriptor error            |                                                                        |
| RF-090  | list_search.rs      | Active   | Search by exact names                                                 |                                                                        |
| RF-091  | list_search.rs      | Active   | Search by exact names with missing names                              |                                                                        |
| RF-092  | list_search.rs      | Active   | Search by name pattern                                                |                                                                        |
| RF-093  | list_search.rs      | Active   | Search by multiple kinds                                              |                                                                        |
| RF-094  | list_search.rs      | Active   | Search with neither exact names nor pattern is rejected               |                                                                        |
| RF-095  | list_search.rs      | Active   | Search pagination and total_count                                     |                                                                        |
| RF-096  | list_search.rs      | Active   | Search account scoping                                                |                                                                        |
| RF-100  | list_all.rs         | Active   | list_all returns summaries across supported kinds                     |                                                                        |
| RF-101  | list_all.rs         | Active   | list_all_identities returns identities across supported kinds         |                                                                        |
| RF-102  | list_all.rs         | Active   | list_all pagination                                                   |                                                                        |
| RF-103  | list_all.rs         | Active   | list_all empty account returns empty result                           |                                                                        |
| RF-110  | summary.rs          | Active   | Summary for empty account                                             |                                                                        |
| RF-111  | summary.rs          | Active   | Summary counts resources by kind                                      |                                                                        |
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
| RF-134  | delete.rs           | Active   | Delete wrong api_version/kind returns mismatch problem                |                                                                        |
| RF-135  | delete.rs           | Active   | Delete is account-scoped                                              |                                                                        |
| RF-136  | delete.rs           | Active   | Repeated delete returns not found                                     |                                                                        |
| RF-140  | error_taxonomy.rs   | Active   | Single-resource lookup error taxonomy is consistent across get/render/delete |                                                                 |
| RF-141  | error_taxonomy.rs   | Active   | Batch lookup problem taxonomy mirrors single-resource taxonomy        |                                                                        |
| RF-142  | error_taxonomy.rs   | Active   | Batch-level error taxonomy (unsupported kind, bad account)            |                                                                        |
| RF-143  | error_taxonomy.rs   | Active   | Apply rejection taxonomy (InvalidMetadata, InvalidSpec, Rejected)     | Also split into apply_manifest.rs for apply-specific rejection cases   |
| RF-150  | cross_impl.rs       | Active   | Local and remote expose same supported kind descriptors               | Verified via `contract_test!` macro, not side-by-side comparison       |
| RF-151  | cross_impl.rs       | Active   | Local-created resource is readable remotely                           | True cross-facade test                                                 |
| RF-152  | cross_impl.rs       | Active   | Remote-created resource is readable locally                           | True cross-facade test                                                 |
| RF-153  | cross_impl.rs       | Active   | Local and remote render equivalent manifests                          | True cross-facade test                                                 |
| RF-154  | cross_impl.rs       | Active   | Local and remote produce equivalent batch responses                   | Verified via `contract_test!` macro, not side-by-side comparison       |
| RF-155  | cross_impl.rs       | Active   | Local and remote produce equivalent apply decisions                   | Verified via `contract_test!` macro, not side-by-side comparison       |
