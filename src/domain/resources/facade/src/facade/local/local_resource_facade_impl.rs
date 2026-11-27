// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{Principal, RequestOrigin};
use kamu_resources::*;
use kamu_resources_services::{
    ResourceDispatcherFactory,
    ResourceExtensionSchemaResolver,
    request_creation_provenance,
};

use super::helpers::*;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceFacade)]
pub struct LocalResourceFacadeImpl {
    dispatcher_factory: Arc<ResourceDispatcherFactory>,
    resource_account_resolver: Arc<dyn ResourceAccountResolver>,
    principal: Arc<Principal>,
    request_origin: Arc<RequestOrigin>,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
    resource_extension_schema_resolver: Arc<ResourceExtensionSchemaResolver>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for LocalResourceFacadeImpl {
    async fn list_supported_resource_types(
        &self,
    ) -> Result<Vec<ResourceTypeDescriptor>, ListSupportedResourceTypesError> {
        Ok(self.list_resource_type_descriptors())
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(&self.principal, request.account.as_ref())
            .await?;

        let resource_counts = self
            .generic_resource_query_service
            .summarize_resources(target_account.did)
            .await?
            .into_iter()
            .map(|row| {
                let schema = TypeUri::new_unchecked(&row.schema);
                let type_name = resource_type_name(&schema)?;

                Ok(ResourceTypeCountSummary {
                    schema,
                    type_name,
                    total_count: row.total_count,
                    phase_counts: row.phase_counts,
                })
            })
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(ResourcesSummaryError::Internal)?;

        Ok(ResourcesSummary { resource_counts })
    }

    async fn get(
        &self,
        resource_refs: Vec<ResourceRef>,
        spec_view: SpecViewOpts,
        visibility: ResourceVisibility,
    ) -> Result<BatchResourceResponse<Resource, ResourceLookupProblem>, BatchResourceError> {
        let (indexed_resources, problems) = self
            .resolve_multiple_resource_views(resource_refs, spec_view, visibility)
            .await?;

        let successes = indexed_resources
            .into_iter()
            .map(|resource| BatchResourceSuccess {
                request_index: resource.request_index,
                item: resource.item,
            })
            .collect();

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn get_handles(
        &self,
        resource_refs: Vec<ResourceRef>,
        visibility: ResourceVisibility,
    ) -> Result<BatchResourceResponse<ResourceHandle, ResourceLookupProblem>, BatchResourceError>
    {
        // An empty batch names nothing, so it yields an empty response.
        let mut successes = Vec::new();
        let (groups, mut problems) = self
            .group_refs_by_target(resource_refs, ResourceAccountOperation::Read)
            .await?;

        for (group, access) in groups {
            let (indexes, resolution_response) = self
                .resolve_permitted_ids(
                    &group.account,
                    &group.schema,
                    group.entries,
                    access,
                    ResourceAccountOperation::Read,
                    visibility,
                )
                .await?;

            let (handles, group_problems) = self
                .resolve_id_handle_groups(
                    &group.account.did,
                    &group.schema,
                    resolution_response.id_entries,
                    resolution_response.problems,
                    visibility,
                )
                .await?;

            // `group_batch_resource_refs` re-indexes from zero within the
            // group, so map back to the caller's positions before merging.
            successes.extend(handles.into_iter().map(|handle| BatchResourceSuccess {
                request_index: indexes[handle.request_index],
                item: handle.item,
            }));
            problems.extend(
                group_problems
                    .into_iter()
                    .map(|problem| BatchResourceProblem {
                        request_index: indexes[problem.request_index],
                        error: problem.error,
                    }),
            );
        }

        successes.sort_by_key(|success| success.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn render_manifests(
        &self,
        resource_refs: Vec<ResourceRef>,
        format: ResourceManifestFormat,
        spec_view: SpecViewOpts,
        visibility: ResourceVisibility,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        let (indexed_resources, problems) = self
            .resolve_multiple_resource_views(resource_refs, spec_view, visibility)
            .await?;

        let successes = indexed_resources
            .into_iter()
            .map(|resource| {
                let manifest = kamu_resources::ResourceManifest::from_resource(&resource.item)
                    .map_err(BatchResourceError::Internal)?;
                let manifest =
                    serialize_manifest(&manifest, format).map_err(BatchResourceError::Internal)?;
                Ok(BatchResourceSuccess {
                    request_index: resource.request_index,
                    item: RenderResourceManifestResult { manifest, format },
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn search(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<SearchResourcesResponse, ListResourcesError> {
        let Some(prepared) = self.prepare_search(request).await? else {
            return Ok(SearchResourcesResponse {
                items: Vec::new(),
                total_count: 0,
            });
        };

        // One scoped query covering every type in the scope, so pagination is
        // global rather than per type. Querying each type separately cannot
        // produce this: page 2 of a merged result is not page 2 of each type.
        let snapshots = self
            .generic_resource_query_service
            .list_snapshots(
                &prepared.account_id,
                &prepared.scope,
                prepared.visibility,
                prepared.owner_id,
                prepared.pagination,
            )
            .await?;

        let items = self.summary_views_with_columns(snapshots)?;

        Ok(SearchResourcesResponse {
            items,
            total_count: prepared.total_count,
        })
    }

    async fn search_handles(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<SearchResourceHandlesResponse, ListResourcesError> {
        let Some(prepared) = self.prepare_search(request).await? else {
            return Ok(SearchResourceHandlesResponse {
                items: Vec::new(),
                total_count: 0,
            });
        };

        let rows = self
            .generic_resource_query_service
            .search_resource_handles(
                &prepared.account_id,
                &prepared.scope,
                prepared.visibility,
                prepared.owner_id,
                prepared.pagination,
            )
            .await?;

        let items = rows
            .into_iter()
            .map(ResourceHandleRow::into_handle)
            .collect::<Vec<_>>();

        Ok(SearchResourceHandlesResponse {
            items,
            total_count: prepared.total_count,
        })
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError> {
        let prepared = self.prepare_apply_manifest(request).await?;

        let plan = prepared
            .dispatcher
            .plan_apply(ResourceCrudDispatcherApplyRequest {
                id: prepared.id,
                headers: prepared.header,
                account_source: prepared.account_source,
                access_check: ApplyResourceAccessCheck::ByGuard,
                spec: prepared.spec,
                provenance: prepared.provenance,
            })
            .await?;

        Ok(match plan {
            ApplyManifestPlanningDecision::Planned(mut plan) => {
                plan.warnings.splice(0..0, prepared.header_warnings);
                refresh_target_account(&mut plan.resource.headers, prepared.target_account);

                ApplyManifestPlanningDecision::Planned(plan)
            }
            ApplyManifestPlanningDecision::Rejected(rejection) => {
                ApplyManifestPlanningDecision::Rejected(rejection)
            }
        })
    }

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestApplicationDecision, ApplyManifestError> {
        let prepared = self.prepare_apply_manifest(request).await?;

        let result = prepared
            .dispatcher
            .apply(ResourceCrudDispatcherApplyRequest {
                id: prepared.id,
                headers: prepared.header,
                account_source: prepared.account_source,
                access_check: ApplyResourceAccessCheck::ByGuard,
                spec: prepared.spec,
                provenance: prepared.provenance,
            })
            .await?;

        Ok(match result {
            ApplyManifestApplicationDecision::Applied(mut result) => {
                result.warnings.splice(0..0, prepared.header_warnings);
                refresh_target_account(&mut result.resource.headers, prepared.target_account);

                ApplyManifestApplicationDecision::Applied(result)
            }
            ApplyManifestApplicationDecision::Rejected(rejection) => {
                ApplyManifestApplicationDecision::Rejected(rejection)
            }
        })
    }

    async fn plan_apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestPlanningDecision>, BatchResourceError> {
        let mut items = Vec::with_capacity(request.items.len());

        for (request_index, item) in request.items.into_iter().enumerate() {
            let outcome = self.plan_apply_manifest(item).await;
            let stop = matches!(outcome, Ok(ApplyManifestPlanningDecision::Rejected(_)))
                || outcome.is_err();

            items.push(ApplyManifestBatchItemResult {
                request_index,
                outcome,
            });

            if stop {
                break;
            }
        }

        Ok(ApplyManifestBatchResponse {
            items,
            rolled_back_successes: Vec::new(),
        })
    }

    async fn apply_manifests(
        &self,
        request: ApplyManifestBatchRequest,
    ) -> Result<ApplyManifestBatchResponse<ApplyManifestApplicationDecision>, BatchResourceError>
    {
        let mut items = Vec::with_capacity(request.items.len());

        for (request_index, item) in request.items.into_iter().enumerate() {
            let outcome = self.apply_manifest(item).await;
            let stop = matches!(outcome, Ok(ApplyManifestApplicationDecision::Rejected(_)))
                || outcome.is_err();

            items.push(ApplyManifestBatchItemResult {
                request_index,
                outcome,
            });

            if stop {
                break;
            }
        }

        Ok(ApplyManifestBatchResponse {
            items,
            rolled_back_successes: Vec::new(),
        })
    }

    async fn delete(
        &self,
        resource_refs: Vec<ResourceRef>,
    ) -> Result<BatchResourceResponse<ResourceDeletion, ResourceLookupProblem>, BatchResourceError>
    {
        // An empty batch names nothing, so it yields an empty response.
        let mut successes = Vec::new();
        let (groups, mut problems) = self
            .group_refs_by_target(resource_refs, ResourceAccountOperation::Delete)
            .await?;

        // One dispatcher call per `(account, schema)` group. The request runs in one
        // transaction (the `#[transactional_handler]` GraphQL handler, or the CLI command's
        // `transaction!` block), so a failure in a later group rolls back earlier deletions.
        for (group, access) in groups {
            let (indexes, resolution_response) = self
                .resolve_permitted_ids(
                    &group.account,
                    &group.schema,
                    group.entries,
                    access,
                    ResourceAccountOperation::Delete,
                    ResourceVisibility::Active,
                )
                .await?;

            problems.extend(resolution_response.problems.into_iter().map(|problem| {
                BatchResourceProblem {
                    request_index: indexes[problem.request_index],
                    error: problem.error,
                }
            }));

            let mut seen_valid_ids = HashSet::new();
            let mut ids_to_delete = Vec::<ResourceID>::new();
            let mut deleted_entries = Vec::new();

            let ids = resolution_response
                .id_entries
                .iter()
                .map(|(_, _, id)| *id)
                .collect::<Vec<_>>();

            let rows_by_id = self
                .generic_resource_query_service
                .find_resource_handles_by_ids(&group.account.did, &ids)
                .await?
                .into_iter()
                .map(|row| (row.id, row))
                .collect::<HashMap<_, _>>();

            for (request_index, resource_ref, id) in resolution_response.id_entries {
                let row_result = rows_by_id
                    .get(id.as_ref())
                    .filter(|row| row.deleted_at.is_none())
                    .cloned()
                    .ok_or_else(|| ref_not_found(&group.schema, &resource_ref, id));

                // Only a ref that supplied its own id asserts a name to verify:
                // for a name-keyed ref the name *is* how the id was found.
                let expected_name = if resource_ref.id.is_some() {
                    resource_ref.name.as_ref()
                } else {
                    None
                };

                match row_result.and_then(|row| {
                    validate_handle_row(
                        row,
                        &group.schema,
                        expected_name,
                        ensure_schema_matches::<ResourceLookupProblem>,
                    )
                }) {
                    Ok(_) => {
                        deleted_entries.push((indexes[request_index], id));

                        if seen_valid_ids.insert(id) {
                            ids_to_delete.push(id);
                        }
                    }
                    Err(error) => problems.push(BatchResourceProblem {
                        request_index: indexes[request_index],
                        error,
                    }),
                }
            }

            if ids_to_delete.is_empty() {
                continue;
            }

            // Registered selector schemas must have a dispatcher.
            let dispatcher = self
                .dispatcher_factory
                .crud_dispatcher_for_trusted_schema(group.schema.as_str())?;
            dispatcher
                .request_deletion(ResourceCrudDispatcherDeletionRequest {
                    account_id: group.account.did.clone(),
                    ids: ids_to_delete.clone(),
                })
                .await?;

            // The controller runtime may complete a deletion before the
            // response
            let being_deleted = self
                .generic_resource_query_service
                .find_resource_handles_by_ids(&group.account.did, &ids_to_delete)
                .await?
                .into_iter()
                .filter(|row| row.deleted_at.is_none())
                .map(|row| ResourceID::new(row.id))
                .collect::<HashSet<_>>();

            successes.extend(deleted_entries.into_iter().map(|(request_index, id)| {
                BatchResourceSuccess {
                    request_index,
                    item: ResourceDeletion {
                        id,
                        phase: if being_deleted.contains(&id) {
                            ResourcePhase::Deleting
                        } else {
                            ResourcePhase::Deleted
                        },
                    },
                }
            }));
        }

        successes.sort_by_key(|success| success.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves one raw type selector against an already-built descriptor list.
///
/// Building that list constructs every registered dispatcher, so callers
/// resolving more than one selector must build it once and reuse it here rather
/// than rebuilding it per selector.
///
/// Accepts anything spelling a type — a raw CLI selector (`vs`), an ODF
/// `TypeRef` (`VariableSet`), or a schema URI — since descriptors match all
/// four forms.
fn resolve_schema_in_descriptors<E>(
    descriptors: &[ResourceTypeDescriptor],
    selector: impl AsRef<str>,
) -> Result<TypeUri, E>
where
    E: From<UnsupportedResourceSelectorError>,
{
    let selector = selector.as_ref();

    descriptors
        .iter()
        .find(|descriptor| descriptor.matches_selector(selector))
        .map(|descriptor| descriptor.schema.clone())
        .ok_or_else(|| UnsupportedResourceSelectorError::NotFound {
            raw_selector: ResourceTypeSelectorRaw::new_unchecked(selector),
        })
        .map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl LocalResourceFacadeImpl {
    fn list_resource_type_descriptors(&self) -> Vec<ResourceTypeDescriptor> {
        let mut seen = HashSet::new();
        let mut descriptors = Vec::new();

        for dispatcher in self
            .dispatcher_factory
            .presentation_dispatchers()
            .expect("Resource presentation dispatcher construction failed")
        {
            let schema = dispatcher.schema();
            let presentation = dispatcher.presentation();

            if seen.insert(schema) {
                descriptors.push(ResourceTypeDescriptor {
                    canonical_selector: presentation.canonical_selector,
                    selector_aliases: presentation.selector_aliases.to_vec(),
                    schema: schema.clone(),
                    list_columns: presentation
                        .list_columns
                        .iter()
                        .copied()
                        .map(Into::into)
                        .collect(),
                });
            }
        }

        descriptors.sort_by(|a, b| a.canonical_selector.cmp(&b.canonical_selector));

        descriptors
    }

    /// Resolves the one schema a ref addresses.
    ///
    /// A ref either names its type or carries an `id`. Addressing by name
    /// *without* a type is refused, because ODF's uniqueness key is
    /// `(account, type, name)` and a bare name may belong to several types at
    /// once (RFC-018 § References). So there is no cross-type name search here:
    /// a ref names exactly one resource, and the caller must supply enough of
    /// the key to say which.
    ///
    /// Enforced here as well as in the GraphQL adapter's `validate_ref`: an
    /// in-process caller never passes through that adapter, and both
    /// implementations must answer identically.
    async fn resolve_ref_schema(
        &self,
        descriptors: &[ResourceTypeDescriptor],
        account_id: &odf::AccountID,
        resource_ref: &ResourceRef,
    ) -> Result<Result<TypeUri, ResourceLookupProblem>, BatchResourceError> {
        if let Some(r#type) = resource_ref.r#type.as_ref() {
            return Ok(Ok(resolve_schema_in_descriptors::<BatchResourceError>(
                descriptors,
                r#type,
            )?));
        }

        // No type given, so the ref must carry an id. An id is globally unique
        // and the stored row already records its schema — one lookup, no scan.
        let Some(id) = resource_ref.id else {
            // Distinguish "named nothing" from "named a resource but no type":
            // reporting the latter as `EmptyRef` would tell a caller who did
            // supply a name that they supplied none.
            return Ok(Err(if resource_ref.name.is_some() {
                ResourceLookupProblem::UntypedName
            } else {
                ResourceLookupProblem::EmptyRef
            }));
        };

        let rows = self
            .generic_resource_query_service
            .find_resource_handles_by_ids(account_id, &[id])
            .await?;

        Ok(match rows.into_iter().next() {
            Some(row) => Ok(TypeUri::new_unchecked(&row.schema)),
            None => Err(id_not_found(id)),
        })
    }

    /// Resolves every ref's account and schema, then splits the batch into
    /// `(account, schema)` groups.
    ///
    /// The shared front half of all three batch-ref pipelines. Accounts are
    /// resolved in one deduplicated pass and schemas against a descriptor list
    /// built once, so a batch costs one account lookup per distinct spelling
    /// and no per-ref catalog work.
    ///
    /// An unresolvable account or an *unknown named* type fails the **whole**
    /// call rather than becoming a per-item problem: both are addressing errors
    /// in the request rather than facts about stored data.
    ///
    /// A **type-less** ref is different: resolving it is a lookup against
    /// stored data, so a miss or an ambiguity is that one ref's problem and is
    /// returned alongside the groups for the caller to merge by request index.
    /// Because all three pipelines share this function, they inherit type-less
    /// refs together rather than one at a time.
    async fn group_refs_by_target(
        &self,
        resource_refs: Vec<ResourceRef>,
        operation: ResourceAccountOperation,
    ) -> Result<
        (
            Vec<(BatchTargetGroup, ResourceAccountAccess)>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let account_refs = distinct_account_refs(&resource_refs);
        let resolved_accounts = self
            .resource_account_resolver
            .resolve_accounts(&self.principal, &account_refs)
            .await?;

        let descriptors = self.list_resource_type_descriptors();

        let mut entries = Vec::new();
        let mut problems = Vec::new();
        // Resolving a type-less ref looks at stored data, so it needs the
        // default rule on its account first
        let mut default_rule_checked = HashSet::new();

        for (request_index, resource_ref) in resource_refs.into_iter().enumerate() {
            let account_position = account_refs
                .iter()
                .position(|account| account == &resource_ref.account)
                .expect("every ref's account is in the deduplicated list");
            let account = resolved_accounts[account_position].clone();

            if resource_ref.r#type.is_none() && default_rule_checked.insert(account.did.clone()) {
                self.resource_account_resolver
                    .ensure_can_use_account(&self.principal, &account)
                    .await?;
            }

            match self
                .resolve_ref_schema(&descriptors, &account.did, &resource_ref)
                .await?
            {
                Ok(schema) => entries.push((request_index, resource_ref, account, schema)),
                Err(error) => problems.push(BatchResourceProblem {
                    request_index,
                    error,
                }),
            }
        }

        let mut groups = Vec::new();
        for group in group_by_account_and_schema(entries) {
            // Registered selector schemas must have a dispatcher.
            let access = self
                .dispatcher_factory
                .crud_dispatcher_for_trusted_schema(group.schema.as_str())?
                .check_account_access(&self.principal.handle()?, &group.account, operation)
                .await
                .map_err(|err| {
                    ResolveManifestAccountError::from_operation_error(&group.account, err)
                })?;
            groups.push((group, access));
        }

        Ok((groups, problems))
    }

    /// Resolves a group's refs to IDs and keeps those the principal may run
    /// `operation` on. The response is indexed within the group; the returned
    /// indexes map it back to the caller's positions.
    async fn resolve_permitted_ids(
        &self,
        account: &odf::ResourceOwnerHandle,
        schema: &TypeUri,
        entries: Vec<(usize, ResourceRef)>,
        access: ResourceAccountAccess,
        operation: ResourceAccountOperation,
        visibility: ResourceVisibility,
    ) -> Result<(Vec<usize>, BatchIdsResolutionResponse), BatchResourceError> {
        let (indexes, refs): (Vec<_>, Vec<_>) = entries.into_iter().unzip();

        let grouped = group_batch_resource_refs(refs);
        let mut resolution_response = resolve_batch_ids(
            self.generic_resource_query_service.as_ref(),
            &account.did,
            schema,
            grouped,
            visibility,
        )
        .await?;
        resolution_response.id_entries = self
            .retain_permitted_entries(
                account,
                schema,
                access,
                operation,
                resolution_response.id_entries,
                &mut resolution_response.problems,
            )
            .await?;

        Ok((indexes, resolution_response))
    }

    /// Keeps the entries whose resources the principal may run `operation`
    /// on, and reports each other one as a problem of its ref. A resource the
    /// principal may not read is not found, so a denial does not reveal it: a
    /// name matching several resources keeps the others silently, and is not
    /// found only when none of them is answered.
    async fn retain_permitted_entries(
        &self,
        account: &odf::ResourceOwnerHandle,
        schema: &TypeUri,
        access: ResourceAccountAccess,
        operation: ResourceAccountOperation,
        id_entries: BatchIdEntries,
        problems: &mut Vec<BatchResourceProblem<ResourceLookupProblem>>,
    ) -> Result<BatchIdEntries, BatchResourceError> {
        match access {
            ResourceAccountAccess::Allowed => return Ok(id_entries),
            ResourceAccountAccess::PerResource => {}
        }

        let ids = id_entries.iter().map(|(_, _, id)| *id).collect::<Vec<_>>();
        // Only resources of the type in the account are checked; any other
        // ID names nothing the principal may see. A deleted resource cannot be
        // deleted again, so its guard is not asked to.
        let checkable = self
            .generic_resource_query_service
            .find_resource_handles_by_ids(&account.did, &ids)
            .await?
            .into_iter()
            .filter(|row| row.schema == schema.as_str())
            .filter(|row| match operation {
                ResourceAccountOperation::Read => true,
                ResourceAccountOperation::Delete => row.deleted_at.is_none(),
            })
            .map(|row| ResourceID::new(row.id))
            .collect::<HashSet<_>>();
        let mut seen_ids = HashSet::with_capacity(ids.len());
        let checked_ids = ids
            .into_iter()
            .filter(|id| checkable.contains(id) && seen_ids.insert(*id))
            .collect::<Vec<_>>();

        let checked = self
            .dispatcher_factory
            .crud_dispatcher_for_trusted_schema(schema.as_str())?
            .check_resource_operations(&self.principal.handle()?, account, operation, &checked_ids)
            .await?;
        let mut results = HashMap::with_capacity(checked_ids.len());
        for (id, result) in checked_ids.into_iter().zip(checked) {
            match result {
                Err(ResourceOperationError::Internal(err)) => {
                    return Err(BatchResourceError::Internal(err));
                }
                result @ (Ok(())
                | Err(
                    ResourceOperationError::NotFound(_) | ResourceOperationError::Access(_),
                )) => {
                    results.insert(id, result);
                }
            }
        }

        // A resource named by several refs gets the same answer for each
        let mut permitted = Vec::with_capacity(id_entries.len());
        let mut answered_indexes = HashSet::new();
        let mut hidden = Vec::new();
        for (request_index, resource_ref, id) in id_entries {
            match results.get(&id) {
                Some(Ok(())) => {
                    answered_indexes.insert(request_index);
                    permitted.push((request_index, resource_ref, id));
                }
                Some(Err(ResourceOperationError::Access(err))) => {
                    answered_indexes.insert(request_index);
                    problems.push(BatchResourceProblem {
                        request_index,
                        error: ResourceLookupProblem::Access(repeat_access_error(err)),
                    });
                }
                Some(Err(
                    ResourceOperationError::NotFound(_) | ResourceOperationError::Internal(_),
                ))
                | None => hidden.push((request_index, resource_ref, id)),
            }
        }

        for (request_index, resource_ref, id) in hidden {
            if answered_indexes.insert(request_index) {
                problems.push(BatchResourceProblem {
                    request_index,
                    error: ref_not_found(schema, &resource_ref, id),
                });
            }
        }

        Ok(permitted)
    }

    /// A selector of a type is checked by the type's guard; one without a
    /// type by the default rule. Each check runs once per call: `checked`
    /// holds those already passed
    async fn ensure_can_list(
        &self,
        account: &odf::ResourceOwnerHandle,
        schema: Option<&TypeUri>,
        scope: &ResourceListScope,
        checked: &mut Vec<ListCheck>,
    ) -> Result<(), ResolveManifestAccountError> {
        let check = (
            account.did.clone(),
            schema.map(|schema| (schema.clone(), scope.clone())),
        );
        if checked.contains(&check) {
            return Ok(());
        }

        match schema {
            None => {
                self.resource_account_resolver
                    .ensure_can_use_account(&self.principal, account)
                    .await?;
            }
            Some(schema) => {
                self.dispatcher_factory
                    .crud_dispatcher_for_trusted_schema(schema.as_str())
                    .int_err()?
                    .check_list(&self.principal.handle()?, account, scope)
                    .await
                    .map_err(|err| {
                        ResolveManifestAccountError::from_operation_error(account, err)
                    })?;
            }
        }

        checked.push(check);
        Ok(())
    }

    /// Converts snapshots into summary views, rendering each one's typed list
    /// columns.
    ///
    /// The results may span several types, so presentation dispatchers are
    /// looked up **once per distinct schema** rather than per row — otherwise a
    /// page of N results would construct N dispatchers.
    fn summary_views_with_columns(
        &self,
        snapshots: Vec<ResourceSnapshot>,
    ) -> Result<Vec<ResourceSummaryView>, InternalError> {
        let mut dispatchers: HashMap<TypeUri, Arc<dyn ResourcePresentationDispatcher>> =
            HashMap::new();

        for dispatcher in self.dispatcher_factory.presentation_dispatchers()? {
            dispatchers
                .entry(dispatcher.schema().clone())
                .or_insert(dispatcher);
        }

        snapshots
            .into_iter()
            .map(|snapshot| {
                // A schema with no presentation dispatcher is not selectable in
                // the first place — the descriptor list this scope was resolved
                // against is built from exactly this registry — so a miss means
                // stored data of a type that is no longer registered. Render it
                // without typed columns rather than failing the whole listing.
                let list_values = match dispatchers.get(&snapshot.schema) {
                    Some(dispatcher) => dispatcher.list_column_values_for_snapshot(&snapshot)?,
                    None => Vec::new(),
                };

                let mut view = ResourceSummaryView::from(snapshot);
                view.list_values = list_values;
                Ok(view)
            })
            .collect()
    }

    /// Resolves a search's account, owner and scope, and counts its matches.
    /// `None` when the scope is vacuous, so nothing can match.
    async fn prepare_search(
        &self,
        request: SearchResourcesRequest,
    ) -> Result<Option<PreparedSearch>, ListResourcesError> {
        // First, as the remote facade does, so a request with several faults
        // fails the same way on both. And before the fast path of
        // `resolve_scope`: a selector narrowed *only* by a field the facade
        // cannot resolve reads as unnarrowed there and would widen into
        // "every resource". The trait is public, so this is the boundary that
        // has to hold — not the GraphQL adapter alone.
        validate_search_request(&request)?;

        // The principal's own account: the default for selectors naming no
        // account, and the check that a principal without one cannot search.
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(&self.principal, None)
            .await?;

        let owner = self.resolve_owner(request.owner).await?;
        let scope = self
            .resolve_scope(request.selectors, owner, request.visibility)
            .await?;
        let owner_id = owner.map(ResourceListOwner::id);

        // Empty exact-name/id queries are vacuous.
        if scope.is_vacuous() {
            return Ok(None);
        }

        let total_count = self
            .generic_resource_query_service
            .count_search_resource_handles(
                &target_account.did,
                &scope,
                request.visibility,
                owner_id,
            )
            .await?;

        Ok(Some(PreparedSearch {
            account_id: target_account.did,
            scope,
            owner_id,
            visibility: request.visibility,
            pagination: request.pagination,
            total_count,
        }))
    }

    /// Resolves the owner that narrows a search. One given by ID need not exist
    /// any more, but if it does, the other parts of the reference must match.
    async fn resolve_owner(
        &self,
        maybe_owner: Option<ResourceRef>,
    ) -> Result<Option<ResourceListOwner>, ListResourcesError> {
        let Some(owner) = maybe_owner else {
            return Ok(None);
        };

        let owner_id = owner.id;
        let response = self
            .get_handles(vec![owner], ResourceVisibility::Active)
            .await?;

        if let Some(owner) = response.successes.into_iter().next() {
            return Ok(Some(ResourceListOwner::Found(owner.item.id)));
        }

        let Some(problem) = response.problems.into_iter().next() else {
            return Err(ListResourcesError::Internal(InternalError::new(
                "Owner lookup gave neither a resource nor a problem",
            )));
        };

        match (problem.error, owner_id) {
            (ResourceLookupProblem::IDNotFound(_), Some(owner_id)) => {
                Ok(Some(ResourceListOwner::NotFound(owner_id)))
            }
            (
                error @ (ResourceLookupProblem::IDNotFound(_)
                | ResourceLookupProblem::NameNotFound(_)
                | ResourceLookupProblem::SchemaMismatch(_)
                | ResourceLookupProblem::NameMismatch(_)
                | ResourceLookupProblem::EmptyRef
                | ResourceLookupProblem::UntypedName
                | ResourceLookupProblem::DeletedNotRevealed(_)
                | ResourceLookupProblem::Access(_)),
                _,
            ) => Err(ListResourcesError::OwnerLookup(error)),
        }
    }

    /// Resolves a request's selectors into a repository scope, each carrying
    /// its own resolved label pairs.
    ///
    /// Each type keeps its own query *and* its own filter through resolution,
    /// so a multi-type scope like `vs/a-% ss/b-%` stays intact and two
    /// selectors filtering by different labels stay apart.
    async fn resolve_scope(
        &self,
        selectors: Vec<ResourceSelector>,
        owner: Option<ResourceListOwner>,
        visibility: ResourceVisibility,
    ) -> Result<ResourceScope, ListResourcesError> {
        // A lone type-less, unnarrowed, account-less, unlabelled selector needs
        // no schemas at all, so it can answer before touching the catalog or
        // resolving any account.
        if let [selector] = selectors.as_slice()
            && selector.r#type.is_none()
            && selector.id.is_none()
            && selector.name.is_none()
            && selector.account.is_none()
            && selector.labels.is_none()
        {
            return Ok(ResourceScope::any_type());
        }

        // Resolved in one batch, deduplicated by spelling. Access is checked
        // per selector below, by the type's guard; any denial fails the whole
        // call.
        //
        // `None` stays `None` rather than resolving to the principal's own
        // account: the repository takes that account as the
        // default for exactly those rows, so resolving here would be redundant
        // work and would lose the "unset" distinction the scope relies on.
        let account_refs = selectors
            .iter()
            .map(|selector| selector.account.clone())
            .collect::<Vec<_>>();
        let accounts = if account_refs.iter().all(Option::is_none) {
            vec![None; account_refs.len()]
        } else {
            self.resource_account_resolver
                .resolve_accounts(&self.principal, &account_refs)
                .await?
                .into_iter()
                .zip(account_refs.iter())
                .map(|(handle, requested)| requested.as_ref().map(|_| handle))
                .collect()
        };

        // Built once: constructing it instantiates every registered dispatcher,
        // so every selector resolves against this one list.
        let descriptors = self.list_resource_type_descriptors();

        // Every registered schema, the fallback a type-less selector resolves
        // its labels against.
        let all_schema_ids = descriptors
            .iter()
            .map(|descriptor| ResourceSchemaId::try_from(&descriptor.schema).int_err())
            .collect::<Result<Vec<_>, _>>()?;

        let mut applicable = Vec::with_capacity(selectors.len());
        // A scope holds label pairs, which cannot be hashed; a request has
        // few selectors
        let mut list_checks = Vec::new();

        for (selector, account) in selectors.into_iter().zip(accounts) {
            let schema = selector
                .r#type
                .as_ref()
                .map(|r#type| {
                    resolve_schema_in_descriptors::<ListResourcesError>(&descriptors, r#type)
                })
                .transpose()?;

            // A typed selector resolves its labels against its own schema; a
            // type-less one against every registered schema.
            let candidate_schema_ids = match &schema {
                Some(schema) => vec![ResourceSchemaId::try_from(schema).int_err()?],
                None => all_schema_ids.clone(),
            };

            // Resolution can drop schemas that cannot carry this selector's
            // labels. With a single candidate that is an error rather than a
            // silent miss — the `n = 1` contract of
            // `resolve_label_filter_for_schemas`.
            let (applicable_schema_ids, resolved_label_filter) = resolve_label_filter_for_schemas(
                &self.resource_extension_schema_resolver,
                selector.labels.clone(),
                &candidate_schema_ids,
            )?;

            // What a type's guard sees of the selector
            let list_scope = |label_pairs| ResourceListScope {
                id: selector.id,
                name_pattern: selector.name.clone(),
                label_pairs,
                owner,
                visibility,
            };

            // This selector's schema cannot carry its labels, so it can never
            // match. Drop it rather than widening the scope, once the caller
            // may list the account at all, judged without the labels.
            if applicable_schema_ids.is_empty() {
                if let Some(account) = &account {
                    self.ensure_can_list(
                        account,
                        schema.as_ref(),
                        &list_scope(Vec::new()),
                        &mut list_checks,
                    )
                    .await?;
                }
                continue;
            }

            // Flattening is what rejects `$not`/`$or`: the repository evaluates
            // a conjunction of pairs, nothing richer.
            let label_pairs =
                ResourceLabelFilterPredicate::flatten_conjunction(&resolved_label_filter)
                    .map_err(|err| {
                        ListResourcesError::InvalidLabelFilter(
                            ResourceInvalidLabelFilterError::unsupported_expression(err),
                        )
                    })?
                    .into_iter()
                    .map(|(key, value)| (key.clone(), value.to_owned()))
                    .collect::<Vec<_>>();

            if let Some(account) = &account {
                self.ensure_can_list(
                    account,
                    schema.as_ref(),
                    &list_scope(label_pairs.clone()),
                    &mut list_checks,
                )
                .await?;
            }

            applicable.push(ResolvedSelector {
                schema,
                id: selector.id,
                // Authored selector names are `LIKE` patterns by definition;
                // an exact name arrives as a `ResourceRef`, not here.
                name: None,
                name_pattern: selector.name,
                account_id: account.map(|account| account.did),
                label_pairs,
            });
        }

        // `None` means nothing can match, which the repository expresses as an
        // empty type list.
        let scope = coalesce_selectors(applicable)?.unwrap_or(ResourceScope::Types(Vec::new()));

        Ok(scope)
    }

    async fn resolve_multiple_resource_views(
        &self,
        resource_refs: Vec<ResourceRef>,
        spec_view: SpecViewOpts,
        visibility: ResourceVisibility,
    ) -> Result<
        (
            Vec<IndexedResource<Resource>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        // An empty batch names nothing, so it yields an empty result.
        let mut indexed_resources = Vec::new();
        let (groups, mut problems) = self
            .group_refs_by_target(resource_refs, ResourceAccountOperation::Read)
            .await?;

        for (group, access) in groups {
            let (indexes, resolution_response) = self
                .resolve_permitted_ids(
                    &group.account,
                    &group.schema,
                    group.entries,
                    access,
                    ResourceAccountOperation::Read,
                    visibility,
                )
                .await?;

            problems.extend(resolution_response.problems.into_iter().map(|problem| {
                BatchResourceProblem {
                    request_index: indexes[problem.request_index],
                    error: problem.error,
                }
            }));

            let ids = resolution_response
                .id_entries
                .iter()
                .map(|(_, _, id)| *id)
                .collect::<Vec<_>>();

            let snapshots_by_id = self
                .generic_resource_query_service
                .find_snapshots_by_ids(&group.account.did, &ids)
                .await?
                .into_iter()
                .map(|snapshot| (snapshot.id, snapshot))
                .collect::<HashMap<_, _>>();

            for (request_index, resource_ref, id) in resolution_response.id_entries {
                // Only a ref that supplied its own id asserts a name to verify:
                // for a name-keyed ref the name *is* how the id was found.
                let expected_name = if resource_ref.id.is_some() {
                    resource_ref.name.clone()
                } else {
                    None
                };

                match snapshots_by_id
                    .get(&id)
                    .filter(|snapshot| {
                        ref_visibility(&resource_ref, visibility).includes(
                            snapshot.headers.deletion_requested_at,
                            snapshot.headers.deleted_at,
                        )
                    })
                    .cloned()
                    .ok_or_else(|| ref_not_found(&group.schema, &resource_ref, id))
                    .and_then(|snapshot| {
                        ensure_schema_matches::<ResourceLookupProblem>(
                            snapshot.id,
                            &group.schema,
                            snapshot.schema.as_str(),
                        )?;
                        // The other half of the `id` + `name` consistency
                        // assertion; without it the id silently wins and the
                        // caller reads a resource they did not name.
                        if let Some(expected_name) = expected_name
                            && expected_name != snapshot.headers.name
                        {
                            return Err(ResourceLookupProblem::NameMismatch(
                                ResourceNameMismatchError {
                                    id: snapshot.id,
                                    expected_name,
                                    actual_name: snapshot.headers.name.clone(),
                                },
                            ));
                        }
                        Ok(snapshot)
                    }) {
                    Ok(mut snapshot) => {
                        snapshot.headers.set_account(group.account.clone());

                        let resource = Resource {
                            schema: snapshot.schema,
                            headers: snapshot.headers,
                            spec: snapshot.spec,
                            status: snapshot.status.unwrap_or_else(new_pending_resource_status),
                        };

                        indexed_resources.push(IndexedResource {
                            request_index: indexes[request_index],
                            item: resource,
                        });
                    }
                    Err(error) => {
                        problems.push(BatchResourceProblem {
                            request_index: indexes[request_index],
                            error,
                        });
                    }
                }
            }
        }

        let mut indexed_resources =
            self.apply_spec_view_mode_batch(indexed_resources, spec_view, &mut problems)?;

        indexed_resources.sort_by_key(|resource| resource.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok((indexed_resources, problems))
    }

    async fn resolve_id_handle_groups(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        id_entries: BatchIdEntries,
        mut problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
        visibility: ResourceVisibility,
    ) -> Result<
        (
            Vec<IndexedResource<ResourceHandle>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let ids = id_entries.iter().map(|(_, _, id)| *id).collect::<Vec<_>>();

        let rows_by_id = self
            .generic_resource_query_service
            .find_resource_handles_by_ids(account_id, &ids)
            .await?
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<HashMap<_, _>>();

        let mut handles = Vec::new();
        for (request_index, resource_ref, id) in id_entries {
            let not_found = || ref_not_found(schema, &resource_ref, id);

            // Only a ref that supplied its own id asserts a name to verify: for
            // a name-keyed ref the name *is* how the id was found.
            let expected_name = if resource_ref.id.is_some() {
                resource_ref.name.as_ref()
            } else {
                None
            };

            let row_result = rows_by_id
                .get(id.as_ref())
                .filter(|row| {
                    ref_visibility(&resource_ref, visibility)
                        .includes(row.deletion_requested_at, row.deleted_at)
                })
                .cloned()
                .ok_or_else(not_found);
            match row_result.and_then(|row| {
                validate_handle_row(
                    row,
                    schema,
                    expected_name,
                    ensure_schema_matches::<ResourceLookupProblem>,
                )
            }) {
                Ok(row) => {
                    let handle = row.into_handle();
                    handles.push(IndexedResource {
                        request_index,
                        item: handle,
                    });
                }
                Err(error) => problems.push(BatchResourceProblem {
                    request_index,
                    error,
                }),
            }
        }

        handles.sort_by_key(|handle| handle.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok((handles, problems))
    }

    /// Reveals the specs when `spec_view` asks for it; a deleted resource with
    /// something to reveal becomes a problem instead.
    fn apply_spec_view_mode_batch(
        &self,
        resources: Vec<IndexedResource<Resource>>,
        spec_view: SpecViewOpts,
        problems: &mut Vec<BatchResourceProblem<ResourceLookupProblem>>,
    ) -> Result<Vec<IndexedResource<Resource>>, InternalError> {
        // A batch can span several schemas, and the dispatcher is schema-specific
        // — a schema may register one or none. Resolving from the first item
        // alone would apply its answer to every other schema, either skipping a
        // transformation the item needs or handing a dispatcher a spec it cannot
        // parse.
        //
        // Cached per schema so a single-schema batch still costs one lookup.
        let mut dispatchers: HashMap<TypeUri, Option<Arc<dyn ResourceSpecViewDispatcher>>> =
            HashMap::new();

        let mut viewed = Vec::with_capacity(resources.len());

        for mut resource in resources {
            let maybe_dispatcher = dispatchers
                .entry(resource.item.schema.clone())
                .or_insert_with(|| {
                    self.try_resolve_spec_view_dispatcher(&resource.item.schema, spec_view)
                });

            if let Some(d) = maybe_dispatcher {
                if resource.item.headers.deleted_at.is_some() {
                    problems.push(BatchResourceProblem {
                        request_index: resource.request_index,
                        error: ResourceDeletedNotRevealedError {
                            id: resource.item.headers.id,
                        }
                        .into(),
                    });
                    continue;
                }

                let spec = std::mem::replace(&mut resource.item.spec, serde_json::Value::Null);
                resource.item.spec = d.reveal_spec(spec)?;
            }

            viewed.push(resource);
        }

        Ok(viewed)
    }

    async fn prepare_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<PreparedApplyManifest, ApplyManifestError> {
        let manifest = parse_manifest(request.format, &request.manifest)?;

        // Access is checked where the resource is placed, by the type's guard
        let target_account = self
            .resource_account_resolver
            .resolve_account(&self.principal, manifest.headers.account.as_ref())
            .await?;
        let account_source = if manifest.headers.account.is_some() {
            ApplyResourceAccountSource::Manifest
        } else {
            ApplyResourceAccountSource::PrincipalDefault
        };

        let dispatcher = self
            .dispatcher_factory
            .crud_dispatcher(manifest.schema.as_str())?;

        let canonical_labels = self
            .resource_extension_schema_resolver
            .canonicalize_entries(
                ResourceExtensionKind::Label,
                manifest.headers.labels.clone(),
                &manifest.schema,
            )?;
        let canonical_annotations = self
            .resource_extension_schema_resolver
            .canonicalize_entries(
                ResourceExtensionKind::Annotation,
                manifest.headers.annotations.clone(),
                &manifest.schema,
            )?;

        let mut header_warnings = collect_manifest_header_warnings(&canonical_annotations.entries);
        header_warnings.extend(collect_non_indexable_label_warnings(
            &canonical_labels.entries,
        ));
        header_warnings.extend(canonical_labels.warnings);
        header_warnings.extend(canonical_annotations.warnings);

        let headers = make_apply_headers(
            manifest.headers.name.as_deref(),
            &target_account,
            canonical_labels.entries,
            canonical_annotations.entries,
            self.canonicalize_owner_references(manifest.headers.owner_references)?,
        )?;

        Ok(PreparedApplyManifest {
            provenance: request_creation_provenance(&self.principal, &self.request_origin)?,
            dispatcher,
            id: manifest.headers.id,
            header: headers,
            account_source,
            header_warnings,
            target_account,
            spec: manifest.spec,
        })
    }

    /// Replaces the type names by schemas. An ID finds the owner without its
    /// type, so there the type may remain a name, even an unknown one.
    fn canonicalize_owner_references(
        &self,
        owner_references: Vec<ResourceRef>,
    ) -> Result<Vec<ResourceRef>, ResourceHeadersValidationError> {
        if owner_references.is_empty() {
            return Ok(owner_references);
        }

        let descriptors = self.list_resource_type_descriptors();

        owner_references
            .into_iter()
            .map(|mut owner_reference| {
                if let Some(r#type) = owner_reference.r#type.take() {
                    owner_reference.r#type = Some(
                        match resolve_schema_in_descriptors::<UnsupportedResourceSelectorError>(
                            &descriptors,
                            &r#type,
                        ) {
                            Ok(schema) => schema.into(),
                            Err(_) if owner_reference.id.is_some() => r#type,
                            Err(_) => {
                                return Err(InvalidOwnerReferenceError::UnknownType(r#type).into());
                            }
                        },
                    );
                }
                Ok(owner_reference)
            })
            .collect()
    }

    fn try_resolve_spec_view_dispatcher(
        &self,
        schema: &TypeUri,
        spec_view: SpecViewOpts,
    ) -> Option<Arc<dyn ResourceSpecViewDispatcher>> {
        if spec_view.revealed {
            self.dispatcher_factory.spec_view_dispatcher(schema)
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PreparedApplyManifest {
    dispatcher: Arc<dyn ResourceCrudDispatcher>,
    id: Option<ResourceID>,
    header: ApplyResourceHeaders,
    account_source: ApplyResourceAccountSource,
    header_warnings: Vec<ResourceWarning>,
    target_account: odf::ResourceOwnerHandle,
    spec: serde_json::Value,
    provenance: CreationProvenance,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PreparedSearch {
    account_id: odf::AccountID,
    scope: ResourceScope,
    owner_id: Option<ResourceID>,
    visibility: ResourceVisibility,
    pagination: PaginationOpts,
    total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A list check that passed: the account, and the type with the selector's
/// scope, or none for the default rule
type ListCheck = (odf::AccountID, Option<(TypeUri, ResourceListScope)>);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IndexedResource<T> {
    request_index: usize,
    item: T,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The resolved handle names the account as it is now; a resource placed
/// elsewhere keeps the handle placement gave
fn refresh_target_account(headers: &mut ResourceHeaders, target_account: odf::ResourceOwnerHandle) {
    if headers.account.did == target_account.did {
        headers.set_account(target_account);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The same denial for another ref to the resource; an `AccessError` carries a
/// boxed source, so it cannot be cloned
fn repeat_access_error(err: &odf::AccessError) -> odf::AccessError {
    use odf::AccessError as E;
    match err {
        E::ReadOnly(source) => E::ReadOnly(source.as_ref().map(|source| source.to_string().into())),
        E::Unauthenticated(source) => E::Unauthenticated(source.to_string().into()),
        E::Unauthorized(source) => E::Unauthorized(source.to_string().into()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
