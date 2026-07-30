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

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::*;
use kamu_resources_services::{
    ResourceExtensionSchemaResolver,
    get_resource_crud_dispatcher,
    get_resource_crud_dispatcher_by_raw_selector,
    get_resource_crud_dispatcher_for_trusted_schema,
};

use super::helpers::*;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceFacade)]
pub struct LocalResourceFacadeImpl {
    catalog: dill::Catalog,
    resource_account_resolver: Arc<dyn ResourceAccountResolver>,
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
            .resolve_target_account(request.account.as_ref())
            .await?;

        let resource_counts = self
            .generic_resource_query_service
            .summarize_resources(target_account.did)
            .await?
            .into_iter()
            .map(|row| {
                let schema = TypeUri::new_unchecked(row.schema);
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
        selector: ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<Resource, GetResourceError> {
        let mut view = self
            .resolve_resource_view::<GetResourceError>(selector)
            .await?;

        self.apply_spec_view_mode::<GetResourceError>(&mut view, spec_view_mode)?;

        Ok(view)
    }

    async fn get_many(
        &self,
        selector: ResourceBatchSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<BatchResourceResponse<Resource, ResourceLookupProblem>, BatchResourceError> {
        let (mut indexed_resources, problems) =
            self.resolve_multiple_resource_views(selector).await?;

        self.apply_spec_view_mode_batch::<BatchResourceError>(
            &mut indexed_resources,
            spec_view_mode,
        )?;

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

    async fn get_handle(
        &self,
        selector: ResourceSelector,
    ) -> Result<ResourceHandle, GetResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let schema =
            self.resolve_schema_for_selector::<GetResourceError>(&selector.resource_type)?;

        let id = resolve_resource_id::<GetResourceError>(
            self.generic_resource_query_service.as_ref(),
            &schema,
            &target_account.did,
            &selector.resource_ref,
        )
        .await?;

        let snapshot = self
            .resolve_snapshot_for_schema::<GetResourceError>(&schema, &target_account.did, id)
            .await?;

        Ok(resource_handle_from_snapshot(snapshot))
    }

    async fn get_handles(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<ResourceHandle, ResourceLookupProblem>, BatchResourceError>
    {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let schema =
            self.resolve_schema_for_selector::<BatchResourceError>(&selector.resource_type)?;

        let resource_schema = ResourceSchemaId::try_from(&schema).int_err()?;
        let label_filter = resolve_label_filter(
            &self.resource_extension_schema_resolver,
            selector.label_filter.clone(),
            &resource_schema,
        )?;

        let groups = group_batch_resource_refs(selector);
        let resolution_response = resolve_batch_ids(
            self.generic_resource_query_service.as_ref(),
            &target_account.did,
            &schema,
            groups,
        )
        .await?;

        let (handles, problems) = self
            .resolve_id_handle_groups(
                &target_account.did,
                &schema,
                resolution_response.id_entries,
                resolution_response.problems,
                &label_filter,
            )
            .await?;

        Ok(BatchResourceResponse {
            successes: handles
                .into_iter()
                .map(|handle| BatchResourceSuccess {
                    request_index: handle.request_index,
                    item: handle.item,
                })
                .collect(),
            problems,
        })
    }

    async fn render_manifest(
        &self,
        selector: ResourceSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let mut view = self
            .resolve_resource_view::<RenderResourceManifestError>(selector)
            .await?;

        self.apply_spec_view_mode::<RenderResourceManifestError>(&mut view, spec_view_mode)?;

        let manifest = resource_to_manifest(view).map_err(RenderResourceManifestError::Internal)?;
        let manifest =
            serialize_manifest(&manifest, format).map_err(RenderResourceManifestError::Internal)?;

        Ok(RenderResourceManifestResult { manifest, format })
    }

    async fn render_manifests(
        &self,
        selector: ResourceBatchSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        let (mut indexed_resources, problems) =
            self.resolve_multiple_resource_views(selector).await?;

        self.apply_spec_view_mode_batch::<BatchResourceError>(
            &mut indexed_resources,
            spec_view_mode,
        )?;

        let successes = indexed_resources
            .into_iter()
            .map(|resource| {
                let manifest =
                    resource_to_manifest(resource.item).map_err(BatchResourceError::Internal)?;
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

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher_by_raw_selector::<ListResourcesError>(
            &self.catalog,
            &request.raw_type_selector,
        )?;

        let resource_schema = ResourceSchemaId::try_from(dispatcher.schema()).int_err()?;

        let label_filter = resolve_label_filter(
            &self.resource_extension_schema_resolver,
            request.label_filter,
            &resource_schema,
        )?;

        dispatcher
            .list(ResourceCrudDispatcherListRequest {
                account_id: target_account.did,
                pagination: request.pagination,
                label_filter,
            })
            .await
            .map_err(Into::into)
    }

    async fn list_handles(
        &self,
        request: ListResourceHandlesRequest,
    ) -> Result<Vec<ResourceHandle>, ListResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let schema =
            self.resolve_schema_for_selector::<ListResourcesError>(&request.raw_type_selector)?;

        let resource_schema = ResourceSchemaId::try_from(&schema).int_err()?;

        let label_filter = resolve_label_filter(
            &self.resource_extension_schema_resolver,
            request.label_filter,
            &resource_schema,
        )?;

        let snapshots = self
            .generic_resource_query_service
            .list_snapshots_by_schema(
                target_account.did,
                &schema,
                &label_filter,
                request.pagination,
            )
            .await?;

        Ok(map_snapshots_to_handles(snapshots))
    }

    async fn search_handles(
        &self,
        request: SearchResourceHandlesRequest,
    ) -> Result<SearchResourceHandlesResponse, ListResourcesError> {
        if request.exact_names.is_none() && request.name_pattern.is_none() {
            return Err(InvalidResourceSearchQueryError.into());
        }

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let schemas = request
            .raw_type_selectors
            .iter()
            .map(|resource_type| {
                self.resolve_schema_for_selector::<ListResourcesError>(resource_type)
            })
            .collect::<Result<Vec<_>, _>>()?;

        let resource_schema_ids = schemas
            .iter()
            .map(|schema| ResourceSchemaId::try_from(schema).int_err())
            .collect::<Result<Vec<_>, _>>()?;

        // Resolution can narrow the candidate types: a schema that rejects the
        // label is dropped rather than failing the whole query.
        let (applicable_schema_ids, label_filter) = resolve_label_filter_for_schemas(
            &self.resource_extension_schema_resolver,
            request.label_filter,
            &resource_schema_ids,
        )?;

        let schemas = schemas
            .into_iter()
            .zip(&resource_schema_ids)
            .filter(|(_, schema_id)| applicable_schema_ids.contains(schema_id))
            .map(|(schema, _)| schema)
            .collect::<Vec<_>>();

        let rows = self
            .generic_resource_query_service
            .search_resource_handles(
                &target_account.did,
                &schemas,
                request.exact_names.as_deref(),
                request.name_pattern.as_deref(),
                &label_filter,
                request.pagination,
            )
            .await?;
        let total_count = self
            .generic_resource_query_service
            .count_search_resource_handles(
                &target_account.did,
                &schemas,
                request.exact_names.as_deref(),
                request.name_pattern.as_deref(),
                &label_filter,
            )
            .await?;

        let items = rows
            .into_iter()
            .map(resource_handle_from_row)
            .collect::<Vec<_>>();

        Ok(SearchResourceHandlesResponse { items, total_count })
    }

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListAllResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let label_filter = self.resolve_label_filter_for_all_schemas(request.label_filter)?;

        let snapshots = self
            .generic_resource_query_service
            .list_all_snapshots(target_account.did, &label_filter, request.pagination)
            .await?;

        Ok(snapshots.into_iter().map(Into::into).collect())
    }

    async fn list_all_handles(
        &self,
        request: ListAllResourceHandlesRequest,
    ) -> Result<Vec<ResourceHandle>, ListAllResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let label_filter = self.resolve_label_filter_for_all_schemas(request.label_filter)?;

        let snapshots = self
            .generic_resource_query_service
            .list_all_snapshots(target_account.did, &label_filter, request.pagination)
            .await?;

        Ok(map_snapshots_to_handles(snapshots))
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
                spec: prepared.spec,
            })
            .await?;

        Ok(match plan {
            ApplyManifestPlanningDecision::Planned(mut plan) => {
                plan.warnings.splice(0..0, prepared.header_warnings);
                plan.resource.headers.account = prepared.target_account;

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
                spec: prepared.spec,
            })
            .await?;

        Ok(match result {
            ApplyManifestApplicationDecision::Applied(mut result) => {
                result.warnings.splice(0..0, prepared.header_warnings);
                result.resource.headers.account = prepared.target_account;

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

    async fn delete_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<ResourceID, ResourceLookupProblem>, BatchResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let schema =
            self.resolve_schema_for_selector::<BatchResourceError>(&selector.resource_type)?;

        let resource_schema = ResourceSchemaId::try_from(&schema).int_err()?;
        let label_filter = resolve_label_filter(
            &self.resource_extension_schema_resolver,
            selector.label_filter.clone(),
            &resource_schema,
        )?;

        let groups = group_batch_resource_refs(selector);
        let resolution_response = resolve_batch_ids(
            self.generic_resource_query_service.as_ref(),
            &target_account.did,
            &schema,
            groups,
        )
        .await?;

        let mut problems = resolution_response.problems;
        let mut successes = Vec::new();
        let mut seen_valid_ids = HashSet::new();
        let mut ids_to_delete = Vec::<ResourceID>::new();

        let ids = resolution_response
            .id_entries
            .iter()
            .map(|(_, _, id)| *id)
            .collect::<Vec<_>>();

        let rows_by_id = self
            .generic_resource_query_service
            .find_resource_handles_by_ids(&target_account.did, &ids, &label_filter)
            .await?
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<HashMap<_, _>>();

        for (request_index, _, id) in resolution_response.id_entries {
            let row_result = rows_by_id
                .get(id.as_ref())
                .cloned()
                .ok_or_else(|| id_not_found(id));

            match row_result.and_then(|row| {
                validate_handle_row(row, &schema, ensure_schema_matches::<ResourceLookupProblem>)
            }) {
                Ok(_) => {
                    successes.push(BatchResourceSuccess {
                        request_index,
                        item: id,
                    });

                    if seen_valid_ids.insert(id) {
                        ids_to_delete.push(id);
                    }
                }
                Err(error) => problems.push(BatchResourceProblem {
                    request_index,
                    error,
                }),
            }
        }

        if !ids_to_delete.is_empty() {
            // `schema` was resolved from a registered selector above, so a
            // missing dispatcher is a data-integrity catastrophe, not a user
            // error.
            let dispatcher =
                get_resource_crud_dispatcher_for_trusted_schema(&self.catalog, schema.as_str())?;
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: target_account.did.clone(),
                    ids: ids_to_delete,
                })
                .await?;
        }

        successes.sort_by_key(|success| success.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn delete(&self, selector: ResourceSelector) -> Result<ResourceID, DeleteResourceError> {
        let response = self
            .delete_many(ResourceBatchSelector {
                account: selector.account,
                resource_type: selector.resource_type,
                resource_refs: vec![selector.resource_ref],
                label_filter: None,
            })
            .await?;

        if let Some(success) = response.successes.into_iter().next() {
            Ok(success.item)
        } else if let Some(problem) = response.problems.into_iter().next() {
            Err(problem.error.into())
        } else {
            Err(InternalError::new("Delete response did not contain an item").into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl LocalResourceFacadeImpl {
    /// Resolves an authored filter for the `all` scope.
    ///
    /// `all` spans every registered schema, so the filter is resolved against
    /// all of them at once — the same uniformity guarantee `search_handles`
    /// relies on. Schemas that reject the filter are simply not returned,
    /// which is harmless here: the repository applies one predicate across all
    /// schemas, and a resource whose schema rejected the key cannot carry it.
    fn resolve_label_filter_for_all_schemas(
        &self,
        label_filter: Option<ResourceLabelFilterInput>,
    ) -> Result<ResolvedResourceLabelFilter, ResourceInvalidLabelFilterError> {
        let resource_schema_ids = self
            .list_resource_type_descriptors()
            .iter()
            .map(|descriptor| ResourceSchemaId::try_from(&descriptor.schema))
            .collect::<Result<Vec<_>, _>>()
            .expect("registered descriptors always carry canonical schema URIs");

        let (_, resolved) = resolve_label_filter_for_schemas(
            &self.resource_extension_schema_resolver,
            label_filter,
            &resource_schema_ids,
        )?;

        Ok(resolved)
    }
    fn list_resource_type_descriptors(&self) -> Vec<ResourceTypeDescriptor> {
        let mut seen = HashSet::new();
        let mut descriptors = Vec::new();

        for builder in self
            .catalog
            .builders_for::<dyn ResourcePresentationDispatcher>()
        {
            let dispatcher = builder
                .get(&self.catalog)
                .expect("Resource presentation dispatcher construction failed");

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

        descriptors.sort_by(|a, b| a.schema.cmp(&b.schema));

        descriptors
    }

    fn resolve_schema_for_selector<E>(
        &self,
        selector: &ResourceTypeSelectorRaw,
    ) -> Result<TypeUri, E>
    where
        E: From<UnsupportedResourceSelectorError>,
    {
        self.list_resource_type_descriptors()
            .into_iter()
            .find(|descriptor| descriptor.matches_selector(selector))
            .map(|descriptor| descriptor.schema)
            .ok_or_else(|| UnsupportedResourceSelectorError::NotFound {
                raw_selector: selector.clone(),
            })
            .map_err(Into::into)
    }

    async fn resolve_resource_view<E>(&self, selector: ResourceSelector) -> Result<Resource, E>
    where
        E: From<ResolveManifestAccountError>
            + From<ResourceLookupProblem>
            + From<UnsupportedResourceSelectorError>
            + From<InternalError>
            + From<GetResourceCrudDispatcherError>,
    {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let schema = self.resolve_schema_for_selector::<E>(&selector.resource_type)?;

        let id = resolve_resource_id::<E>(
            self.generic_resource_query_service.as_ref(),
            &schema,
            &target_account.did,
            &selector.resource_ref,
        )
        .await?;

        let snapshot = self
            .resolve_snapshot_for_schema::<E>(&schema, &target_account.did, id)
            .await?;

        // The schema was resolved from a registered selector above, so a missing
        // dispatcher here is a data-integrity catastrophe, not a user error.
        let dispatcher = get_resource_crud_dispatcher_for_trusted_schema(
            &self.catalog,
            snapshot.schema.as_str(),
        )?;

        let view = dispatcher
            .get(ResourceCrudDispatcherGetRequest {
                account_id: target_account.did.clone(),
                id,
            })
            .await?;

        Ok(Resource {
            headers: kamu_resources::ResourceHeaders {
                account: target_account,
                ..view.headers
            },
            ..view
        })
    }

    async fn resolve_multiple_resource_views(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        (
            Vec<IndexedResource<Resource>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let schema =
            self.resolve_schema_for_selector::<BatchResourceError>(&selector.resource_type)?;

        // Batch selectors name their targets explicitly, so callers narrow by
        // labels through `get_handles` before reaching this point.
        let groups = group_batch_resource_refs(selector);

        let resolution_response = resolve_batch_ids(
            self.generic_resource_query_service.as_ref(),
            &target_account.did,
            &schema,
            groups,
        )
        .await?;

        let mut indexed_resources = Vec::new();
        let mut problems = resolution_response.problems;

        let ids = resolution_response
            .id_entries
            .iter()
            .map(|(_, _, id)| *id)
            .collect::<Vec<_>>();

        let snapshots_by_id = self
            .generic_resource_query_service
            .find_snapshots_by_ids(&target_account.did, &ids)
            .await?
            .into_iter()
            .map(|snapshot| (snapshot.id, snapshot))
            .collect::<HashMap<_, _>>();

        for (request_index, _, id) in resolution_response.id_entries {
            match snapshots_by_id
                .get(&id)
                .cloned()
                .ok_or(ResourceLookupProblem::IDNotFound(ResourceIDNotFoundError(
                    id,
                )))
                .and_then(|snapshot| {
                    ensure_schema_matches::<ResourceLookupProblem>(
                        snapshot.id,
                        &schema,
                        snapshot.schema.as_str(),
                    )?;
                    Ok(snapshot)
                }) {
                Ok(snapshot) => {
                    let resource = Resource {
                        schema: snapshot.schema,
                        headers: kamu_resources::ResourceHeaders {
                            account: target_account.clone(),
                            ..snapshot.headers
                        },
                        spec: snapshot.spec,
                        status: snapshot.status.unwrap_or_else(new_pending_resource_status),
                    };

                    indexed_resources.push(IndexedResource {
                        request_index,
                        item: resource,
                    });
                }
                Err(error) => {
                    problems.push(BatchResourceProblem {
                        request_index,
                        error,
                    });
                }
            }
        }

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
        label_filter: &ResolvedResourceLabelFilter,
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
            .find_resource_handles_by_ids(account_id, &ids, label_filter)
            .await?
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<HashMap<_, _>>();

        let mut handles = Vec::new();
        for (request_index, resource_ref, id) in id_entries {
            // A row can be absent because the id does not exist or because the
            // label filter excluded it. Either way, report the miss in the
            // terms the caller selected by, so a filtered-out `vs/my-vars`
            // still names `my-vars` rather than its resolved id.
            let not_found = || match &resource_ref {
                ResourceRef::ByName(name) => resource_type_name(schema)
                    .map(|type_name| {
                        ResourceLookupProblem::NameNotFound(ResourceNameNotFoundError {
                            type_name,
                            name: name.clone(),
                        })
                    })
                    .unwrap_or_else(|_| id_not_found(id)),
                ResourceRef::ById(_) => id_not_found(id),
            };

            let row_result = rows_by_id.get(id.as_ref()).cloned().ok_or_else(not_found);
            match row_result.and_then(|row| {
                validate_handle_row(row, schema, ensure_schema_matches::<ResourceLookupProblem>)
            }) {
                Ok(row) => {
                    let handle = resource_handle_from_row(row);
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

    async fn resolve_snapshot_for_schema<E>(
        &self,
        schema: &TypeUri,
        account_id: &odf::AccountID,
        id: ResourceID,
    ) -> Result<ResourceSnapshot, E>
    where
        E: From<InternalError> + From<ResourceLookupProblem>,
    {
        let Some(snapshot) = self.find_account_snapshot(account_id, id).await? else {
            return Err(ResourceLookupProblem::IDNotFound(ResourceIDNotFoundError(id)).into());
        };

        ensure_schema_matches::<E>(id, schema, snapshot.schema.as_str())?;

        Ok(snapshot)
    }

    async fn find_account_snapshot(
        &self,
        account_id: &odf::AccountID,
        id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let Some(snapshot) = self
            .generic_resource_query_service
            .get_snapshot_by_id(&id)
            .await?
        else {
            return Ok(None);
        };

        if snapshot.headers.account.did != *account_id {
            return Ok(None);
        }

        Ok(Some(snapshot))
    }

    async fn ensure_manifest_id_is_accessible(
        &self,
        schema: &TypeUri,
        account_id: &odf::AccountID,
        maybe_id: Option<ResourceID>,
    ) -> Result<(), ApplyManifestError> {
        let Some(id) = maybe_id else {
            return Ok(());
        };

        let Some(snapshot) = self.find_account_snapshot(account_id, id).await? else {
            return Err(ResourceIDNotFoundError(id).into());
        };

        if snapshot.schema != *schema {
            return Err(ResourceTypeMismatchError::new(id, schema.clone(), snapshot.schema).into());
        }

        Ok(())
    }

    fn apply_spec_view_mode<E>(
        &self,
        view: &mut Resource,
        spec_view_mode: SpecViewMode,
    ) -> Result<(), E>
    where
        E: From<InternalError>,
    {
        if let Some(d) = self.try_resolve_spec_view_dispatcher(&view.schema, spec_view_mode) {
            let spec = std::mem::replace(&mut view.spec, serde_json::Value::Null);
            view.spec = d.reveal_spec(spec).map_err(E::from)?;
        }
        Ok(())
    }

    fn apply_spec_view_mode_batch<E>(
        &self,
        resources: &mut [IndexedResource<Resource>],
        spec_view_mode: SpecViewMode,
    ) -> Result<(), E>
    where
        E: From<InternalError>,
    {
        // Dispatcher is resolved once from the first item; all items in a batch share
        // the same schema, so this avoids redundant catalog lookups.
        let maybe_dispatcher = resources
            .first()
            .and_then(|r| self.try_resolve_spec_view_dispatcher(&r.item.schema, spec_view_mode));

        if let Some(d) = maybe_dispatcher {
            for resource in resources.iter_mut() {
                let spec = std::mem::replace(&mut resource.item.spec, serde_json::Value::Null);
                resource.item.spec = d.reveal_spec(spec).map_err(E::from)?;
            }
        }

        Ok(())
    }

    async fn prepare_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<PreparedApplyManifest, ApplyManifestError> {
        let manifest = parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.headers.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            manifest.schema.as_str(),
        )?;

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

        let headers = make_headers_input(
            manifest.headers.name.as_str(),
            &target_account,
            canonical_labels.entries,
            canonical_annotations.entries,
        )?;

        self.ensure_manifest_id_is_accessible(
            manifest.schema.typ(),
            &target_account.did,
            manifest.headers.id,
        )
        .await?;

        Ok(PreparedApplyManifest {
            dispatcher,
            id: manifest.headers.id,
            header: headers,
            header_warnings,
            target_account,
            spec: manifest.spec,
        })
    }

    fn try_resolve_spec_view_dispatcher(
        &self,
        schema: &TypeUri,
        spec_view_mode: SpecViewMode,
    ) -> Option<Arc<dyn ResourceSpecViewDispatcher>> {
        if spec_view_mode == SpecViewMode::Revealed {
            get_resource_spec_view_dispatcher_from_catalog(&self.catalog, schema)
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PreparedApplyManifest {
    dispatcher: Arc<dyn ResourceCrudDispatcher>,
    id: Option<ResourceID>,
    header: ResourceHeadersInput,
    header_warnings: Vec<ResourceWarning>,
    target_account: odf::AccountHandle,
    spec: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IndexedResource<T> {
    request_index: usize,
    item: T,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
