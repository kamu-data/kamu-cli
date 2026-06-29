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

use internal_error::InternalError;
use kamu_resources::*;
use kamu_resources_services::{
    get_resource_crud_dispatcher,
    get_resource_crud_dispatcher_by_selector,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for LocalResourceFacadeImpl {
    async fn list_supported_kinds(
        &self,
    ) -> Result<Vec<ResourceKindDescriptor>, ListSupportedResourceKindsError> {
        Ok(self.list_resource_kind_descriptors())
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let names_by_schema = self.resource_kind_names_by_schema();

        let resource_counts = self
            .generic_resource_query_service
            .summarize_resources(target_account.id)
            .await?
            .into_iter()
            .map(|row| {
                let name = names_by_schema
                    .get(&row.schema)
                    .ok_or_else(|| {
                        ResourcesSummaryError::Internal(InternalError::new(format!(
                            "No resource descriptor registered for {}",
                            row.schema
                        )))
                    })?
                    .clone();

                Ok(ResourceTypeCountSummary {
                    schema: row.schema,
                    name,
                    total_count: row.total_count,
                    phase_counts: row.phase_counts,
                })
            })
            .collect::<Result<Vec<_>, ResourcesSummaryError>>()?;

        Ok(ResourcesSummary { resource_counts })
    }

    async fn get(
        &self,
        selector: ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<ResourceView, GetResourceError> {
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
    ) -> Result<BatchResourceResponse<ResourceView, ResourceLookupProblem>, BatchResourceError>
    {
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

    async fn get_identity(
        &self,
        selector: ResourceSelector,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;
        let schema = self.resolve_schema_for_selector::<GetResourceError>(&selector.kind)?;

        let id = resolve_resource_id::<GetResourceError>(
            self.generic_resource_query_service.as_ref(),
            &schema,
            &target_account.id,
            &selector.resource_ref,
        )
        .await?;

        let snapshot = self
            .resolve_snapshot_for_schema::<GetResourceError>(&schema, &target_account.id, id)
            .await?;

        let names_by_schema = self.resource_kind_names_by_schema();
        resource_identity_from_snapshot::<GetResourceError>(snapshot, &names_by_schema)
    }

    async fn get_identities(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        BatchResourceResponse<ResourceIdentityView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let names_by_schema = self.resource_kind_names_by_schema();
        let schema = self.resolve_schema_for_selector::<BatchResourceError>(&selector.kind)?;

        let groups = group_batch_resource_refs(selector);
        let resolution_response = resolve_batch_ids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
            &schema,
            groups,
        )
        .await?;

        let (identities, problems) = self
            .resolve_id_identity_groups(
                &target_account.id,
                &schema,
                resolution_response.id_entries,
                resolution_response.problems,
                &names_by_schema,
            )
            .await?;

        Ok(BatchResourceResponse {
            successes: identities
                .into_iter()
                .map(|identity| BatchResourceSuccess {
                    request_index: identity.request_index,
                    item: identity.item,
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

        let manifest =
            resource_view_to_manifest(view).map_err(RenderResourceManifestError::Internal)?;
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
                let manifest = resource_view_to_manifest(resource.item)
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

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher_by_selector::<ListResourcesError>(
            &self.catalog,
            &request.kind,
        )?;

        dispatcher
            .list(ResourceCrudDispatcherListRequest {
                account_id: target_account.id,
                pagination: request.pagination,
            })
            .await
            .map_err(Into::into)
    }

    async fn list_identities(
        &self,
        request: ListResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let schema = self.resolve_schema_for_selector::<ListResourcesError>(&request.kind)?;

        let snapshots = self
            .generic_resource_query_service
            .list_snapshots_by_schema(target_account.id, &schema, request.pagination)
            .await?;

        let names_by_schema = self.resource_kind_names_by_schema();
        map_snapshots_to_identities(snapshots, &names_by_schema).map_err(Into::into)
    }

    async fn search_identities(
        &self,
        request: SearchResourceIdentitiesRequest,
    ) -> Result<SearchResourceIdentitiesResponse, ListResourcesError> {
        if request.exact_names.is_none() && request.name_pattern.is_none() {
            return Err(InvalidResourceSearchQueryError.into());
        }

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let schemas = request
            .kinds
            .iter()
            .map(|kind| self.resolve_schema_for_selector::<ListResourcesError>(kind))
            .collect::<Result<Vec<_>, _>>()?;

        let rows = self
            .generic_resource_query_service
            .search_resource_identities(
                &target_account.id,
                &schemas,
                request.exact_names.as_deref(),
                request.name_pattern.as_deref(),
                request.pagination,
            )
            .await?;
        let total_count = self
            .generic_resource_query_service
            .count_search_resource_identities(
                &target_account.id,
                &schemas,
                request.exact_names.as_deref(),
                request.name_pattern.as_deref(),
            )
            .await?;

        let names_by_schema = self.resource_kind_names_by_schema();
        let items = rows
            .into_iter()
            .map(|row| resource_identity_from_row::<ListResourcesError>(row, &names_by_schema))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(SearchResourceIdentitiesResponse { items, total_count })
    }

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<ResourceSummaryView>, ListAllResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let snapshots = self
            .generic_resource_query_service
            .list_all_snapshots(target_account.id, request.pagination)
            .await?;

        Ok(snapshots.into_iter().map(Into::into).collect())
    }

    async fn list_all_identities(
        &self,
        request: ListAllResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListAllResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let snapshots = self
            .generic_resource_query_service
            .list_all_snapshots(target_account.id, request.pagination)
            .await?;

        let names_by_schema = self.resource_kind_names_by_schema();
        map_snapshots_to_identities(snapshots, &names_by_schema).map_err(Into::into)
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
                plan.resource.account = ResourceViewAccount {
                    id: prepared.target_account.id,
                    name: Some(prepared.target_account.name),
                };

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
                result.resource.account = ResourceViewAccount {
                    id: prepared.target_account.id,
                    name: Some(prepared.target_account.name),
                };

                ApplyManifestApplicationDecision::Applied(result)
            }
            ApplyManifestApplicationDecision::Rejected(rejection) => {
                ApplyManifestApplicationDecision::Rejected(rejection)
            }
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

        let schema = self.resolve_schema_for_selector::<BatchResourceError>(&selector.kind)?;

        let groups = group_batch_resource_refs(selector);
        let resolution_response = resolve_batch_ids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
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
            .find_resource_identities_by_ids(&target_account.id, &ids)
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
                validate_identity_row(row, &schema, ensure_schema_matches::<ResourceLookupProblem>)
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
            let dispatcher =
                get_resource_crud_dispatcher::<BatchResourceError>(&self.catalog, &schema)?;
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: target_account.id.clone(),
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
                kind: selector.kind,
                resource_refs: vec![selector.resource_ref],
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
    fn list_resource_kind_descriptors(&self) -> Vec<ResourceKindDescriptor> {
        let mut seen = HashSet::new();
        let mut descriptors = Vec::new();

        for builder in self
            .catalog
            .builders_for::<dyn ResourcePresentationDispatcher>()
        {
            let dispatcher = builder
                .get(&self.catalog)
                .expect("Resource presentation dispatcher construction failed");

            let descriptor = dispatcher.descriptor();
            let presentation = dispatcher.presentation();

            if seen.insert(descriptor.schema) {
                descriptors.push(ResourceKindDescriptor {
                    name: presentation.resource_name.to_string(),
                    short_names: presentation
                        .resource_short_names
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                    schema: descriptor.schema.to_string(),
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

    fn resource_kind_names_by_schema(&self) -> HashMap<String, String> {
        self.list_resource_kind_descriptors()
            .into_iter()
            .map(|descriptor| (descriptor.schema, descriptor.name))
            .collect()
    }

    fn resolve_schema_for_selector<E>(&self, selector: &str) -> Result<String, E>
    where
        E: From<UnsupportedResourceDescriptorError>,
    {
        self.list_resource_kind_descriptors()
            .into_iter()
            .find(|descriptor| descriptor.matches_selector(selector))
            .map(|descriptor| descriptor.schema)
            .ok_or_else(|| UnsupportedResourceDescriptorError::SelectorNotFound {
                selector: selector.to_string(),
            })
            .map_err(Into::into)
    }

    async fn resolve_resource_view<E>(&self, selector: ResourceSelector) -> Result<ResourceView, E>
    where
        E: From<ResolveManifestAccountError>
            + From<ResourceLookupProblem>
            + From<UnsupportedResourceDescriptorError>
            + From<InternalError>
            + From<GetResourceCrudDispatcherError>,
    {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;
        let schema = self.resolve_schema_for_selector::<E>(&selector.kind)?;

        let id = resolve_resource_id::<E>(
            self.generic_resource_query_service.as_ref(),
            &schema,
            &target_account.id,
            &selector.resource_ref,
        )
        .await?;

        let snapshot = self
            .resolve_snapshot_for_schema::<E>(&schema, &target_account.id, id)
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<E>(&self.catalog, &snapshot.schema)?;

        let view = dispatcher
            .get(ResourceCrudDispatcherGetRequest {
                account_id: target_account.id.clone(),
                id,
            })
            .await?;

        Ok(ResourceView {
            account: ResourceViewAccount {
                id: target_account.id,
                name: Some(target_account.name),
            },
            ..view
        })
    }

    async fn resolve_multiple_resource_views(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        (
            Vec<IndexedResource<ResourceView>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let names_by_schema = self.resource_kind_names_by_schema();

        let schema = self.resolve_schema_for_selector::<BatchResourceError>(&selector.kind)?;

        let groups = group_batch_resource_refs(selector);

        let resolution_response = resolve_batch_ids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
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
            .find_snapshots_by_ids(&target_account.id, &ids)
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
                        &snapshot.schema,
                    )?;
                    Ok(snapshot)
                }) {
                Ok(snapshot) => {
                    if !names_by_schema.contains_key(&snapshot.schema) {
                        return Err(UnsupportedResourceDescriptorError::NotFound {
                            schema: snapshot.schema.clone(),
                        }
                        .into());
                    }

                    let resource = ResourceView {
                        schema: snapshot.schema,
                        account: ResourceViewAccount {
                            id: target_account.id.clone(),
                            name: Some(target_account.name.clone()),
                        },
                        headers: ResourceViewHeaders::from_owned(id, snapshot.headers),
                        last_reconciled_at: snapshot.last_reconciled_at,
                        spec: snapshot.spec,
                        status: snapshot.status,
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

    async fn resolve_id_identity_groups(
        &self,
        account_id: &odf::AccountID,
        schema: &str,
        id_entries: BatchIdEntries,
        mut problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
        names_by_schema: &HashMap<String, String>,
    ) -> Result<
        (
            Vec<IndexedResource<ResourceIdentityView>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let ids = id_entries.iter().map(|(_, _, id)| *id).collect::<Vec<_>>();

        let rows_by_id = self
            .generic_resource_query_service
            .find_resource_identities_by_ids(account_id, &ids)
            .await?
            .into_iter()
            .map(|row| (row.id, row))
            .collect::<HashMap<_, _>>();

        let mut identities = Vec::new();
        for (request_index, _, id) in id_entries {
            let row_result = rows_by_id
                .get(id.as_ref())
                .cloned()
                .ok_or_else(|| id_not_found(id));
            match row_result.and_then(|row| {
                validate_identity_row(row, schema, ensure_schema_matches::<ResourceLookupProblem>)
            }) {
                Ok(row) => {
                    let identity =
                        resource_identity_from_row::<BatchResourceError>(row, names_by_schema)?;
                    identities.push(IndexedResource {
                        request_index,
                        item: identity,
                    });
                }
                Err(error) => problems.push(BatchResourceProblem {
                    request_index,
                    error,
                }),
            }
        }

        identities.sort_by_key(|identity| identity.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok((identities, problems))
    }

    async fn resolve_snapshot_for_schema<E>(
        &self,
        schema: &str,
        account_id: &odf::AccountID,
        id: ResourceID,
    ) -> Result<ResourceSnapshot, E>
    where
        E: From<InternalError> + From<ResourceLookupProblem>,
    {
        let Some(snapshot) = self.find_account_snapshot(account_id, id).await? else {
            return Err(ResourceLookupProblem::IDNotFound(ResourceIDNotFoundError(id)).into());
        };

        ensure_schema_matches::<E>(id, schema, &snapshot.schema)?;

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

        if snapshot.headers.account != *account_id {
            return Ok(None);
        }

        Ok(Some(snapshot))
    }

    async fn ensure_manifest_id_is_accessible(
        &self,
        schema: &str,
        account_id: &odf::AccountID,
        maybe_id: Option<ResourceID>,
    ) -> Result<(), ApplyManifestError> {
        let Some(id) = maybe_id else {
            return Ok(());
        };

        let Some(snapshot) = self.find_account_snapshot(account_id, id).await? else {
            return Err(ResourceIDNotFoundError(id).into());
        };

        if snapshot.schema != schema {
            return Err(
                ResourceTypeMismatchError::new(id, schema.to_string(), snapshot.schema).into(),
            );
        }

        Ok(())
    }

    fn apply_spec_view_mode<E>(
        &self,
        view: &mut ResourceView,
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
        resources: &mut [IndexedResource<ResourceView>],
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

        let headers = make_headers_input(&manifest, &target_account)?;
        let header_warnings = collect_manifest_header_warnings(&manifest);

        self.ensure_manifest_id_is_accessible(
            manifest.schema.as_str(),
            &target_account.id,
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
        schema: &str,
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
    target_account: ResolvedAccount,
    spec: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IndexedResource<T> {
    request_index: usize,
    item: T,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
