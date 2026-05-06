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
use kamu_resources_services::{get_resource_crud_dispatcher, get_resource_crud_dispatcher_by_kind};

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

        let descriptors_by_key = self.resource_kind_names_by_key();

        let resource_counts = self
            .generic_resource_query_service
            .summarize_resources(target_account.id)
            .await?
            .into_iter()
            .map(|row| {
                let name = descriptors_by_key
                    .get(&(row.kind.clone(), row.api_version.clone()))
                    .ok_or_else(|| {
                        ResourcesSummaryError::Internal(InternalError::new(format!(
                            "No resource descriptor registered for {}/{}",
                            row.kind, row.api_version
                        )))
                    })?
                    .clone();

                Ok(ResourceTypeCountSummary {
                    kind: row.kind,
                    name,
                    api_version: row.api_version,
                    total_count: row.total_count,
                    phase_counts: row.phase_counts,
                })
            })
            .collect::<Result<Vec<_>, ResourcesSummaryError>>()?;

        Ok(ResourcesSummary { resource_counts })
    }

    async fn get(&self, selector: ResourceSelector) -> Result<ResourceView, GetResourceError> {
        self.resolve_resource_view::<GetResourceError>(selector)
            .await
    }

    async fn get_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<ResourceView, ResourceLookupProblem>, BatchResourceError>
    {
        let (indexed_resources, problems) = self.resolve_multiple_resource_views(selector).await?;

        Ok(BatchResourceResponse {
            successes: indexed_resources
                .into_iter()
                .map(|resource| BatchResourceSuccess {
                    request_index: resource.request_index,
                    item: resource.item,
                })
                .collect(),
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

        let uid = resolve_resource_uid::<GetResourceError>(
            self.generic_resource_query_service.as_ref(),
            &selector.kind,
            &target_account.id,
            &selector.resource_ref,
        )
        .await?;

        let snapshot = self
            .resolve_snapshot_for_kind::<GetResourceError>(&selector.kind, &target_account.id, uid)
            .await?;

        ensure_requested_api_version::<GetResourceError>(
            selector.api_version.as_ref(),
            &snapshot.api_version,
        )?;

        let descriptors_by_key = self.resource_kind_names_by_key();
        resource_identity_from_snapshot::<GetResourceError>(snapshot, &descriptors_by_key)
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

        let descriptors_by_key = self.resource_kind_names_by_key();

        let kind = selector.kind.clone();
        let api_version = selector.api_version.clone();
        get_resource_crud_dispatcher_by_kind::<BatchResourceError>(&self.catalog, &kind)?;

        let groups = group_batch_resource_refs(selector);
        let resolution_response = resolve_batch_uids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
            &kind,
            groups,
        )
        .await?;

        let (identities, problems) = self
            .resolve_uid_identity_groups(
                &target_account.id,
                &kind,
                api_version.as_ref(),
                resolution_response.uid_entries,
                resolution_response.problems,
                &descriptors_by_key,
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
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let view = self
            .resolve_resource_view::<RenderResourceManifestError>(selector)
            .await?;

        let manifest = resource_view_to_manifest(view);
        let manifest =
            serialize_manifest(&manifest, format).map_err(RenderResourceManifestError::Internal)?;

        Ok(RenderResourceManifestResult { manifest, format })
    }

    async fn render_manifests(
        &self,
        selector: ResourceBatchSelector,
        format: ResourceManifestFormat,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        let (indexed_resources, get_problems) =
            self.resolve_multiple_resource_views(selector).await?;

        let mut successes = Vec::new();
        let problems = get_problems;

        for resource in indexed_resources {
            let manifest = resource_view_to_manifest(resource.item);
            let manifest =
                serialize_manifest(&manifest, format).map_err(BatchResourceError::Internal)?;

            successes.push(BatchResourceSuccess {
                request_index: resource.request_index,
                item: RenderResourceManifestResult { manifest, format },
            });
        }

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

        let dispatcher = get_resource_crud_dispatcher_by_kind::<ListResourcesError>(
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

        get_resource_crud_dispatcher_by_kind::<ListResourcesError>(&self.catalog, &request.kind)?;

        let snapshots = self
            .generic_resource_query_service
            .list_snapshots_by_kind(target_account.id, &request.kind, request.pagination)
            .await?;

        let descriptors_by_key = self.resource_kind_names_by_key();
        map_snapshots_to_identities(snapshots, &descriptors_by_key).map_err(Into::into)
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

        let descriptors_by_key = self.resource_kind_names_by_key();
        map_snapshots_to_identities(snapshots, &descriptors_by_key).map_err(Into::into)
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError> {
        let manifest = parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.metadata.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            &manifest.kind,
            &manifest.api_version,
        )?;

        let metadata = make_metadata_input(&manifest, &target_account)?;
        let metadata_warnings = collect_manifest_metadata_warnings(&manifest);

        self.ensure_manifest_uid_is_accessible(
            &manifest.kind,
            &manifest.api_version,
            &target_account.id,
            manifest.metadata.uid,
        )
        .await?;

        let plan = dispatcher
            .plan_apply(ResourceCrudDispatcherApplyRequest {
                uid: manifest.metadata.uid,
                metadata,
                spec: manifest.spec,
            })
            .await?;

        Ok(match plan {
            ApplyManifestPlanningDecision::Planned(mut plan) => {
                plan.warnings.splice(0..0, metadata_warnings);
                plan.resource.account = ResourceViewAccount {
                    id: target_account.id,
                    name: Some(target_account.name),
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
        let manifest = parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.metadata.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            &manifest.kind,
            &manifest.api_version,
        )?;

        let metadata = make_metadata_input(&manifest, &target_account)?;
        let metadata_warnings = collect_manifest_metadata_warnings(&manifest);

        self.ensure_manifest_uid_is_accessible(
            &manifest.kind,
            &manifest.api_version,
            &target_account.id,
            manifest.metadata.uid,
        )
        .await?;

        let result = dispatcher
            .apply(ResourceCrudDispatcherApplyRequest {
                uid: manifest.metadata.uid,
                metadata,
                spec: manifest.spec,
            })
            .await?;

        Ok(match result {
            ApplyManifestApplicationDecision::Applied(mut result) => {
                result.warnings.splice(0..0, metadata_warnings);
                result.resource.account = ResourceViewAccount {
                    id: target_account.id,
                    name: Some(target_account.name),
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
    ) -> Result<BatchResourceResponse<ResourceUID, ResourceLookupProblem>, BatchResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(selector.account.as_ref())
            .await?;

        let kind = selector.kind.clone();
        let api_version = selector.api_version.clone();
        get_resource_crud_dispatcher_by_kind::<BatchResourceError>(&self.catalog, &kind)?;

        let groups = group_batch_resource_refs(selector);
        let resolution_response = resolve_batch_uids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
            &kind,
            groups,
        )
        .await?;

        let mut problems = resolution_response.problems;
        let mut successes = Vec::new();
        let mut seen_valid_uids = HashSet::new();
        let mut uids_by_api_version = HashMap::<String, Vec<ResourceUID>>::new();

        let uids = resolution_response
            .uid_entries
            .iter()
            .map(|(_, _, uid)| *uid)
            .collect::<Vec<_>>();

        let rows_by_uid = self
            .generic_resource_query_service
            .find_resource_identities_by_uids(&target_account.id, &uids)
            .await?
            .into_iter()
            .map(|row| (row.uid, row))
            .collect::<HashMap<_, _>>();

        for (request_index, _, uid) in resolution_response.uid_entries {
            let row_result = rows_by_uid
                .get(uid.as_ref())
                .cloned()
                .ok_or_else(|| uid_not_found(uid));

            match row_result.and_then(|row| {
                validate_identity_row(
                    row,
                    &kind,
                    api_version.as_ref(),
                    ensure_kind_matches::<ResourceLookupProblem>,
                    ensure_requested_api_version::<ResourceLookupProblem>,
                )
            }) {
                Ok(row) => {
                    successes.push(BatchResourceSuccess {
                        request_index,
                        item: uid,
                    });

                    if seen_valid_uids.insert(uid) {
                        uids_by_api_version
                            .entry(row.api_version)
                            .or_default()
                            .push(uid);
                    }
                }
                Err(error) => problems.push(BatchResourceProblem {
                    request_index,
                    error,
                }),
            }
        }

        for (api_version, uids) in uids_by_api_version {
            let dispatcher = get_resource_crud_dispatcher::<BatchResourceError>(
                &self.catalog,
                &kind,
                &api_version,
            )?;
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: target_account.id.clone(),
                    uids,
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

    async fn delete(&self, selector: ResourceSelector) -> Result<ResourceUID, DeleteResourceError> {
        let response = self
            .delete_many(ResourceBatchSelector {
                account: selector.account,
                kind: selector.kind,
                api_version: selector.api_version,
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

            if seen.insert((descriptor.resource_type, descriptor.api_version)) {
                descriptors.push(ResourceKindDescriptor {
                    name: presentation.resource_name.to_string(),
                    short_names: presentation
                        .resource_short_names
                        .iter()
                        .map(ToString::to_string)
                        .collect(),
                    kind: descriptor.resource_type.to_string(),
                    api_version: descriptor.api_version.to_string(),
                    list_columns: presentation
                        .list_columns
                        .iter()
                        .copied()
                        .map(Into::into)
                        .collect(),
                });
            }
        }

        descriptors.sort_by(|a, b| {
            a.kind
                .cmp(&b.kind)
                .then_with(|| a.api_version.cmp(&b.api_version))
        });

        descriptors
    }

    fn resource_kind_names_by_key(&self) -> HashMap<(String, String), String> {
        self.list_resource_kind_descriptors()
            .into_iter()
            .map(|descriptor| ((descriptor.kind, descriptor.api_version), descriptor.name))
            .collect()
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

        let uid = resolve_resource_uid::<E>(
            self.generic_resource_query_service.as_ref(),
            &selector.kind,
            &target_account.id,
            &selector.resource_ref,
        )
        .await?;

        let snapshot = self
            .resolve_snapshot_for_kind::<E>(&selector.kind, &target_account.id, uid)
            .await?;

        ensure_requested_api_version::<E>(selector.api_version.as_ref(), &snapshot.api_version)?;

        let dispatcher = get_resource_crud_dispatcher::<E>(
            &self.catalog,
            &snapshot.kind,
            &snapshot.api_version,
        )?;

        let view = dispatcher
            .get(ResourceCrudDispatcherGetRequest {
                account_id: target_account.id.clone(),
                uid,
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

        let descriptors_by_key = self.resource_kind_names_by_key();

        let kind = selector.kind.clone();
        let api_version = selector.api_version.clone();
        get_resource_crud_dispatcher_by_kind::<BatchResourceError>(&self.catalog, &kind)?;

        let groups = group_batch_resource_refs(selector);

        let resolution_response = resolve_batch_uids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
            &kind,
            groups,
        )
        .await?;

        let mut indexed_resources = Vec::new();
        let mut problems = resolution_response.problems;

        let uids = resolution_response
            .uid_entries
            .iter()
            .map(|(_, _, uid)| *uid)
            .collect::<Vec<_>>();

        let snapshots_by_uid = self
            .generic_resource_query_service
            .find_snapshots_by_uids(&target_account.id, &uids)
            .await?
            .into_iter()
            .map(|snapshot| (snapshot.uid, snapshot))
            .collect::<HashMap<_, _>>();

        for (request_index, _, uid) in resolution_response.uid_entries {
            match snapshots_by_uid
                .get(&uid)
                .cloned()
                .ok_or(ResourceLookupProblem::UIDNotFound(
                    ResourceUIDNotFoundError(uid),
                ))
                .and_then(|snapshot| {
                    ensure_kind_matches::<ResourceLookupProblem>(
                        snapshot.uid,
                        &kind,
                        &snapshot.kind,
                    )?;
                    ensure_requested_api_version::<ResourceLookupProblem>(
                        api_version.as_ref(),
                        &snapshot.api_version,
                    )?;
                    Ok(snapshot)
                }) {
                Ok(snapshot) => {
                    if !descriptors_by_key
                        .contains_key(&(snapshot.kind.clone(), snapshot.api_version.clone()))
                    {
                        return Err(UnsupportedResourceDescriptorError::NotFound {
                            kind: snapshot.kind.clone(),
                            api_version: snapshot.api_version.clone(),
                        }
                        .into());
                    }

                    let resource = ResourceView {
                        kind: snapshot.kind,
                        api_version: snapshot.api_version,
                        account: ResourceViewAccount {
                            id: target_account.id.clone(),
                            name: Some(target_account.name.clone()),
                        },
                        metadata: ResourceViewMetadata::from_owned(uid, snapshot.metadata),
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

    async fn resolve_uid_identity_groups(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        api_version: Option<&String>,
        uid_entries: BatchUidEntries,
        mut problems: Vec<BatchResourceProblem<ResourceLookupProblem>>,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<
        (
            Vec<IndexedResource<ResourceIdentityView>>,
            Vec<BatchResourceProblem<ResourceLookupProblem>>,
        ),
        BatchResourceError,
    > {
        let uids = uid_entries
            .iter()
            .map(|(_, _, uid)| *uid)
            .collect::<Vec<_>>();

        let rows_by_uid = self
            .generic_resource_query_service
            .find_resource_identities_by_uids(account_id, &uids)
            .await?
            .into_iter()
            .map(|row| (row.uid, row))
            .collect::<HashMap<_, _>>();

        let mut identities = Vec::new();
        for (request_index, _, uid) in uid_entries {
            let row_result = rows_by_uid
                .get(uid.as_ref())
                .cloned()
                .ok_or_else(|| uid_not_found(uid));
            match row_result.and_then(|row| {
                validate_identity_row(
                    row,
                    kind,
                    api_version,
                    ensure_kind_matches::<ResourceLookupProblem>,
                    ensure_requested_api_version::<ResourceLookupProblem>,
                )
            }) {
                Ok(row) => {
                    let identity = resource_identity_from_row(row, descriptors_by_key)?;
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

    async fn resolve_snapshot_for_kind<E>(
        &self,
        kind: &str,
        account_id: &odf::AccountID,
        uid: ResourceUID,
    ) -> Result<ResourceSnapshot, E>
    where
        E: From<InternalError> + From<ResourceLookupProblem>,
    {
        let Some(snapshot) = self.find_account_snapshot(account_id, uid).await? else {
            return Err(ResourceLookupProblem::UIDNotFound(ResourceUIDNotFoundError(uid)).into());
        };

        ensure_kind_matches::<E>(uid, kind, &snapshot.kind)?;

        Ok(snapshot)
    }

    async fn find_account_snapshot(
        &self,
        account_id: &odf::AccountID,
        uid: ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let Some(snapshot) = self
            .generic_resource_query_service
            .get_snapshot_by_uid(&uid)
            .await?
        else {
            return Ok(None);
        };

        if snapshot.metadata.account != *account_id {
            return Ok(None);
        }

        Ok(Some(snapshot))
    }

    async fn ensure_manifest_uid_is_accessible(
        &self,
        kind: &str,
        api_version: &str,
        account_id: &odf::AccountID,
        maybe_uid: Option<ResourceUID>,
    ) -> Result<(), ApplyManifestError> {
        let Some(uid) = maybe_uid else {
            return Ok(());
        };

        let Some(snapshot) = self.find_account_snapshot(account_id, uid).await? else {
            return Err(ResourceUIDNotFoundError(uid).into());
        };

        if snapshot.kind != kind || snapshot.api_version != api_version {
            return Err(ResourceTypeMismatchError::new(
                uid,
                kind.to_string(),
                api_version.to_string(),
                snapshot.kind,
                snapshot.api_version,
            )
            .into());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IndexedResource<T> {
    request_index: usize,
    item: T,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
