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

use domain::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    GenericResourceQueryService,
    ResourceAPIVersionMismatchError,
    ResourceCrudDispatcherApplyRequest,
    ResourceCrudDispatcherDeleteRequest,
    ResourceCrudDispatcherGetRequest,
    ResourceCrudDispatcherListRequest,
    ResourceIdentityRow,
    ResourceIdentityView,
    ResourceKindDescriptor,
    ResourceNameNotFoundError,
    ResourcePresentationDispatcher,
    ResourceSnapshot,
    ResourceSummaryView,
    ResourceTypeCountSummary,
    ResourceUID,
    ResourceUIDNotFoundError,
    ResourceView,
    ResourcesSummary,
    UnsupportedResourceDescriptorError,
};
use internal_error::InternalError;
use kamu_resources::{self as domain};
use kamu_resources_services::{get_resource_crud_dispatcher, get_resource_crud_dispatcher_by_kind};

use super::batch_uid_resolver::{
    BatchUidEntries,
    group_batch_requests,
    resolve_batch_uids,
    resolve_resource_uid,
    uid_not_found,
};
use super::manifest_support;
use crate::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchRequest,
    BatchRequestProblem,
    BatchRequestResponse,
    DeleteResourceError,
    DeleteResourceRequest,
    GetResourceError,
    GetResourceRequest,
    ListAllResourceIdentitiesRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourceIdentitiesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ListSupportedResourceKindsError,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    RenderResourceManifestResult,
    ResourceAccountResolver,
    ResourceFacade,
    ResourceKindMismatchError,
    ResourceRef,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    ScalarRequest,
};

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

    async fn get(
        &self,
        request: ScalarRequest<GetResourceRequest>,
    ) -> Result<ResourceView, GetResourceError> {
        let ScalarRequest { account, request } = request;
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(account.as_ref())
            .await?;

        let uid = self
            .resolve_resource_uid::<GetResourceError>(
                &request.kind,
                &target_account.id,
                &request.resource_ref,
            )
            .await?;

        let snapshot = self
            .resolve_snapshot_for_kind::<GetResourceError>(&request.kind, &target_account.id, uid)
            .await?;

        Self::ensure_requested_api_version::<GetResourceError>(
            request.api_version.as_ref(),
            &snapshot.api_version,
        )?;

        let dispatcher = get_resource_crud_dispatcher::<GetResourceError>(
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
            account: domain::ResourceViewAccount {
                id: target_account.id,
                name: Some(target_account.name),
            },
            ..view
        })
    }

    async fn get_many(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<ResourceView, GetResourceError>, GetResourceError> {
        let (indexed_resources, problems) = self.get_many_indexed(request).await?;

        Ok(BatchRequestResponse {
            items: indexed_resources
                .into_iter()
                .map(|resource| resource.resource)
                .collect(),
            problems,
        })
    }

    async fn get_identity(
        &self,
        request: ScalarRequest<GetResourceRequest>,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let ScalarRequest { account, request } = request;
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(account.as_ref())
            .await?;

        let uid = self
            .resolve_resource_uid::<GetResourceError>(
                &request.kind,
                &target_account.id,
                &request.resource_ref,
            )
            .await?;

        let snapshot = self
            .resolve_snapshot_for_kind::<GetResourceError>(&request.kind, &target_account.id, uid)
            .await?;

        Self::ensure_requested_api_version::<GetResourceError>(
            request.api_version.as_ref(),
            &snapshot.api_version,
        )?;

        let descriptors_by_key = self.resource_kind_names_by_key();
        Self::resource_identity_from_snapshot::<GetResourceError>(snapshot, &descriptors_by_key)
    }

    async fn get_identities(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<ResourceIdentityView, GetResourceError>, GetResourceError>
    {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let descriptors_by_key = self.resource_kind_names_by_key();
        let groups = group_batch_requests(request.requests);
        let resolution_response = resolve_batch_uids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
            groups,
        )
        .await?;

        let (identities, problems) = self
            .resolve_uid_identity_groups(
                &target_account.id,
                resolution_response.uid_entries,
                resolution_response.problems,
                &descriptors_by_key,
            )
            .await?;

        Ok(BatchRequestResponse {
            items: identities
                .into_iter()
                .map(|indexed_identity| indexed_identity.identity)
                .collect(),
            problems,
        })
    }

    async fn render_manifest(
        &self,
        request: ScalarRequest<RenderResourceManifestRequest>,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let ScalarRequest { account, request } = request;
        let view = self
            .get(ScalarRequest {
                account,
                request: GetResourceRequest {
                    kind: request.kind,
                    api_version: request.api_version,
                    resource_ref: request.resource_ref,
                },
            })
            .await?;

        let manifest = manifest_support::resource_view_to_manifest(view);
        let manifest = manifest_support::serialize_manifest(&manifest, request.format)?;

        Ok(RenderResourceManifestResult {
            manifest,
            format: request.format,
        })
    }

    async fn render_manifests(
        &self,
        request: BatchRequest<RenderResourceManifestRequest>,
    ) -> Result<
        BatchRequestResponse<RenderResourceManifestResult, RenderResourceManifestError>,
        RenderResourceManifestError,
    > {
        let formats_by_index = request
            .requests
            .iter()
            .map(|r| r.format)
            .collect::<Vec<_>>();

        let get_batch = BatchRequest {
            account: request.account,
            requests: request
                .requests
                .into_iter()
                .map(|r| GetResourceRequest {
                    kind: r.kind,
                    api_version: r.api_version,
                    resource_ref: r.resource_ref,
                })
                .collect(),
        };

        let (indexed_resources, get_problems) = self.get_many_indexed(get_batch).await?;

        let manifests = indexed_resources
            .into_iter()
            .map(|resource| {
                let format = formats_by_index[resource.request_index];
                let manifest = manifest_support::resource_view_to_manifest(resource.resource);
                let manifest = manifest_support::serialize_manifest(&manifest, format)?;
                Ok(RenderResourceManifestResult { manifest, format })
            })
            .collect::<Result<Vec<_>, RenderResourceManifestError>>()?;

        let problems = get_problems
            .into_iter()
            .map(|problem| BatchRequestProblem {
                request_index: problem.request_index,
                error: problem.error.into(),
            })
            .collect();

        Ok(BatchRequestResponse {
            items: manifests,
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
        Self::map_snapshots_to_identities(snapshots, &descriptors_by_key).map_err(Into::into)
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
        Self::map_snapshots_to_identities(snapshots, &descriptors_by_key).map_err(Into::into)
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError> {
        let manifest = manifest_support::parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.metadata.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            &manifest.kind,
            &manifest.api_version,
        )?;

        let metadata = manifest_support::make_metadata_input(&manifest, &target_account)?;
        let metadata_warnings = manifest_support::collect_manifest_metadata_warnings(&manifest);

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
                plan.resource.account = domain::ResourceViewAccount {
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
        let manifest = manifest_support::parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.metadata.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            &manifest.kind,
            &manifest.api_version,
        )?;

        let metadata = manifest_support::make_metadata_input(&manifest, &target_account)?;
        let metadata_warnings = manifest_support::collect_manifest_metadata_warnings(&manifest);

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
                result.resource.account = domain::ResourceViewAccount {
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

    async fn delete(
        &self,
        request: DeleteResourceRequest,
    ) -> Result<ResourceUID, DeleteResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let uid = self
            .resolve_resource_uid::<DeleteResourceError>(
                &request.kind,
                &target_account.id,
                &request.resource_ref,
            )
            .await?;

        let snapshot = self
            .resolve_snapshot_for_kind::<DeleteResourceError>(
                &request.kind,
                &target_account.id,
                uid,
            )
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<DeleteResourceError>(
            &self.catalog,
            &request.kind,
            &snapshot.api_version,
        )?;
        dispatcher
            .delete(ResourceCrudDispatcherDeleteRequest {
                account_id: target_account.id,
                uids: vec![uid],
            })
            .await?;

        Ok(uid)
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

    async fn get_many_indexed(
        &self,
        batch: BatchRequest<GetResourceRequest>,
    ) -> Result<
        (
            Vec<IndexedResourceView>,
            Vec<BatchRequestProblem<GetResourceError>>,
        ),
        GetResourceError,
    > {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(batch.account.as_ref())
            .await?;

        let descriptors_by_key = self.resource_kind_names_by_key();
        let groups = group_batch_requests(batch.requests);

        let resolution_response = resolve_batch_uids(
            self.generic_resource_query_service.as_ref(),
            &target_account.id,
            groups,
        )
        .await?;

        let uid_entries = resolution_response.uid_entries;
        let mut indexed_resources = Vec::new();
        let mut problems = resolution_response.problems;

        let uids = uid_entries
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

        for (request_index, get_request, uid) in uid_entries {
            match snapshots_by_uid
                .get(&uid)
                .cloned()
                .ok_or_else(|| GetResourceError::UIDNotFound(ResourceUIDNotFoundError(uid)))
                .and_then(|snapshot| {
                    Self::validate_materialized_snapshot(
                        snapshot,
                        &get_request,
                        &descriptors_by_key,
                    )
                }) {
                Ok(snapshot) => {
                    let mut resource = Self::resource_view_from_snapshot(snapshot);
                    resource.account = domain::ResourceViewAccount {
                        id: target_account.id.clone(),
                        name: Some(target_account.name.clone()),
                    };

                    indexed_resources.push(IndexedResourceView {
                        request_index,
                        resource,
                    });
                }
                Err(error) => {
                    problems.push(BatchRequestProblem {
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

    fn validate_materialized_snapshot(
        snapshot: ResourceSnapshot,
        request: &GetResourceRequest,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<ResourceSnapshot, GetResourceError> {
        Self::ensure_kind_matches::<GetResourceError>(snapshot.uid, &request.kind, &snapshot.kind)?;
        Self::ensure_requested_api_version::<GetResourceError>(
            request.api_version.as_ref(),
            &snapshot.api_version,
        )?;

        if !descriptors_by_key.contains_key(&(snapshot.kind.clone(), snapshot.api_version.clone()))
        {
            return Err(UnsupportedResourceDescriptorError::NotFound {
                kind: snapshot.kind,
                api_version: snapshot.api_version,
            }
            .into());
        }

        Ok(snapshot)
    }

    fn resource_view_from_snapshot(snapshot: ResourceSnapshot) -> ResourceView {
        let ResourceSnapshot {
            uid,
            kind,
            api_version,
            metadata,
            spec,
            status,
            last_reconciled_at,
            ..
        } = snapshot;

        ResourceView {
            kind,
            api_version,
            account: domain::ResourceViewAccount {
                id: metadata.account.clone(),
                name: None,
            },
            metadata: domain::ResourceViewMetadata::from_owned(uid, metadata),
            last_reconciled_at,
            spec,
            status,
        }
    }

    async fn resolve_uid_identity_groups(
        &self,
        account_id: &odf::AccountID,
        uid_entries: BatchUidEntries,
        mut problems: Vec<BatchRequestProblem<GetResourceError>>,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<
        (
            Vec<IndexedResourceIdentity>,
            Vec<BatchRequestProblem<GetResourceError>>,
        ),
        GetResourceError,
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
        for (request_index, get_request, uid) in uid_entries {
            let row_result = rows_by_uid
                .get(uid.as_ref())
                .cloned()
                .ok_or_else(|| uid_not_found(uid));
            match row_result
                .and_then(|row| Self::validate_identity_row(row, &get_request, descriptors_by_key))
            {
                Ok(identity) => identities.push(IndexedResourceIdentity {
                    request_index,
                    identity,
                }),
                Err(error) => problems.push(BatchRequestProblem {
                    request_index,
                    error,
                }),
            }
        }

        identities.sort_by_key(|identity| identity.request_index);
        problems.sort_by_key(|problem| problem.request_index);

        Ok((identities, problems))
    }

    fn resource_identity_from_snapshot<E>(
        snapshot: ResourceSnapshot,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<ResourceIdentityView, E>
    where
        E: From<UnsupportedResourceDescriptorError>,
    {
        let key = (snapshot.kind.clone(), snapshot.api_version.clone());
        let found = descriptors_by_key.get(&key);
        let canonical_kind_name = found
            .ok_or_else(|| {
                let (kind, api_version) = key;
                UnsupportedResourceDescriptorError::NotFound { kind, api_version }
            })?
            .clone();

        Ok(ResourceIdentityView {
            kind: snapshot.kind,
            api_version: snapshot.api_version,
            canonical_kind_name,
            uid: snapshot.uid,
            name: snapshot.metadata.name,
        })
    }

    fn resource_identity_from_row(
        row: ResourceIdentityRow,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let key = (row.kind.clone(), row.api_version.clone());
        let found = descriptors_by_key.get(&key);
        let canonical_kind_name = found
            .ok_or_else(|| {
                let (kind, api_version) = key;
                UnsupportedResourceDescriptorError::NotFound { kind, api_version }
            })?
            .clone();

        Ok(ResourceIdentityView {
            kind: row.kind,
            api_version: row.api_version,
            canonical_kind_name,
            uid: ResourceUID::new(row.uid),
            name: row.name,
        })
    }

    fn validate_identity_row(
        row: ResourceIdentityRow,
        request: &GetResourceRequest,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        Self::ensure_kind_matches::<GetResourceError>(
            ResourceUID::new(row.uid),
            &request.kind,
            &row.kind,
        )?;
        Self::ensure_requested_api_version::<GetResourceError>(
            request.api_version.as_ref(),
            &row.api_version,
        )?;

        Self::resource_identity_from_row(row, descriptors_by_key)
    }

    async fn resolve_resource_uid<E>(
        &self,
        kind: &str,
        account_id: &odf::AccountID,
        resource_ref: &ResourceRef,
    ) -> Result<ResourceUID, E>
    where
        E: From<InternalError> + From<ResourceNameNotFoundError>,
    {
        resolve_resource_uid(
            self.generic_resource_query_service.as_ref(),
            kind,
            account_id,
            resource_ref,
        )
        .await
    }

    fn map_snapshots_to_identities(
        snapshots: Vec<ResourceSnapshot>,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<Vec<ResourceIdentityView>, InternalError> {
        snapshots
            .into_iter()
            .map(|snapshot| {
                Self::resource_identity_from_snapshot::<UnsupportedResourceDescriptorError>(
                    snapshot,
                    descriptors_by_key,
                )
                .map_err(|error| InternalError::new(format!("{error}")))
            })
            .collect()
    }

    fn ensure_kind_matches<E>(
        uid: ResourceUID,
        expected_kind: &str,
        actual_kind: &str,
    ) -> Result<(), E>
    where
        E: From<ResourceKindMismatchError>,
    {
        if actual_kind != expected_kind {
            return Err(ResourceKindMismatchError {
                uid,
                expected_kind: expected_kind.to_string(),
                actual_kind: actual_kind.to_string(),
            }
            .into());
        }

        Ok(())
    }

    fn ensure_requested_api_version<E>(
        expected_api_version: Option<&String>,
        actual_api_version: &str,
    ) -> Result<(), E>
    where
        E: From<ResourceAPIVersionMismatchError>,
    {
        if let Some(expected_api_version) = expected_api_version
            && actual_api_version != expected_api_version
        {
            return Err(ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.clone(),
                actual_api_version: actual_api_version.to_string(),
            }
            .into());
        }

        Ok(())
    }

    async fn resolve_snapshot_for_kind<E>(
        &self,
        kind: &str,
        account_id: &odf::AccountID,
        uid: ResourceUID,
    ) -> Result<ResourceSnapshot, E>
    where
        E: From<InternalError> + From<ResourceUIDNotFoundError> + From<ResourceKindMismatchError>,
    {
        let Some(snapshot) = self
            .generic_resource_query_service
            .get_snapshot_by_uid(&uid)
            .await?
        else {
            return Err(ResourceUIDNotFoundError(uid).into());
        };

        Self::ensure_kind_matches::<E>(uid, kind, &snapshot.kind)?;

        if snapshot.metadata.account != *account_id {
            return Err(ResourceUIDNotFoundError(uid).into());
        }

        Ok(snapshot)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct IndexedResourceIdentity {
    request_index: usize,
    identity: ResourceIdentityView,
}

struct IndexedResourceView {
    request_index: usize,
    resource: ResourceView,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
