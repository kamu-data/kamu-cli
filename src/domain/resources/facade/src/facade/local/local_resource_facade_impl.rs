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
    ResourceManifest,
    ResourceMetadataInput,
    ResourceName,
    ResourceNameNotFoundError,
    ResourcePresentationDispatcher,
    ResourceSnapshot,
    ResourceSummaryView,
    ResourceTypeCountSummary,
    ResourceUID,
    ResourceUIDNotFoundError,
    ResourceView,
    ResourceWarning,
    ResourcesSummary,
    UnsupportedResourceDescriptorError,
};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources as domain;
use kamu_resources_services::{get_resource_crud_dispatcher, get_resource_crud_dispatcher_by_kind};

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
    ParseResourceManifestError,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    RenderResourceManifestResult,
    ResolvedAccount,
    ResourceAccountResolver,
    ResourceFacade,
    ResourceKindMismatchError,
    ResourceManifestFormat,
    ResourceRef,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
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

        let resource_kind_descriptors = self.list_resource_kind_descriptors();
        let descriptors_by_key: HashMap<(String, String), String> = resource_kind_descriptors
            .into_iter()
            .map(|descriptor| ((descriptor.kind, descriptor.api_version), descriptor.name))
            .collect();

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

    async fn get(&self, request: GetResourceRequest) -> Result<ResourceView, GetResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let uid = self
            .resolve_resource_uid::<GetResourceError>(
                &request.kind,
                &target_account.id,
                &request.resource_ref,
            )
            .await?;

        let snapshot = self
            .resolve_snapshot_for_kind(&request.kind, &target_account.id, &uid)
            .await?;

        if let Some(expected_api_version) = request.api_version.as_ref()
            && snapshot.api_version != *expected_api_version
        {
            return Err(ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.clone(),
                actual_api_version: snapshot.api_version,
            }
            .into());
        }

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

        self.resource_account_resolver
            .hydrate_resource_view_account(view, Some(&target_account))
            .await
            .map_err(Into::into)
    }

    async fn get_many(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<ResourceView, GetResourceError>, GetResourceError> {
        let (mut indexed_resources, mut problems) = self.get_many_indexed(request.requests).await?;

        indexed_resources.sort_by_key(|resource| resource.request_index);
        problems.sort_by_key(|problem| problem.request_index);

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
        request: GetResourceRequest,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let uid = self
            .resolve_resource_uid::<GetResourceError>(
                &request.kind,
                &target_account.id,
                &request.resource_ref,
            )
            .await?;

        let snapshot = self
            .resolve_snapshot_for_kind(&request.kind, &target_account.id, &uid)
            .await?;

        if let Some(expected_api_version) = request.api_version.as_ref()
            && snapshot.api_version != *expected_api_version
        {
            return Err(ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.clone(),
                actual_api_version: snapshot.api_version,
            }
            .into());
        }

        self.resource_identity_from_snapshot(snapshot)
    }

    async fn get_identities(
        &self,
        request: BatchRequest<GetResourceRequest>,
    ) -> Result<BatchRequestResponse<ResourceIdentityView, GetResourceError>, GetResourceError>
    {
        let descriptors_by_key = self.resource_kind_names_by_key();
        let mut groups = self.group_batch_identity_requests(request.requests).await;
        let mut indexed_identities = Vec::new();

        self.resolve_uid_identity_groups(
            groups.uid_groups,
            &descriptors_by_key,
            &mut indexed_identities,
            &mut groups.problems,
        )
        .await?;

        self.resolve_name_identity_groups(
            groups.name_groups,
            &descriptors_by_key,
            &mut indexed_identities,
            &mut groups.problems,
        )
        .await?;

        indexed_identities.sort_by_key(|identity| identity.request_index);
        groups.problems.sort_by_key(|problem| problem.request_index);

        Ok(BatchRequestResponse {
            items: indexed_identities
                .into_iter()
                .map(|indexed_identity| indexed_identity.identity)
                .collect(),
            problems: groups.problems,
        })
    }

    async fn render_manifest(
        &self,
        request: RenderResourceManifestRequest,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let view = self
            .get(GetResourceRequest {
                kind: request.kind,
                api_version: request.api_version,
                account: request.account,
                resource_ref: request.resource_ref,
            })
            .await?;

        let manifest = Self::resource_view_to_manifest(view);
        let manifest = Self::serialize_manifest(&manifest, request.format)?;

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
            .map(|request| request.format)
            .collect::<Vec<_>>();

        let get_requests = request
            .requests
            .into_iter()
            .map(|request| GetResourceRequest {
                kind: request.kind,
                api_version: request.api_version,
                account: request.account,
                resource_ref: request.resource_ref,
            })
            .collect();

        let (mut indexed_resources, mut get_problems) = self.get_many_indexed(get_requests).await?;

        indexed_resources.sort_by_key(|resource| resource.request_index);
        get_problems.sort_by_key(|problem| problem.request_index);

        let manifests = indexed_resources
            .into_iter()
            .map(|resource| {
                let format = formats_by_index[resource.request_index];
                let manifest = Self::resource_view_to_manifest(resource.resource);
                let manifest = Self::serialize_manifest(&manifest, format)?;
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

        snapshots
            .into_iter()
            .map(|snapshot| {
                self.resource_identity_from_snapshot::<UnsupportedResourceDescriptorError>(snapshot)
                    .map_err(|error| InternalError::new(format!("{error}")).into())
            })
            .collect()
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

        snapshots
            .into_iter()
            .map(|snapshot| {
                self.resource_identity_from_snapshot::<UnsupportedResourceDescriptorError>(snapshot)
                    .map_err(|error| InternalError::new(format!("{error}")).into())
            })
            .collect()
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestPlanningDecision, ApplyManifestError> {
        let manifest = self.parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.metadata.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            &manifest.kind,
            &manifest.api_version,
        )?;

        let metadata = self.make_metadata_input(&manifest, &target_account)?;
        let metadata_warnings = Self::collect_manifest_metadata_warnings(&manifest);

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
                plan.resource = self
                    .resource_account_resolver
                    .hydrate_resource_view_account(plan.resource, Some(&target_account))
                    .await?;

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
        let manifest = self.parse_manifest(request.format, &request.manifest)?;

        let target_account = self
            .resource_account_resolver
            .resolve_target_account(manifest.metadata.account.as_ref())
            .await?;

        let dispatcher = get_resource_crud_dispatcher::<ApplyManifestError>(
            &self.catalog,
            &manifest.kind,
            &manifest.api_version,
        )?;

        let metadata = self.make_metadata_input(&manifest, &target_account)?;
        let metadata_warnings = Self::collect_manifest_metadata_warnings(&manifest);

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
                result.resource = self
                    .resource_account_resolver
                    .hydrate_resource_view_account(result.resource, Some(&target_account))
                    .await?;

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
            .generic_resource_query_service
            .get_snapshot_by_uid(&uid)
            .await?;

        let snapshot =
            self.ensure_snapshot_matches_kind(&request.kind, &target_account.id, uid, snapshot)?;

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
    const WARNING_CODE_MISSING_DESCRIPTION: &str = "missing_description";

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
        requests: Vec<GetResourceRequest>,
    ) -> Result<
        (
            Vec<IndexedResourceView>,
            Vec<BatchRequestProblem<GetResourceError>>,
        ),
        GetResourceError,
    > {
        let descriptors_by_key = self.resource_kind_names_by_key();
        let groups = self.group_batch_materialization_requests(requests).await;
        let mut indexed_resources = Vec::new();
        let mut problems = groups.problems;

        for (account_id, entries) in groups.uid_groups {
            let uids = entries
                .iter()
                .map(|(_, _, uid, _)| *uid)
                .collect::<Vec<_>>();
            let snapshots_by_uid = self
                .generic_resource_query_service
                .find_snapshots_by_uids(&account_id, &uids)
                .await?
                .into_iter()
                .map(|snapshot| (snapshot.uid, snapshot))
                .collect::<HashMap<_, _>>();

            for (request_index, get_request, uid, target_account) in entries {
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
                        let resource = Self::resource_view_from_snapshot(snapshot);
                        let resource = self
                            .resource_account_resolver
                            .hydrate_resource_view_account(resource, Some(&target_account))
                            .await
                            .map_err(GetResourceError::Internal)?;
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
        }

        Ok((indexed_resources, problems))
    }

    async fn group_batch_materialization_requests(
        &self,
        requests: Vec<GetResourceRequest>,
    ) -> BatchMaterializationGroups {
        let mut uid_groups = HashMap::new();
        let mut problems = Vec::new();

        for (request_index, get_request) in requests.into_iter().enumerate() {
            let target_account = match self
                .resource_account_resolver
                .resolve_target_account(get_request.account.as_ref())
                .await
            {
                Ok(target_account) => target_account,
                Err(error) => {
                    problems.push(BatchRequestProblem {
                        request_index,
                        error: error.into(),
                    });
                    continue;
                }
            };

            let uid = match self
                .resolve_resource_uid::<GetResourceError>(
                    &get_request.kind,
                    &target_account.id,
                    &get_request.resource_ref,
                )
                .await
            {
                Ok(uid) => uid,
                Err(error) => {
                    problems.push(BatchRequestProblem {
                        request_index,
                        error,
                    });
                    continue;
                }
            };

            uid_groups
                .entry(target_account.id.clone())
                .or_insert_with(Vec::new)
                .push((request_index, get_request, uid, target_account));
        }

        BatchMaterializationGroups {
            uid_groups,
            problems,
        }
    }

    fn validate_materialized_snapshot(
        snapshot: ResourceSnapshot,
        request: &GetResourceRequest,
        descriptors_by_key: &HashMap<(String, String), String>,
    ) -> Result<ResourceSnapshot, GetResourceError> {
        if snapshot.kind != request.kind {
            return Err(ResourceKindMismatchError {
                uid: snapshot.uid,
                expected_kind: request.kind.clone(),
                actual_kind: snapshot.kind,
            }
            .into());
        }

        if let Some(expected_api_version) = request.api_version.as_ref()
            && snapshot.api_version != *expected_api_version
        {
            return Err(ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.clone(),
                actual_api_version: snapshot.api_version,
            }
            .into());
        }

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

    async fn group_batch_identity_requests(
        &self,
        requests: Vec<GetResourceRequest>,
    ) -> BatchIdentityLookupGroups {
        let mut uid_groups = HashMap::new();
        let mut name_groups = HashMap::new();
        let mut problems = Vec::new();

        for (request_index, get_request) in requests.into_iter().enumerate() {
            let target_account = match self
                .resource_account_resolver
                .resolve_target_account(get_request.account.as_ref())
                .await
            {
                Ok(target_account) => target_account,
                Err(error) => {
                    problems.push(BatchRequestProblem {
                        request_index,
                        error: error.into(),
                    });
                    continue;
                }
            };

            match &get_request.resource_ref {
                ResourceRef::ById(uid) => {
                    let uid = *uid;
                    uid_groups
                        .entry(target_account.id)
                        .or_insert_with(Vec::new)
                        .push((request_index, get_request, uid));
                }
                ResourceRef::ByName(name) => {
                    let name = name.clone();
                    name_groups
                        .entry((target_account.id, get_request.kind.clone()))
                        .or_insert_with(Vec::new)
                        .push((request_index, get_request, name));
                }
            }
        }

        BatchIdentityLookupGroups {
            uid_groups,
            name_groups,
            problems,
        }
    }

    async fn resolve_uid_identity_groups(
        &self,
        uid_groups: BatchUidIdentityGroups,
        descriptors_by_key: &HashMap<(String, String), String>,
        identities: &mut Vec<IndexedResourceIdentity>,
        problems: &mut Vec<BatchRequestProblem<GetResourceError>>,
    ) -> Result<(), GetResourceError> {
        for (account_id, entries) in uid_groups {
            let uids = entries.iter().map(|(_, _, uid)| *uid).collect::<Vec<_>>();

            let rows_by_uid = self
                .generic_resource_query_service
                .find_resource_identities_by_uids(&account_id, &uids)
                .await?
                .into_iter()
                .map(|row| (row.uid, row))
                .collect::<HashMap<_, _>>();

            for (request_index, get_request, uid) in entries {
                Self::push_batch_identity_result(
                    request_index,
                    rows_by_uid.get(uid.as_ref()).cloned().ok_or_else(|| {
                        GetResourceError::UIDNotFound(ResourceUIDNotFoundError(uid))
                    }),
                    &get_request,
                    descriptors_by_key,
                    identities,
                    problems,
                );
            }
        }

        Ok(())
    }

    async fn resolve_name_identity_groups(
        &self,
        name_groups: BatchNameIdentityGroups,
        descriptors_by_key: &HashMap<(String, String), String>,
        identities: &mut Vec<IndexedResourceIdentity>,
        problems: &mut Vec<BatchRequestProblem<GetResourceError>>,
    ) -> Result<(), GetResourceError> {
        for ((account_id, kind), entries) in name_groups {
            let names = entries
                .iter()
                .map(|(_, _, name)| name.clone())
                .collect::<Vec<_>>();

            let rows_by_name = self
                .generic_resource_query_service
                .find_resource_identities_by_names(&account_id, &kind, &names)
                .await?
                .into_iter()
                .map(|row| (row.name.clone(), row))
                .collect::<HashMap<_, _>>();

            for (request_index, get_request, name) in entries {
                Self::push_batch_identity_result(
                    request_index,
                    rows_by_name.get(&name).cloned().ok_or_else(|| {
                        GetResourceError::NameNotFound(ResourceNameNotFoundError {
                            kind: get_request.kind.clone(),
                            name,
                        })
                    }),
                    &get_request,
                    descriptors_by_key,
                    identities,
                    problems,
                );
            }
        }

        Ok(())
    }

    fn push_batch_identity_result(
        request_index: usize,
        row_result: Result<ResourceIdentityRow, GetResourceError>,
        request: &GetResourceRequest,
        descriptors_by_key: &HashMap<(String, String), String>,
        identities: &mut Vec<IndexedResourceIdentity>,
        problems: &mut Vec<BatchRequestProblem<GetResourceError>>,
    ) {
        match row_result
            .and_then(|row| Self::validate_identity_row(row, request, descriptors_by_key))
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

    fn resource_identity_from_snapshot<E>(
        &self,
        snapshot: ResourceSnapshot,
    ) -> Result<ResourceIdentityView, E>
    where
        E: From<UnsupportedResourceDescriptorError>,
    {
        let canonical_kind_name = self
            .list_resource_kind_descriptors()
            .into_iter()
            .find(|descriptor| {
                descriptor.kind == snapshot.kind && descriptor.api_version == snapshot.api_version
            })
            .ok_or_else(|| UnsupportedResourceDescriptorError::NotFound {
                kind: snapshot.kind.clone(),
                api_version: snapshot.api_version.clone(),
            })?
            .name;

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
        let canonical_kind_name = descriptors_by_key
            .get(&(row.kind.clone(), row.api_version.clone()))
            .ok_or_else(|| UnsupportedResourceDescriptorError::NotFound {
                kind: row.kind.clone(),
                api_version: row.api_version.clone(),
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
        if row.kind != request.kind {
            return Err(ResourceKindMismatchError {
                uid: ResourceUID::new(row.uid),
                expected_kind: request.kind.clone(),
                actual_kind: row.kind,
            }
            .into());
        }

        if let Some(expected_api_version) = request.api_version.as_ref()
            && row.api_version != *expected_api_version
        {
            return Err(ResourceAPIVersionMismatchError {
                expected_api_version: expected_api_version.clone(),
                actual_api_version: row.api_version,
            }
            .into());
        }

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
        match resource_ref {
            ResourceRef::ById(uid) => Ok(*uid),
            ResourceRef::ByName(name) => self
                .generic_resource_query_service
                .find_resource_uid_by_name(account_id, kind, name)
                .await?
                .ok_or_else(|| {
                    ResourceNameNotFoundError {
                        kind: kind.to_string(),
                        name: name.clone(),
                    }
                    .into()
                }),
        }
    }

    fn parse_manifest(
        &self,
        format: ResourceManifestFormat,
        manifest: &str,
    ) -> Result<ResourceManifest, ParseResourceManifestError> {
        match format {
            ResourceManifestFormat::Json => {
                serde_json::from_str(manifest).map_err(|e| ParseResourceManifestError {
                    message: format!("input is not valid JSON: {e}"),
                })
            }
            ResourceManifestFormat::Yaml => {
                serde_yaml::from_str(manifest).map_err(|e| ParseResourceManifestError {
                    message: format!("input is not valid YAML: {e}"),
                })
            }
        }
    }

    fn make_metadata_input(
        &self,
        manifest: &ResourceManifest,
        target_account: &ResolvedAccount,
    ) -> Result<ResourceMetadataInput, ApplyManifestError> {
        ResourceMetadataInput::try_new(
            target_account.id.clone(),
            manifest.metadata.name.clone(),
            manifest.metadata.description.clone(),
            manifest.metadata.labels.clone(),
            manifest.metadata.annotations.clone(),
        )
        .map_err(Into::into)
    }

    fn collect_manifest_metadata_warnings(manifest: &ResourceManifest) -> Vec<ResourceWarning> {
        let mut warnings = Vec::new();

        if manifest
            .metadata
            .description
            .as_ref()
            .is_none_or(|description| description.trim().is_empty())
        {
            warnings.push(ResourceWarning {
                code: Self::WARNING_CODE_MISSING_DESCRIPTION,
                path: Some("metadata.description".to_string()),
                message: "Resource has no description".to_string(),
            });
        }

        warnings
    }

    fn resource_view_to_manifest(view: ResourceView) -> ResourceManifest {
        let ResourceView {
            kind,
            api_version,
            account,
            metadata,
            spec,
            ..
        } = view;

        ResourceManifest {
            api_version,
            kind,
            metadata: kamu_resources::ResourceManifestMetadata {
                uid: Some(metadata.uid),
                account: Some(kamu_resources::ResourceManifestAccount {
                    id: Some(account.id),
                    name: account.name.map(|name| name.to_string()),
                }),
                name: metadata.name,
                description: metadata.description,
                labels: metadata.labels.into_iter().collect(),
                annotations: metadata.annotations.into_iter().collect(),
            },
            spec,
        }
    }

    fn serialize_manifest(
        manifest: &ResourceManifest,
        format: ResourceManifestFormat,
    ) -> Result<String, RenderResourceManifestError> {
        match format {
            ResourceManifestFormat::Json => serde_json::to_string_pretty(manifest)
                .int_err()
                .map_err(Into::into),
            ResourceManifestFormat::Yaml => serde_yaml::to_string(manifest)
                .int_err()
                .map_err(Into::into),
        }
    }

    async fn resolve_snapshot_for_kind(
        &self,
        kind: &str,
        account_id: &odf::AccountID,
        uid: &ResourceUID,
    ) -> Result<ResourceSnapshot, GetResourceError> {
        let Some(snapshot) = self
            .generic_resource_query_service
            .get_snapshot_by_uid(uid)
            .await?
        else {
            return Err(ResourceUIDNotFoundError(*uid).into());
        };

        if snapshot.kind != kind {
            return Err(ResourceKindMismatchError {
                uid: *uid,
                expected_kind: kind.to_string(),
                actual_kind: snapshot.kind,
            }
            .into());
        }

        if snapshot.metadata.account != *account_id {
            return Err(ResourceUIDNotFoundError(*uid).into());
        }

        Ok(snapshot)
    }

    fn ensure_snapshot_matches_kind(
        &self,
        kind: &str,
        account_id: &odf::AccountID,
        uid: ResourceUID,
        maybe_snapshot: Option<ResourceSnapshot>,
    ) -> Result<ResourceSnapshot, DeleteResourceError> {
        let Some(snapshot) = maybe_snapshot else {
            return Err(ResourceUIDNotFoundError(uid).into());
        };

        if snapshot.kind != kind {
            return Err(ResourceKindMismatchError {
                uid,
                expected_kind: kind.to_string(),
                actual_kind: snapshot.kind,
            }
            .into());
        }

        if snapshot.metadata.account != *account_id {
            return Err(ResourceUIDNotFoundError(uid).into());
        }

        Ok(snapshot)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type BatchUidIdentityGroups =
    HashMap<odf::AccountID, Vec<(usize, GetResourceRequest, ResourceUID)>>;
type BatchNameIdentityGroups =
    HashMap<(odf::AccountID, String), Vec<(usize, GetResourceRequest, ResourceName)>>;
type BatchMaterializationUidGroups =
    HashMap<odf::AccountID, Vec<(usize, GetResourceRequest, ResourceUID, ResolvedAccount)>>;

struct BatchIdentityLookupGroups {
    uid_groups: BatchUidIdentityGroups,
    name_groups: BatchNameIdentityGroups,
    problems: Vec<BatchRequestProblem<GetResourceError>>,
}

struct BatchMaterializationGroups {
    uid_groups: BatchMaterializationUidGroups,
    problems: Vec<BatchRequestProblem<GetResourceError>>,
}

struct IndexedResourceIdentity {
    request_index: usize,
    identity: ResourceIdentityView,
}

struct IndexedResourceView {
    request_index: usize,
    resource: ResourceView,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
