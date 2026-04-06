// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyManifestPlanningDecision,
    GenericResourceQueryService,
    ResourceAPIVersionMismatchError,
    ResourceCrudDispatcherApplyRequest,
    ResourceCrudDispatcherDeleteRequest,
    ResourceCrudDispatcherGetRequest,
    ResourceCrudDispatcherListRequest,
    ResourceManifest,
    ResourceMetadataInput,
    ResourceNameNotFoundError,
    ResourceSnapshot,
    ResourceSummaryView,
    ResourceUID,
    ResourceUIDNotFoundError,
    ResourceView,
};
use kamu_resources_services::{get_resource_crud_dispatcher, get_resource_crud_dispatcher_by_kind};

use crate::{
    ApplyManifestError,
    ApplyManifestRequest,
    DeleteResourceError,
    DeleteResourceRequest,
    GetResourceError,
    GetResourceRef,
    GetResourceRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ParseResourceManifestError,
    RenderResourceManifestError,
    RenderResourceManifestRequest,
    RenderResourceManifestResult,
    ResolvedAccount,
    ResourceAccountResolver,
    ResourceFacade,
    ResourceKindMismatchError,
    ResourceManifestFormat,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn ResourceFacade)]
pub struct ResourceFacadeImpl {
    catalog: dill::Catalog,
    resource_account_resolver: Arc<dyn ResourceAccountResolver>,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for ResourceFacadeImpl {
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

        let plan = dispatcher
            .plan_apply(ResourceCrudDispatcherApplyRequest {
                uid: manifest.metadata.uid,
                metadata,
                spec: manifest.spec,
            })
            .await?;

        Ok(match plan {
            ApplyManifestPlanningDecision::Planned(mut plan) => {
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

        let result = dispatcher
            .apply(ResourceCrudDispatcherApplyRequest {
                uid: manifest.metadata.uid,
                metadata,
                spec: manifest.spec,
            })
            .await?;

        Ok(match result {
            ApplyManifestApplicationDecision::Applied(mut result) => {
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
                account_id: target_account.id.clone(),
                pagination: request.pagination,
            })
            .await
            .map_err(Into::into)
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
            .list_all_snapshots(target_account.id.clone(), request.pagination)
            .await?;

        Ok(snapshots.into_iter().map(Into::into).collect())
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
                account_id: target_account.id.clone(),
                uids: vec![uid],
            })
            .await?;

        Ok(uid)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceFacadeImpl {
    async fn resolve_resource_uid<E>(
        &self,
        kind: &str,
        account_id: &odf::AccountID,
        resource_ref: &GetResourceRef,
    ) -> Result<ResourceUID, E>
    where
        E: From<InternalError> + From<ResourceNameNotFoundError>,
    {
        match resource_ref {
            GetResourceRef::ById(uid) => Ok(*uid),
            GetResourceRef::ByName(name) => self
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
