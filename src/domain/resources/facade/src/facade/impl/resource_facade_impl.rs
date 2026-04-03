// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use kamu_resources::{
    ApplyManifestResult,
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
    DeleteResourcesError,
    DeleteResourcesRequest,
    GetResourceError,
    GetResourceRef,
    GetResourceRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ParseResourceManifestError,
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
    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<ApplyManifestResult, ApplyManifestError> {
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

        let mut result = dispatcher
            .apply(ResourceCrudDispatcherApplyRequest {
                uid: manifest.metadata.uid,
                metadata,
                spec: manifest.spec,
            })
            .await?;

        result.resource = self
            .resource_account_resolver
            .hydrate_resource_view_account(result.resource, Some(&target_account))
            .await?;

        Ok(result)
    }

    async fn get(&self, request: GetResourceRequest) -> Result<ResourceView, GetResourceError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let uid = self
            .resolve_resource_uid(&request.kind, &target_account.id, &request.resource_ref)
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

    async fn delete(&self, request: DeleteResourcesRequest) -> Result<(), DeleteResourcesError> {
        let target_account = self
            .resource_account_resolver
            .resolve_target_account(request.account.as_ref())
            .await?;

        let snapshots = self
            .generic_resource_query_service
            .get_snapshots_by_uids(&request.uids)
            .await?;

        let mut uids_by_api_version: BTreeMap<String, Vec<ResourceUID>> = BTreeMap::new();
        for (uid, maybe_snapshot) in std::iter::zip(request.uids, snapshots) {
            let snapshot = self.ensure_snapshot_matches_kind(
                &request.kind,
                &target_account.id,
                uid,
                maybe_snapshot,
            )?;
            uids_by_api_version
                .entry(snapshot.api_version)
                .or_default()
                .push(uid);
        }

        for (api_version, uids) in uids_by_api_version {
            let dispatcher = get_resource_crud_dispatcher::<DeleteResourcesError>(
                &self.catalog,
                &request.kind,
                &api_version,
            )?;
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: target_account.id.clone(),
                    uids,
                })
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceFacadeImpl {
    async fn resolve_resource_uid(
        &self,
        kind: &str,
        account_id: &odf::AccountID,
        resource_ref: &GetResourceRef,
    ) -> Result<ResourceUID, GetResourceError> {
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
    ) -> Result<ResourceSnapshot, DeleteResourcesError> {
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
