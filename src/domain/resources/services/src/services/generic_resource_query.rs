// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::domain::{
    FindOwnedResourceError,
    FindOwnedSnapshotsOutcome,
    GenericResourceQueryService,
    ResourceIdentityRow,
    ResourceName,
    ResourceNotOwnedByAccountError,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSummaryRow,
    ResourceTypeMismatchError,
    ResourceUID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn GenericResourceQueryService)]
pub struct GenericResourceQueryServiceImpl {
    resource_repository: Arc<dyn ResourceRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl GenericResourceQueryService for GenericResourceQueryServiceImpl {
    async fn allocate_uid(&self) -> Result<ResourceUID, InternalError> {
        self.resource_repository.new_resource_uid().await
    }

    async fn find_resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceUID>, InternalError> {
        self.resource_repository
            .find_resource_uid_by_name(account_id, kind, name)
            .await
    }

    async fn find_resource_identities_by_uids(
        &self,
        account_id: &odf::AccountID,
        uids: &[ResourceUID],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .find_resource_identities_by_uids(account_id, uids)
            .await
    }

    async fn find_resource_identities_by_names(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        names: &[ResourceName],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .find_resource_identities_by_names(account_id, kind, names)
            .await
    }

    async fn search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        kinds: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .search_resource_identities(account_id, kinds, exact_names, name_pattern, pagination)
            .await
    }

    async fn count_search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        kinds: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
    ) -> Result<usize, InternalError> {
        self.resource_repository
            .count_search_resource_identities(account_id, kinds, exact_names, name_pattern)
            .await
    }

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        api_version: &'static str,
        uid: ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError> {
        let query = ResourceRawEventQuery {
            kind: kind.to_string(),
            uid,
        };

        let Some(resource_snapshot) = self
            .resource_repository
            .find_resource_snapshot(&query)
            .await?
        else {
            return Ok(None);
        };

        if resource_snapshot.metadata.account != *account_id {
            return Err(odf::AccessError::Unauthorized(
                ResourceNotOwnedByAccountError {
                    uid: resource_snapshot.uid,
                    resource_type: kind,
                }
                .into(),
            )
            .into());
        }

        if resource_snapshot.api_version != api_version {
            return Err(ResourceTypeMismatchError {
                uid: resource_snapshot.uid,
                expected_kind: kind.to_string(),
                expected_api_version: api_version.to_string(),
                actual_kind: resource_snapshot.kind,
                actual_api_version: resource_snapshot.api_version,
            }
            .into());
        }

        Ok(Some(resource_snapshot))
    }

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        api_version: &'static str,
        uids: &[ResourceUID],
    ) -> Result<FindOwnedSnapshotsOutcome, InternalError> {
        let owned_any_kind: HashMap<ResourceUID, ResourceSnapshot> = self
            .resource_repository
            .find_resource_snapshots_by_uids(account_id, uids)
            .await?
            .into_iter()
            .map(|snapshot| (snapshot.uid, snapshot))
            .collect();

        let kind_mismatch: Vec<(ResourceUID, String)> = owned_any_kind
            .values()
            .filter(|snapshot| snapshot.kind != kind)
            .map(|snapshot| (snapshot.uid, snapshot.kind.clone()))
            .collect();

        let api_version_mismatch: Vec<(ResourceUID, String)> = owned_any_kind
            .values()
            .filter(|snapshot| snapshot.kind == kind && snapshot.api_version != api_version)
            .map(|snapshot| (snapshot.uid, snapshot.api_version.clone()))
            .collect();

        let missing_uids: Vec<ResourceUID> = uids
            .iter()
            .copied()
            .filter(|uid| !owned_any_kind.contains_key(uid))
            .collect();

        let access_denied_snapshots = self
            .resource_repository
            .find_resource_snapshots_by_kind_and_uids(kind, &missing_uids)
            .await?;
        let access_denied_uids: std::collections::HashSet<ResourceUID> =
            access_denied_snapshots.iter().map(|s| s.uid).collect();

        let access_denied: Vec<ResourceUID> = access_denied_uids.iter().copied().collect();

        let not_found: Vec<ResourceUID> = missing_uids
            .into_iter()
            .filter(|uid| !access_denied_uids.contains(uid))
            .collect();

        let found: Vec<ResourceSnapshot> = owned_any_kind
            .into_values()
            .filter(|snapshot| snapshot.kind == kind && snapshot.api_version == api_version)
            .collect();

        Ok(FindOwnedSnapshotsOutcome {
            found,
            not_found,
            kind_mismatch,
            api_version_mismatch,
            access_denied,
        })
    }

    async fn get_snapshot_by_uid(
        &self,
        uid: &ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        self.resource_repository
            .find_resource_snapshot_by_uid(uid)
            .await
    }

    async fn find_snapshots_by_uids(
        &self,
        account_id: &odf::AccountID,
        uids: &[ResourceUID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        self.resource_repository
            .find_resource_snapshots_by_uids(account_id, uids)
            .await
    }

    async fn list_snapshots_by_kind(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        let mut resource_snapshots_stream = self
            .resource_repository
            .list_resource_snapshots_by_kind(account_id, kind, pagination);

        use tokio_stream::StreamExt;

        let mut resource_snapshots = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            resource_snapshots.push(resource_snapshot?);
        }

        Ok(resource_snapshots)
    }

    async fn list_all_snapshots(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        let mut resource_snapshots_stream = self
            .resource_repository
            .list_all_resource_snapshots(account_id, pagination);

        use tokio_stream::StreamExt;

        let mut resource_snapshots = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            resource_snapshots.push(resource_snapshot?);
        }

        Ok(resource_snapshots)
    }

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError> {
        self.resource_repository
            .summarize_resources(account_id)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
