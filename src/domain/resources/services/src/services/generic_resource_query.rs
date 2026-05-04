// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::domain::{
    FindOwnedResourceError,
    GenericResourceQueryService,
    ResourceNotOwnedByAccountError,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSummaryRow,
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
    async fn allocate_uid(&self) -> Result<crate::domain::ResourceUID, InternalError> {
        self.resource_repository.new_resource_uid().await
    }

    async fn find_resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &crate::domain::ResourceName,
    ) -> Result<Option<crate::domain::ResourceUID>, InternalError> {
        self.resource_repository
            .find_resource_uid_by_name(account_id, kind, name)
            .await
    }

    async fn find_resource_identities_by_uids(
        &self,
        account_id: &odf::AccountID,
        uids: &[crate::domain::ResourceUID],
    ) -> Result<Vec<crate::domain::ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .find_resource_identities_by_uids(account_id, uids)
            .await
    }

    async fn find_resource_identities_by_names(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        names: &[crate::domain::ResourceName],
    ) -> Result<Vec<crate::domain::ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .find_resource_identities_by_names(account_id, kind, names)
            .await
    }

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        kind: &'static str,
        uid: crate::domain::ResourceUID,
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

        Ok(Some(resource_snapshot))
    }

    async fn get_snapshot_by_uid(
        &self,
        uid: &crate::domain::ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        self.resource_repository
            .find_resource_snapshot_by_uid(uid)
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
