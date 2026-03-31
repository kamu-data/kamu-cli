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

use crate::domain::{AllResourcesQueryService, ResourceRepository, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn AllResourcesQueryService)]
pub struct AllResourcesQueryServiceImpl {
    resource_repository: Arc<dyn ResourceRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AllResourcesQueryService for AllResourcesQueryServiceImpl {
    async fn get_snapshot_by_uid(
        &self,
        uid: &crate::domain::ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        self.resource_repository
            .find_resource_snapshot_by_uid(uid)
            .await
    }

    async fn get_snapshots_by_uids(
        &self,
        uids: &[crate::domain::ResourceUID],
    ) -> Result<Vec<Option<ResourceSnapshot>>, InternalError> {
        let mut snapshots = Vec::with_capacity(uids.len());

        // TODO: bulk query in repository instead of one by one
        for uid in uids {
            snapshots.push(
                self.resource_repository
                    .find_resource_snapshot_by_uid(uid)
                    .await?,
            );
        }

        Ok(snapshots)
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
