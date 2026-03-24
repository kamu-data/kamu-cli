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
use dill::{component, interface};
use tokio_stream::StreamExt;

use crate::domain::{ListAllResourcesUseCase, ResourceRepository, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ListAllResourcesUseCase)]
pub struct ListAllResourcesUseCaseImpl {
    resource_repository: Arc<dyn ResourceRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ListAllResourcesUseCase for ListAllResourcesUseCaseImpl {
    async fn execute(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, internal_error::InternalError> {
        let mut resource_snapshots_stream = self
            .resource_repository
            .list_all_resource_snapshots(account_id, pagination);

        let mut resource_snapshots = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            resource_snapshots.push(resource_snapshot?);
        }

        Ok(resource_snapshots)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
