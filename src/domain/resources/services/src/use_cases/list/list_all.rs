// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use dill::{component, interface};

use crate::domain::{AllResourcesQueryService, ListAllResourcesUseCase, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ListAllResourcesUseCaseHelper<'a> {
    all_resources_query_service: &'a dyn AllResourcesQueryService,
}

impl<'a> ListAllResourcesUseCaseHelper<'a> {
    pub fn new(all_resources_query_service: &'a dyn AllResourcesQueryService) -> Self {
        Self {
            all_resources_query_service,
        }
    }

    pub async fn execute(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, internal_error::InternalError> {
        self.all_resources_query_service
            .list_all_snapshots(account_id, pagination)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ListAllResourcesUseCase)]
pub struct ListAllResourcesUseCaseImpl {
    all_resources_query_service: std::sync::Arc<dyn AllResourcesQueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ListAllResourcesUseCase for ListAllResourcesUseCaseImpl {
    async fn execute(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, internal_error::InternalError> {
        let helper = ListAllResourcesUseCaseHelper::new(self.all_resources_query_service.as_ref());

        helper.execute(account_id, pagination).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
