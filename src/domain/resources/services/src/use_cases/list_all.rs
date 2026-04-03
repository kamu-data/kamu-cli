// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;

use crate::domain::{GenericResourceQueryService, ListAllResourcesUseCase, ResourceSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn ListAllResourcesUseCase)]
pub struct ListAllResourcesUseCaseImpl {
    generic_resource_query_service: std::sync::Arc<dyn GenericResourceQueryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ListAllResourcesUseCase for ListAllResourcesUseCaseImpl {
    async fn execute(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, internal_error::InternalError> {
        self.generic_resource_query_service
            .list_all_snapshots(account_id, pagination)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
