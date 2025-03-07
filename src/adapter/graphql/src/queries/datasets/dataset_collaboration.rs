// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetCollaboration<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetCollaboration<'a> {
    const DEFAULT_RESULTS_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(dataset_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state: dataset_state,
        }
    }

    /// Accounts (and their roles) that have access to the dataset
    #[tracing::instrument(level = "info", name = DatasetCollaboration_account_roles, skip_all)]
    async fn account_roles(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AccountWithRoleConnection> {
        self.dataset_request_state
            .check_dataset_maintain_access(ctx)
            .await?;

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        // TODO: Private Datasets: implementation

        Ok(AccountWithRoleConnection::new(vec![], page, per_page, 0))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    AccountWithRole,
    AccountWithRoleConnection,
    AccountWithRoleEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
