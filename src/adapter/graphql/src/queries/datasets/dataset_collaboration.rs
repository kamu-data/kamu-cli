// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::AccountService;

use crate::prelude::*;
use crate::queries::{Account, DatasetRequestState};
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetCollaboration<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetCollaboration<'a> {
    const DEFAULT_RESULTS_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub async fn new_with_access_check(
        ctx: &Context<'_>,
        dataset_request_state: &'a DatasetRequestState,
    ) -> Result<Self> {
        utils::check_dataset_maintain_access(ctx, dataset_request_state).await?;

        Ok(Self {
            dataset_request_state,
        })
    }

    /// Accounts (and their roles) that have access to the dataset
    #[tracing::instrument(level = "info", name = DatasetCollaboration_account_roles, skip_all, fields(?page, ?per_page))]
    async fn account_roles(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AccountWithRoleConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_RESULTS_PER_PAGE);

        let account_service = from_catalog_n!(ctx, dyn AccountService);

        let authorized_accounts = self.dataset_request_state.authorized_accounts(ctx).await?;
        let authorized_account_ids = authorized_accounts
            .iter()
            .map(|a| &a.account_id)
            .collect::<Vec<_>>();
        let mut authorized_account_map = account_service
            .get_account_map(&authorized_account_ids)
            .await
            .int_err()?;

        let nodes = authorized_accounts
            .iter()
            .map(|authorized_account| {
                // Safety: The map guarantees the pair presence
                let account_model = authorized_account_map
                    .remove(&authorized_account.account_id)
                    .unwrap();

                AccountWithRole {
                    account: Account::from_account(account_model),
                    role: authorized_account.role.into(),
                }
            })
            .collect::<Vec<_>>();
        let total = nodes.len();
        let page_nodes = nodes
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .collect::<Vec<_>>();

        Ok(AccountWithRoleConnection::new(
            page_nodes, page, per_page, total,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    AccountWithRole,
    AccountWithRoleConnection,
    AccountWithRoleEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
