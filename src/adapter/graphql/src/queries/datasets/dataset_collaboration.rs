// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::AccountService;
use kamu_auth_rebac::RebacService;

use crate::prelude::*;
use crate::queries::{Account, DatasetRequestState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetCollaboration<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetCollaboration<'a> {
    const DEFAULT_RESULTS_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
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

        let (rebac_service, account_service) =
            from_catalog_n!(ctx, dyn RebacService, dyn AccountService);

        let authorized_accounts = rebac_service
            .get_authorized_accounts(&self.dataset_request_state.dataset_handle().id)
            .await
            .int_err()?;
        let authorized_account_ids = authorized_accounts
            .iter()
            .map(|a| a.account_id.clone())
            .collect::<Vec<_>>();
        let mut authorized_account_map = account_service
            .get_account_map(&authorized_account_ids)
            .await
            .int_err()?;

        let nodes = authorized_accounts
            .into_iter()
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

        Ok(AccountWithRoleConnection::new(nodes, page, per_page, total))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    AccountWithRole,
    AccountWithRoleConnection,
    AccountWithRoleEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
