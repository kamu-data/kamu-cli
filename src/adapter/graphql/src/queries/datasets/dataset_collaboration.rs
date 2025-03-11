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

        let (rebac_service, account_service) =
            from_catalog_n!(ctx, dyn RebacService, dyn AccountService);

        let account_id_relation_tuples = rebac_service
            .get_accounts_dataset_relations(&self.dataset_request_state.dataset_handle.id)
            .await
            .int_err()?;
        let account_ids = account_id_relation_tuples
            .iter()
            .map(|t| t.0.clone())
            .collect::<Vec<_>>();
        let mut account_map = account_service
            .get_account_map(account_ids)
            .await
            .int_err()?;

        let nodes = account_id_relation_tuples
            .into_iter()
            .map(|(account_id, role)| {
                // Safety: The map guarantees the pair presence
                let account_model = account_map.remove(&account_id).unwrap();

                AccountWithRole {
                    account: Account::from_account(account_model),
                    role: role.into(),
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
