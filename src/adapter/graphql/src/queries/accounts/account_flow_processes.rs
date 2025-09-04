// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account as AccountEntity;

use crate::prelude::*;
use crate::queries::{
    Account,
    Dataset,
    DatasetFlowProcesses,
    DatasetRequestState,
    DatasetRequestStateWithOwner,
    list_accessible_datasets_with_flows,
};
use crate::scalars::FlowProcessGroupRollup;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowProcesses<'a> {
    account: &'a AccountEntity,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> AccountFlowProcesses<'a> {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account: &'a AccountEntity) -> Self {
        Self { account }
    }

    #[allow(clippy::unused_async)]
    pub async fn primary_rollup(&self) -> FlowProcessGroupRollup {
        // TODO: Implement actual logic

        let active = 0;
        let failing = 0;
        let paused = 0;
        let stopped = 0;
        let total = active + failing + paused + stopped;
        let worst_consecutive_failures = 0;

        FlowProcessGroupRollup {
            total,
            active,
            failing,
            paused,
            stopped,
            worst_consecutive_failures,
        }
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_dataset_cards, skip_all, fields(?page, ?per_page))]
    async fn dataset_cards(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AccountFlowProcessDatasetCardConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let (dataset_handles, total_datasets_with_flows) =
            list_accessible_datasets_with_flows(ctx, &self.account.id, page, per_page).await?;

        let account = Account::new(
            self.account.id.clone().into(),
            self.account.account_name.clone().into(),
        );

        let dataset_cards: Vec<_> = dataset_handles
            .into_iter()
            .map(|hdl| AccountFlowProcessDatasetCard::new(account.clone(), hdl))
            .collect();

        Ok(AccountFlowProcessDatasetCardConnection::new(
            dataset_cards,
            page,
            per_page,
            total_datasets_with_flows,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowProcessDatasetCard {
    dataset_request_state: DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl AccountFlowProcessDatasetCard {
    #[graphql(skip)]
    pub fn new(account: Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            dataset_request_state: DatasetRequestState::new(dataset_handle).with_owner(account),
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn dataset(&self) -> Dataset {
        Dataset::new_access_checked(
            self.dataset_request_state.owner().clone(),
            self.dataset_request_state.dataset_handle().clone(),
        )
    }

    #[allow(clippy::unused_async)]
    pub async fn processes(&self) -> DatasetFlowProcesses {
        DatasetFlowProcesses::new(&self.dataset_request_state)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    AccountFlowProcessDatasetCard,
    AccountFlowProcessDatasetCardConnection,
    AccountFlowProcessDatasetCardEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
