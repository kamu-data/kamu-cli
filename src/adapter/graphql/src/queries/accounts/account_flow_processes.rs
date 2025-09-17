// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::HashMap;

use database_common::PaginationOpts;
use kamu_accounts::Account as AccountEntity;
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_core::DatasetRegistry;
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
use kamu_flow_system::{
    FlowProcessListFilter,
    FlowProcessOrder,
    FlowProcessState,
    FlowProcessStateQuery,
};

use crate::prelude::*;
use crate::queries::{
    Account,
    Dataset,
    DatasetRequestState,
    DatasetRequestStateWithOwner,
    FlowProcess,
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
    pub async fn primary_rollup(&self, ctx: &Context<'_>) -> Result<FlowProcessGroupRollup> {
        let (dataset_entry_service, flow_process_state_query) =
            from_catalog_n!(ctx, dyn DatasetEntryService, dyn FlowProcessStateQuery);

        let owned_dataset_ids = dataset_entry_service
            .get_owned_dataset_ids(&self.account.id)
            .await
            .int_err()?;

        let owned_dataset_id_refs = owned_dataset_ids.iter().collect::<Vec<_>>();

        let scope_query =
            FlowScopeDataset::query_for_multiple_datasets_only(&owned_dataset_id_refs);

        let rollup = flow_process_state_query
            .rollup_by_scope(
                scope_query,
                Some(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]),
                None,
            )
            .await?;

        Ok(rollup.into())
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_dataset_cards, skip_all, fields(?page, ?per_page))]
    async fn dataset_cards(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AccountFlowProcessDatasetCardConnection> {
        let (dataset_entry_service, dataset_registry, flow_process_state_query) = from_catalog_n!(
            ctx,
            dyn DatasetEntryService,
            dyn DatasetRegistry,
            dyn FlowProcessStateQuery
        );

        let owned_dataset_ids = dataset_entry_service
            .get_owned_dataset_ids(&self.account.id)
            .await
            .int_err()?;

        let owned_dataset_id_refs = owned_dataset_ids.iter().collect::<Vec<_>>();

        let scope_query =
            FlowScopeDataset::query_for_multiple_datasets_only(&owned_dataset_id_refs);

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let listing = flow_process_state_query
            .list_processes(
                FlowProcessListFilter::for_scope(scope_query)
                    .for_flow_types(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]),
                FlowProcessOrder::recent(),
                Some(PaginationOpts::from_page(page, per_page)),
            )
            .await?;

        // The way we queried for processes guarantees the IDs won't duplicate
        let matched_dataset_ids = listing
            .processes
            .iter()
            .map(|proc| FlowScopeDataset::new(&proc.flow_binding().scope).dataset_id())
            .map(Cow::Owned)
            .collect::<Vec<_>>();

        let dataset_handles_resolution = dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&matched_dataset_ids)
            .await
            .int_err()?;

        if !dataset_handles_resolution.unresolved_datasets.is_empty() {
            tracing::error!(
                "Some datasets with flows could not be resolved: {:?}",
                dataset_handles_resolution.unresolved_datasets
            );
            unreachable!("Inconsistent state: all datasets with flows must exist");
        }

        let dataset_handles_by_ids = dataset_handles_resolution
            .resolved_handles
            .into_iter()
            .map(|hdl| (hdl.id.clone(), hdl))
            .collect::<HashMap<_, _>>();

        let account = Account::new(
            self.account.id.clone().into(),
            self.account.account_name.clone().into(),
        );

        let dataset_cards: Vec<_> = listing
            .processes
            .into_iter()
            .map(|process_state| {
                let dataset_id =
                    FlowScopeDataset::new(&process_state.flow_binding().scope).dataset_id();
                let hdl = dataset_handles_by_ids
                    .get(&dataset_id)
                    .expect("Inconsistent state: all datasets with flows must exist")
                    .clone();

                AccountFlowProcessDatasetCard::new(account.clone(), hdl, process_state)
            })
            .collect();

        Ok(AccountFlowProcessDatasetCardConnection::new(
            dataset_cards,
            page,
            per_page,
            listing.total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowProcessDatasetCard {
    dataset_request_state: DatasetRequestStateWithOwner,
    process_state: FlowProcessState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl AccountFlowProcessDatasetCard {
    #[graphql(skip)]
    pub fn new(
        account: Account,
        dataset_handle: odf::DatasetHandle,
        process_state: FlowProcessState,
    ) -> Self {
        Self {
            dataset_request_state: DatasetRequestState::new(dataset_handle).with_owner(account),
            process_state,
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
    pub async fn primary(&self) -> FlowProcess {
        FlowProcess::new(Cow::Borrowed(&self.process_state))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    AccountFlowProcessDatasetCard,
    AccountFlowProcessDatasetCardConnection,
    AccountFlowProcessDatasetCardEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
