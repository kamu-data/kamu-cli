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
use kamu_flow_system as fs;

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
        let scope_query = self.build_scope_query(ctx).await?;

        let flow_process_state_query = from_catalog_n!(ctx, dyn fs::FlowProcessStateQuery);
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
    pub async fn dataset_cards(
        &self,
        ctx: &Context<'_>,
        ordering: Option<FlowProcessOrdering>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AccountFlowProcessDatasetCardConnection> {
        let scope_query = self.build_scope_query(ctx).await?;
        let filter = fs::FlowProcessListFilter::for_scope(scope_query)
            .for_flow_types(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]);

        let ordering = self.convert_ordering(ordering);

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let pagination = PaginationOpts::from_page(page, per_page);

        let flow_process_state_query = from_catalog_n!(ctx, dyn fs::FlowProcessStateQuery);
        let listing = flow_process_state_query
            .list_processes(filter, ordering, Some(pagination))
            .await?;

        let dataset_handles_by_id = self.build_dataset_id_handle_mapping(ctx, &listing).await?;

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
                let hdl = dataset_handles_by_id
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

    #[graphql(skip)]
    async fn build_scope_query(&self, ctx: &Context<'_>) -> Result<fs::FlowScopeQuery> {
        let dataset_entry_service = from_catalog_n!(ctx, dyn DatasetEntryService);

        let owned_dataset_ids = dataset_entry_service
            .get_owned_dataset_ids(&self.account.id)
            .await
            .int_err()?;

        let owned_dataset_id_refs = owned_dataset_ids.iter().collect::<Vec<_>>();

        Ok(FlowScopeDataset::query_for_multiple_datasets_only(
            &owned_dataset_id_refs,
        ))
    }

    #[graphql(skip)]
    fn convert_ordering(&self, ordering: Option<FlowProcessOrdering>) -> fs::FlowProcessOrder {
        ordering
            .map(|ordering| fs::FlowProcessOrder {
                field: ordering.field.into(),
                desc: match ordering.direction {
                    OrderingDirection::Asc => false,
                    OrderingDirection::Desc => true,
                },
            })
            .unwrap_or_else(fs::FlowProcessOrder::recent)
    }

    #[graphql(skip)]
    async fn build_dataset_id_handle_mapping(
        &self,
        ctx: &Context<'_>,
        matched_processes_listing: &fs::FlowProcessStateListing,
    ) -> Result<HashMap<odf::DatasetID, odf::DatasetHandle>> {
        let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);

        let matched_dataset_ids = matched_processes_listing
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

        Ok(dataset_handles_resolution
            .resolved_handles
            .into_iter()
            .map(|hdl| (hdl.id.clone(), hdl))
            .collect::<HashMap<_, _>>())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowProcessDatasetCard {
    dataset_request_state: DatasetRequestStateWithOwner,
    process_state: fs::FlowProcessState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl AccountFlowProcessDatasetCard {
    #[graphql(skip)]
    pub fn new(
        account: Account,
        dataset_handle: odf::DatasetHandle,
        process_state: fs::FlowProcessState,
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

#[derive(InputObject, Debug, Clone)]
pub(crate) struct FlowProcessOrdering {
    pub field: FlowProcessOrderField,
    pub direction: OrderingDirection,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(remote = "fs::FlowProcessOrderField")]
pub(crate) enum FlowProcessOrderField {
    /// Default for “recent activity”.
    LastAttemptAt,

    /// “What’s next”
    NextPlannedAt,

    /// Triage hot spots.
    LastFailureAt,

    /// Chronic issues first.
    ConsecutiveFailures,

    /// Severity bucketing
    EffectiveState,

    /// By flow type
    FlowType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
