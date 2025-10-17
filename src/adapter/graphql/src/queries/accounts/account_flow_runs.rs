// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use database_common::PaginationOpts;
use futures::TryStreamExt;
use kamu_accounts::Account as AccountEntity;
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use super::Account;
use crate::prelude::*;
use crate::queries::{
    Dataset,
    DatasetConnection,
    Flow,
    FlowConnection,
    FlowProcessTypeFilterInput,
    InitiatorFilterInput,
    list_accessible_datasets_with_flows,
    prepare_flows_filter_by_initiator,
    prepare_flows_filter_by_types,
    prepare_flows_scope_query,
};
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowRuns<'a> {
    account: &'a AccountEntity,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> AccountFlowRuns<'a> {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account: &'a AccountEntity) -> Self {
        Self { account }
    }

    #[tracing::instrument(level = "info", name = AccountFlowRuns_list_flows, skip_all, fields(?page, ?per_page, ?filters))]
    pub async fn list_flows(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<AccountFlowFilters>,
    ) -> Result<FlowConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let logged = utils::logged_account(ctx);
        if !logged {
            return Ok(FlowConnection::new(Vec::new(), page, per_page, 0));
        }

        let (flow_query_service, dataset_entry_service) =
            from_catalog_n!(ctx, dyn fs::FlowQueryService, dyn DatasetEntryService);

        let dataset_ids = {
            let maybe_expected_dataset_ids: Option<HashSet<odf::DatasetID>> =
                filters.as_ref().map(|filters| {
                    filters
                        .by_dataset_ids
                        .iter()
                        .cloned()
                        .map(Into::into)
                        .collect()
                });

            // Note: consider using ReBAC to filter dataset
            // It should be okay if any kind of relation exists explicitly to view flows,
            //   which is wider than viewing only the ones you own
            let account_dataset_ids = dataset_entry_service
                .get_owned_dataset_ids(&self.account.id)
                .await
                .int_err()?;

            if let Some(expected_dataset_ids) = maybe_expected_dataset_ids
                && !expected_dataset_ids.is_empty()
            {
                account_dataset_ids
                    .into_iter()
                    .filter(|dataset_id| expected_dataset_ids.contains(dataset_id))
                    .collect()
            } else {
                account_dataset_ids
            }
        };

        let dataset_id_refs = dataset_ids.iter().collect::<Vec<_>>();

        let scope_query = filters
            .as_ref()
            .map(|filters| {
                prepare_flows_scope_query(filters.by_process_type.as_ref(), &dataset_id_refs)
            })
            .unwrap_or_else(|| {
                afs::FlowScopeDataset::query_for_multiple_datasets(&dataset_id_refs)
            });

        let dataset_flow_filters = filters
            .map(|filters| kamu_flow_system::FlowFilters {
                by_flow_types: prepare_flows_filter_by_types(filters.by_process_type.as_ref()),
                by_flow_status: filters.by_status.map(Into::into),
                by_initiator: prepare_flows_filter_by_initiator(filters.by_initiator),
            })
            .unwrap_or_default();

        let flows_state_listing = flow_query_service
            .list_scoped_flows(
                scope_query,
                dataset_flow_filters,
                PaginationOpts::from_page(page, per_page),
            )
            .await
            .int_err()?;

        let account_flow_states: Vec<_> = flows_state_listing.matched_stream.try_collect().await?;
        let total_count = flows_state_listing.total_count;
        let matched_flows = Flow::build_batch(account_flow_states, ctx).await?;

        Ok(FlowConnection::new(
            matched_flows,
            page,
            per_page,
            total_count,
        ))
    }

    #[tracing::instrument(level = "info", name = AccountFlowRuns_list_datasets_with_flow, skip_all)]
    pub async fn list_datasets_with_flow(&self, ctx: &Context<'_>) -> Result<DatasetConnection> {
        let logged = utils::logged_account(ctx);
        if !logged {
            return Ok(DatasetConnection::new(Vec::new(), 0, 0, 0));
        }

        let (dataset_handles, _) =
            list_accessible_datasets_with_flows(ctx, &self.account.id, 0, usize::MAX).await?;

        let account = Account::new(
            self.account.id.clone().into(),
            self.account.account_name.clone().into(),
        );
        let readable_datasets: Vec<_> = dataset_handles
            .into_iter()
            .map(|dataset_handle| Dataset::new_access_checked(account.clone(), dataset_handle))
            .collect();
        let total_count = readable_datasets.len();

        Ok(DatasetConnection::new(
            readable_datasets,
            0,
            total_count,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct AccountFlowFilters {
    by_status: Option<FlowStatus>,
    by_initiator: Option<InitiatorFilterInput>,
    by_dataset_ids: Vec<DatasetID<'static>>,
    by_process_type: Option<FlowProcessTypeFilterInput>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
