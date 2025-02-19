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
use kamu::utils::datasets_filtering::filter_datasets_by_local_pattern;
use kamu_accounts::Account as AccountEntity;
use kamu_core::DatasetRegistry;
use kamu_flow_system as fs;

use super::Account;
use crate::prelude::*;
use crate::queries::{Dataset, DatasetConnection, Flow, FlowConnection, InitiatorFilterInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowRuns {
    account: AccountEntity,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl AccountFlowRuns {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account: AccountEntity) -> Self {
        Self { account }
    }

    #[tracing::instrument(level = "info", name = AccountFlowRuns_list_flows, skip_all, fields(?page, ?per_page, ?filters))]
    async fn list_flows(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<AccountFlowFilters>,
    ) -> Result<FlowConnection> {
        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let filters = match filters {
            Some(filters) => Some(kamu_flow_system::AccountFlowFilters {
                by_flow_type: filters.by_flow_type.map(Into::into),
                by_flow_status: filters.by_status.map(Into::into),
                by_dataset_ids: filters
                    .by_dataset_ids
                    .into_iter()
                    .map(Into::into)
                    .collect::<HashSet<_>>(),
                by_initiator: match filters.by_initiator {
                    Some(initiator_filter) => match initiator_filter {
                        InitiatorFilterInput::System(_) => {
                            Some(kamu_flow_system::InitiatorFilter::System)
                        }
                        InitiatorFilterInput::Accounts(account_ids) => Some(
                            kamu_flow_system::InitiatorFilter::Account(HashSet::from_iter(
                                account_ids.iter().map(Into::into).collect::<Vec<_>>(),
                            )),
                        ),
                    },
                    None => None,
                },
            }),
            None => None,
        };

        let flows_state_listing = flow_query_service
            .list_all_flows_by_account(
                &self.account.id,
                filters.unwrap_or_default(),
                PaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                },
            )
            .await
            .int_err()?;

        let matched_flow_states: Vec<_> = flows_state_listing.matched_stream.try_collect().await?;
        let total_count = flows_state_listing.total_count;
        let matched_flows = Flow::build_batch(matched_flow_states, ctx).await?;

        Ok(FlowConnection::new(
            matched_flows,
            page,
            per_page,
            total_count,
        ))
    }

    #[tracing::instrument(level = "info", name = AccountFlowRuns_list_datasets_with_flow, skip_all)]
    async fn list_datasets_with_flow(&self, ctx: &Context<'_>) -> Result<DatasetConnection> {
        let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

        let datasets_with_flows: Vec<_> = flow_query_service
            .list_all_datasets_with_flow_by_account(&self.account.id)
            .await
            .int_err()?
            .matched_stream
            .map_ok(|dataset_id| odf::DatasetRefPattern::Ref(dataset_id.as_local_ref()))
            .try_collect()
            .await?;

        let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);

        let account = Account::new(
            self.account.id.clone().into(),
            self.account.account_name.clone().into(),
        );

        let matched_datasets: Vec<_> =
            filter_datasets_by_local_pattern(dataset_registry.as_ref(), datasets_with_flows)
                .map_ok(|dataset_handle| Dataset::new(account.clone(), dataset_handle))
                .try_collect()
                .await
                .int_err()?;

        let total_count = matched_datasets.len();

        Ok(DatasetConnection::new(
            matched_datasets,
            0,
            total_count,
            total_count,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct AccountFlowFilters {
    by_flow_type: Option<DatasetFlowType>,
    by_status: Option<FlowStatus>,
    by_initiator: Option<InitiatorFilterInput>,
    by_dataset_ids: Vec<DatasetID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
