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
use kamu_core::{auth, DatasetRegistry};
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
use kamu_flow_system as fs;

use super::Account;
use crate::prelude::*;
use crate::queries::{Dataset, DatasetConnection, Flow, FlowConnection, InitiatorFilterInput};
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowRuns<'a> {
    account: &'a AccountEntity,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> AccountFlowRuns<'a> {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account: &'a AccountEntity) -> Self {
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
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let logged = utils::logged_account(ctx);
        if !logged {
            return Ok(FlowConnection::new(Vec::new(), page, per_page, 0));
        }

        let maybe_filters = filters.map(|filters| kamu_flow_system::AccountFlowFilters {
            by_flow_type: filters.by_flow_type.map(Into::into),
            by_flow_status: filters.by_status.map(Into::into),
            by_dataset_ids: filters
                .by_dataset_ids
                .into_iter()
                .map(Into::into)
                .collect::<HashSet<_>>(),
            by_initiator: filters
                .by_initiator
                .map(|initiator_filter| match initiator_filter {
                    InitiatorFilterInput::System(_) => kamu_flow_system::InitiatorFilter::System,
                    InitiatorFilterInput::Accounts(account_ids) => {
                        kamu_flow_system::InitiatorFilter::Account(
                            account_ids.into_iter().map(Into::into).collect(),
                        )
                    }
                }),
        });

        let (flow_query_service, dataset_entry_service) =
            from_catalog_n!(ctx, dyn fs::FlowQueryService, dyn DatasetEntryService);

        let dataset_ids = {
            let maybe_expected_dataset_ids = maybe_filters
                .as_ref()
                .map(|filters| &filters.by_dataset_ids);

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

        let dataset_flow_filters = maybe_filters
            .map(|fs| kamu_flow_system::DatasetFlowFilters {
                by_flow_type: fs.by_flow_type,
                by_flow_status: fs.by_flow_status,
                by_initiator: fs.by_initiator,
            })
            .unwrap_or_default();
        let flows_state_listing = flow_query_service
            .list_all_flows_by_dataset_ids(
                &dataset_id_refs,
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
    async fn list_datasets_with_flow(&self, ctx: &Context<'_>) -> Result<DatasetConnection> {
        let logged = utils::logged_account(ctx);
        if !logged {
            return Ok(DatasetConnection::new(Vec::new(), 0, 0, 0));
        }

        let (flow_query_service, dataset_registry, dataset_action_authorizer) = from_catalog_n!(
            ctx,
            dyn fs::FlowQueryService,
            dyn DatasetRegistry,
            dyn auth::DatasetActionAuthorizer
        );

        let dataset_ids: Vec<_> = flow_query_service
            .list_all_datasets_with_flow_by_account(&self.account.id)
            .await
            .int_err()?
            .matched_stream
            .try_collect()
            .await?;
        let dataset_handles_resolution = dataset_registry
            .resolve_multiple_dataset_handles_by_ids(dataset_ids)
            .await
            .int_err()?;

        for (dataset_id, _) in dataset_handles_resolution.unresolved_datasets {
            tracing::warn!(
                %dataset_id,
                "Ignoring point that refers to a dataset not present in the registry",
            );
        }

        let readable_dataset_handles = dataset_action_authorizer
            .filter_datasets_allowing(
                dataset_handles_resolution.resolved_handles,
                auth::DatasetAction::Read,
            )
            .await?;

        let account = Account::new(
            self.account.id.clone().into(),
            self.account.account_name.clone().into(),
        );
        let readable_datasets: Vec<_> = readable_dataset_handles
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
    by_flow_type: Option<DatasetFlowType>,
    by_status: Option<FlowStatus>,
    by_initiator: Option<InitiatorFilterInput>,
    by_dataset_ids: Vec<DatasetID<'static>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
