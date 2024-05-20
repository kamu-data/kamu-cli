// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use futures::TryStreamExt;
use kamu_accounts::Account;
use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::{Flow, FlowConnection, InitiatorFilterInput};

pub struct AccountFlowRuns {
    account: Account,
}

#[Object]
impl AccountFlowRuns {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    async fn list_flows(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<AccountFlowFilters>,
    ) -> Result<FlowConnection> {
        let flow_service = from_catalog::<dyn fs::FlowService>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let filters = match filters {
            Some(filters) => Some(kamu_flow_system::AccountFlowFilters {
                by_flow_type: filters.by_flow_type.map(Into::into),
                by_flow_status: filters.by_status.map(Into::into),
                by_dataset_ids: filters
                    .by_dataset_ids
                    .iter()
                    .map(|dataset_id| dataset_id.clone().into())
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

        let flows_state_listing = flow_service
            .list_all_flows_by_account(
                &self.account.id,
                match filters {
                    Some(filters) => filters,
                    None => Default::default(),
                },
                fs::FlowPaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                },
            )
            .await
            .int_err()?;

        let matched_flows: Vec<_> = flows_state_listing
            .matched_stream
            .map_ok(Flow::new)
            .try_collect()
            .await?;
        let total_count = flows_state_listing.total_count;

        Ok(FlowConnection::new(
            matched_flows,
            page,
            per_page,
            total_count,
        ))
    }
}

#[derive(InputObject)]
pub struct AccountFlowFilters {
    by_flow_type: Option<DatasetFlowType>,
    by_status: Option<FlowStatus>,
    by_initiator: Option<InitiatorFilterInput>,
    by_dataset_ids: Vec<DatasetID>,
}
