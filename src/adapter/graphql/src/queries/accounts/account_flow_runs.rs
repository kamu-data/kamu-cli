// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use {kamu_flow_system as fs, opendatafabric as odf};

use crate::prelude::*;
use crate::queries::{Flow, FlowConnection};

pub struct AccountFlowRuns {
    account_name: odf::AccountName,
}

#[Object]
impl AccountFlowRuns {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account_name: odf::AccountName) -> Self {
        Self { account_name }
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

        let flows_state_listing = flow_service
            .list_all_flows_by_account(
                &self.account_name,
                match filters {
                    Some(filters) => filters.into(),
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
    by_dataset_name: Option<DatasetName>,
}

impl From<AccountFlowFilters> for fs::AccountFlowFilters {
    fn from(value: AccountFlowFilters) -> Self {
        Self {
            by_dataset_name: value.by_dataset_name.map(Into::into),
        }
    }
}
