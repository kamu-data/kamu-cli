// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use kamu_core::DatasetRepository;
use {kamu_flow_system as fs, opendatafabric as odf};

use crate::prelude::*;
use crate::queries::{DatasetFlowFilters, Flow, FlowConnection};
use crate::utils;

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
        filters: Option<DatasetFlowFilters>,
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

        let flows: Vec<_> = flows_state_listing.matched_stream.try_collect().await?;
        let mut matched_flows: Vec<Flow> = vec![];

        for flow in &flows {
            let dataset_repo = from_catalog::<dyn DatasetRepository>(ctx).unwrap();

            if let fs::FlowKey::Dataset(dataset_flow_key) = &flow.flow_key {
                let dataset_handle = dataset_repo
                    .resolve_dataset_ref(&dataset_flow_key.dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                // ToDo improve to check more general permissions
                if utils::check_dataset_read_access(ctx, &dataset_handle)
                    .await
                    .is_ok()
                {
                    matched_flows.push(Flow::new(flow.clone()));
                }
            }
        }
        let total_count = flows_state_listing.total_count;

        Ok(FlowConnection::new(
            matched_flows,
            page,
            per_page,
            total_count,
        ))
    }
}
