// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use fs::{DatasetFlowFilters, FlowConfigurationService};
use futures::TryStreamExt;
use kamu_core::DatasetRepository;
use {kamu_flow_system as fs, opendatafabric as odf};

use super::ensure_scheduling_permission;
use crate::prelude::*;

pub struct AccountFlowConfigsMut {
    account_name: odf::AccountName,
}

#[Object]
impl AccountFlowConfigsMut {
    #[graphql(skip)]
    pub fn new(account_name: odf::AccountName) -> Self {
        Self { account_name }
    }

    #[graphql(skip)]
    async fn get_account_flow_keys(&self, ctx: &Context<'_>) -> Result<Vec<fs::FlowKey>> {
        let flow_service = from_catalog::<dyn fs::FlowService>(ctx).unwrap();
        let flow_event_store = from_catalog::<dyn fs::FlowEventStore>(ctx).unwrap();
        let flows_count = flow_event_store
            .get_count_flows_by_account(&self.account_name, &DatasetFlowFilters::default())
            .await
            .int_err()?;

        let account_flow_keys: Vec<_> = flow_service
            .list_all_flows_by_account(
                &self.account_name,
                DatasetFlowFilters::default(),
                fs::FlowPaginationOpts {
                    offset: 0,
                    limit: flows_count,
                },
            )
            .await
            .int_err()?
            .matched_stream
            .map_ok(|flow_state| flow_state.flow_key)
            .try_collect()
            .await?;

        Ok(account_flow_keys)
    }

    async fn resume_account_dataset_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();
        let dataset_repo = from_catalog::<dyn DatasetRepository>(ctx).unwrap();

        let account_flow_keys = self.get_account_flow_keys(ctx).await?;
        for account_flow_key in &account_flow_keys {
            if let fs::FlowKey::Dataset(dataset_flow_key) = account_flow_key {
                let dataset_handle = dataset_repo
                    .resolve_dataset_ref(&dataset_flow_key.dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                // ToDo improve to check more general permissions
                if ensure_scheduling_permission(ctx, &dataset_handle)
                    .await
                    .is_ok()
                {
                    flow_config_service
                        .resume_flow_configuration(Utc::now(), account_flow_key.clone())
                        .await
                        .int_err()?;
                }
            }
        }

        Ok(true)
    }

    async fn pause_account_dataset_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();
        let dataset_repo = from_catalog::<dyn DatasetRepository>(ctx).unwrap();

        let account_flow_keys = self.get_account_flow_keys(ctx).await?;
        for account_flow_key in &account_flow_keys {
            if let fs::FlowKey::Dataset(dataset_flow_key) = account_flow_key {
                let dataset_handle = dataset_repo
                    .resolve_dataset_ref(&dataset_flow_key.dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                // ToDo improve to check more general permissions
                if ensure_scheduling_permission(ctx, &dataset_handle)
                    .await
                    .is_ok()
                {
                    flow_config_service
                        .pause_flow_configuration(Utc::now(), account_flow_key.clone())
                        .await
                        .int_err()?;
                }
            }
        }

        Ok(true)
    }
}
