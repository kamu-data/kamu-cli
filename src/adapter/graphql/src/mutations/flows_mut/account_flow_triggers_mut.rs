// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use kamu_accounts::Account;
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
use kamu_flow_system::FlowTriggerService;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowTriggersMut {
    account: Account,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl AccountFlowTriggersMut {
    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    #[graphql(skip)]
    async fn get_account_dataset_ids(&self, ctx: &Context<'_>) -> Result<Vec<odf::DatasetID>> {
        let dataset_entry_service = from_catalog_n!(ctx, dyn DatasetEntryService);
        let dataset_ids: Vec<_> = dataset_entry_service
            .get_owned_dataset_ids(&self.account.id)
            .await
            .int_err()?;

        Ok(dataset_ids)
    }

    #[tracing::instrument(level = "info", name = AccountFlowTriggersMut_resume_account_dataset_flows, skip_all)]
    async fn resume_account_dataset_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let account_dataset_ids = self.get_account_dataset_ids(ctx).await?;
        for dataset_id in &account_dataset_ids {
            flow_trigger_service
                .resume_dataset_flows(Utc::now(), dataset_id, None)
                .await
                .int_err()?;
        }

        Ok(true)
    }

    #[tracing::instrument(level = "info", name = AccountFlowTriggersMut_pause_account_dataset_flows, skip_all)]
    async fn pause_account_dataset_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);

        let account_dataset_ids = self.get_account_dataset_ids(ctx).await?;
        for dataset_id in &account_dataset_ids {
            flow_trigger_service
                .pause_dataset_flows(Utc::now(), dataset_id, None)
                .await
                .int_err()?;
        }

        Ok(true)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
