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
use kamu_datasets::DatasetOwnershipService;
use kamu_flow_system::FlowTriggerService;
use opendatafabric::DatasetID;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowTriggersMut {
    account: Account,
}

#[Object]
impl AccountFlowTriggersMut {
    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    #[graphql(skip)]
    async fn get_account_dataset_ids(&self, ctx: &Context<'_>) -> Result<Vec<DatasetID>> {
        let dataset_ownership_service = from_catalog_n!(ctx, dyn DatasetOwnershipService);
        let dataset_ids: Vec<_> = dataset_ownership_service
            .get_owned_datasets(&self.account.id)
            .await?;

        Ok(dataset_ids)
    }

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
