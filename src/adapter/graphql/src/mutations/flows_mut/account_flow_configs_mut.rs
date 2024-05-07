// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use fs::FlowConfigurationService;
use futures::TryStreamExt;
use kamu_accounts::Account;
use kamu_core::DatasetRepository;
use kamu_flow_system as fs;
use opendatafabric::DatasetID;

use crate::prelude::*;

pub struct AccountFlowConfigsMut {
    account: Account,
}

#[Object]
impl AccountFlowConfigsMut {
    #[graphql(skip)]
    pub fn new(account: Account) -> Self {
        Self { account }
    }

    #[graphql(skip)]
    async fn get_account_dataset_ids(&self, ctx: &Context<'_>) -> Result<Vec<DatasetID>> {
        let datset_repo = from_catalog::<dyn DatasetRepository>(ctx).unwrap();
        let datset_ids: Vec<_> = datset_repo
            .get_datasets_by_owner(&self.account.account_name)
            .map_ok(|dataset_handle| dataset_handle.id)
            .try_collect()
            .await?;

        Ok(datset_ids)
    }

    async fn resume_account_dataset_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let account_dataset_ids = self.get_account_dataset_ids(ctx).await?;
        for dataset_id in &account_dataset_ids {
            flow_config_service
                .resume_dataset_flows(Utc::now(), dataset_id, None)
                .await
                .int_err()?;
        }

        Ok(true)
    }

    async fn pause_account_dataset_flows(&self, ctx: &Context<'_>) -> Result<bool> {
        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let account_dataset_ids = self.get_account_dataset_ids(ctx).await?;
        for dataset_id in &account_dataset_ids {
            flow_config_service
                .pause_dataset_flows(Utc::now(), dataset_id, None)
                .await
                .int_err()?;
        }

        Ok(true)
    }
}
