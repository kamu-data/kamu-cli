// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::StreamExt;
use kamu_accounts::Account as AccountEntity;
use kamu_core::DatasetOwnershipService;
use kamu_flow_system::FlowConfigurationService;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowConfigs {
    account: AccountEntity,
}

#[Object]
impl AccountFlowConfigs {
    #[graphql(skip)]
    pub fn new(account: AccountEntity) -> Self {
        Self { account }
    }

    /// Checks if all configs of all datasets in account are disabled
    async fn all_paused(&self, ctx: &Context<'_>) -> Result<bool> {
        let dataset_ownership_service = from_catalog::<dyn DatasetOwnershipService>(ctx).unwrap();
        let owned_dataset_ids: Vec<_> = dataset_ownership_service
            .get_owned_datasets(&self.account.id)
            .await?;
        let flow_config_service = from_catalog::<dyn FlowConfigurationService>(ctx).unwrap();

        let mut all_configurations = flow_config_service
            .find_configurations_by_datasets(owned_dataset_ids)
            .await;

        while let Some(configuration_result) = all_configurations.next().await {
            if let Ok(configuration) = configuration_result
                && configuration.is_active()
            {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
