// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Account as AccountEntity;
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
use kamu_flow_system::FlowTriggerService;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowTriggers<'a> {
    account: &'a AccountEntity,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> AccountFlowTriggers<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a AccountEntity) -> Self {
        Self { account }
    }

    /// Checks if all triggers of all datasets in account are disabled
    #[tracing::instrument(level = "info", name = AccountFlowTriggers_all_paused, skip_all)]
    async fn all_paused(&self, ctx: &Context<'_>) -> Result<bool> {
        let (dataset_entry_service, flow_trigger_service) =
            from_catalog_n!(ctx, dyn DatasetEntryService, dyn FlowTriggerService);

        let owned_dataset_ids: Vec<_> = dataset_entry_service
            .get_owned_dataset_ids(&self.account.id)
            .await
            .int_err()?;

        let has_active_triggers = flow_trigger_service
            .has_active_triggers_for_datasets(&owned_dataset_ids)
            .await?;

        Ok(!has_active_triggers)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
