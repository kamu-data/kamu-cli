// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{FlowKeyDataset, FlowTriggerService};

use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowTriggers<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetFlowTriggers<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Returns defined trigger for a flow of specified type
    #[tracing::instrument(level = "info", name = DatasetFlowTriggers_by_type, skip_all, fields(?dataset_flow_type))]
    async fn by_type(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<Option<FlowTrigger>> {
        self.dataset_request_state
            .check_dataset_read_access(ctx)
            .await?;

        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);
        let maybe_flow_trigger = flow_trigger_service
            .find_trigger(
                FlowKeyDataset::new(
                    self.dataset_request_state.dataset_handle().id.clone(),
                    dataset_flow_type.into(),
                )
                .into(),
            )
            .await
            .int_err()?;

        Ok(maybe_flow_trigger.map(Into::into))
    }

    /// Checks if all triggers of this dataset are disabled
    #[tracing::instrument(level = "info", name = DatasetFlowTriggers_all_paused, skip_all)]
    async fn all_paused(&self, ctx: &Context<'_>) -> Result<bool> {
        self.dataset_request_state
            .check_dataset_read_access(ctx)
            .await?;

        let flow_trigger_service = from_catalog_n!(ctx, dyn FlowTriggerService);
        for dataset_flow_type in kamu_flow_system::DatasetFlowType::all() {
            let maybe_flow_trigger = flow_trigger_service
                .find_trigger(
                    FlowKeyDataset::new(
                        self.dataset_request_state.dataset_handle().id.clone(),
                        *dataset_flow_type,
                    )
                    .into(),
                )
                .await
                .int_err()?;

            if let Some(flow_trigger) = maybe_flow_trigger
                && flow_trigger.is_active()
            {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
