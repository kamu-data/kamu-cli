// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetRegistry;
use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowTriggerInstance {
    Manual(FlowTriggerManual),
    AutoPolling(FlowTriggerAutoPolling),
    Push(FlowTriggerPush),
    InputDatasetFlow(FlowTriggerInputDatasetFlow),
}

impl FlowTriggerInstance {
    pub async fn build(
        trigger: &fs::FlowTriggerInstance,
        ctx: &Context<'_>,
    ) -> Result<Self, InternalError> {
        Ok(match &trigger {
            fs::FlowTriggerInstance::Manual(manual) => {
                let initiator =
                    Account::from_account_id(ctx, manual.initiator_account_id.clone()).await?;
                Self::Manual(FlowTriggerManual { initiator })
            }
            fs::FlowTriggerInstance::AutoPolling(auto_polling) => {
                Self::AutoPolling(auto_polling.clone().into())
            }
            fs::FlowTriggerInstance::Push(push) => Self::Push(push.clone().into()),
            fs::FlowTriggerInstance::InputDatasetFlow(input) => {
                let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);

                let hdl = dataset_registry
                    .resolve_dataset_handle_by_ref(&input.dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                let account = Account::from_dataset_alias(ctx, &hdl.alias)
                    .await?
                    .expect("Account must exist");
                Self::InputDatasetFlow(FlowTriggerInputDatasetFlow::new(
                    Dataset::new_access_checked(account, hdl),
                    input.flow_type.into(),
                    input.flow_id.into(),
                ))
            }
        })
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowTriggerManual {
    initiator: Account,
}

#[derive(SimpleObject)]
pub(crate) struct FlowTriggerAutoPolling {
    dummy: bool,
}

impl From<fs::FlowTriggerAutoPolling> for FlowTriggerAutoPolling {
    fn from(_: fs::FlowTriggerAutoPolling) -> Self {
        Self { dummy: true }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowTriggerPush {
    dummy: bool,
}

impl From<fs::FlowTriggerPush> for FlowTriggerPush {
    fn from(_: fs::FlowTriggerPush) -> Self {
        Self { dummy: true }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowTriggerInputDatasetFlow {
    dataset: Dataset,
    flow_type: DatasetFlowType,
    flow_id: FlowID,
}

impl FlowTriggerInputDatasetFlow {
    pub fn new(dataset: Dataset, flow_type: DatasetFlowType, flow_id: FlowID) -> Self {
        Self {
            dataset,
            flow_type,
            flow_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
