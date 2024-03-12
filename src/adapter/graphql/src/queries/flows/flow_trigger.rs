// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetRepository;
use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::{Account, Dataset};

///////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowTrigger {
    Manual(FlowTriggerManual),
    AutoPolling(FlowTriggerAutoPolling),
    Push(FlowTriggerPush),
    InputDatasetFlow(FlowTriggerInputDatasetFlow),
}

impl FlowTrigger {
    pub async fn build(trigger: fs::FlowTrigger, ctx: &Context<'_>) -> Result<Self, InternalError> {
        Ok(match trigger {
            fs::FlowTrigger::Manual(manual) => Self::Manual(manual.into()),
            fs::FlowTrigger::AutoPolling(auto_polling) => Self::AutoPolling(auto_polling.into()),
            fs::FlowTrigger::Push(push) => Self::Push(push.into()),
            fs::FlowTrigger::InputDatasetFlow(input) => {
                let dataset_repository = from_catalog::<dyn DatasetRepository>(ctx).unwrap();
                let hdl = dataset_repository
                    .resolve_dataset_ref(&input.dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                Self::InputDatasetFlow(FlowTriggerInputDatasetFlow::new(
                    Dataset::new(Account::from_dataset_alias(ctx, &hdl.alias), hdl),
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

impl From<fs::FlowTriggerManual> for FlowTriggerManual {
    fn from(value: fs::FlowTriggerManual) -> Self {
        Self {
            initiator: Account::from_account_name(value.initiator_account_name),
        }
    }
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

///////////////////////////////////////////////////////////////////////////////
