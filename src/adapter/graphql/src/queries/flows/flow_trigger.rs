// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::Account;

///////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowTrigger {
    Manual(FlowTriggerManual),
    AutoPolling(FlowTriggerAutoPolling),
    Push(FlowTriggerPush),
    InputDatasetFlow(FlowTriggerInputDatasetFlow),
}

impl From<fs::FlowTrigger> for FlowTrigger {
    fn from(value: fs::FlowTrigger) -> Self {
        match value {
            fs::FlowTrigger::Manual(manual) => Self::Manual(manual.into()),
            fs::FlowTrigger::AutoPolling(auto_polling) => Self::AutoPolling(auto_polling.into()),
            fs::FlowTrigger::Push(push) => Self::Push(push.into()),
            fs::FlowTrigger::InputDatasetFlow(input) => Self::InputDatasetFlow(input.into()),
        }
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
    dataset_id: DatasetID,
    flow_type: DatasetFlowType,
    flow_id: FlowID,
}

impl From<fs::FlowTriggerInputDatasetFlow> for FlowTriggerInputDatasetFlow {
    fn from(value: fs::FlowTriggerInputDatasetFlow) -> Self {
        Self {
            dataset_id: value.dataset_id.into(),
            flow_type: value.flow_type.into(),
            flow_id: value.flow_id.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
