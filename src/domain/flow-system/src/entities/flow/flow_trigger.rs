// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{AccountID, AccountName, DatasetID};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowTrigger {
    Manual(FlowTriggerManual),
    AutoPolling(FlowTriggerAutoPolling),
    Push(FlowTriggerPush),
    InputDatasetFlow(FlowTriggerInputDatasetFlow),
}

impl FlowTrigger {
    pub fn initiator_account_name(&self) -> Option<&AccountName> {
        if let FlowTrigger::Manual(manual) = self {
            Some(&manual.initiator_account_name)
        } else {
            None
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerManual {
    pub initiator_account_id: AccountID,
    pub initiator_account_name: AccountName,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerAutoPolling {}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerPush {
    // TODO: source (HTTP, MQTT, CMD, ...)
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerInputDatasetFlow {
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
    pub flow_id: FlowID,
}

/////////////////////////////////////////////////////////////////////////////////////////
