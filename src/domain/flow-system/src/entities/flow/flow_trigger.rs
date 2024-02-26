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

    pub fn push_source_name(&self) -> Option<String> {
        if let FlowTrigger::Push(trigger_push) = self {
            trigger_push.source_name.clone()
        } else {
            panic!("Any trigger kind except Push unexpected")
        }
    }

    /// Checks if new trigger is unique compared to the existing triggers
    pub fn is_unique_vs(&self, existing_triggers: &[FlowTrigger]) -> bool {
        // Try finding a similar existing trigger and abort early, when found
        for existing in existing_triggers {
            match (self, existing) {
                (FlowTrigger::Manual(this), FlowTrigger::Manual(existing)) if this == existing => {
                    return false
                }
                (FlowTrigger::AutoPolling(_), FlowTrigger::AutoPolling(_)) => return false,
                (FlowTrigger::Push(this), FlowTrigger::Push(existing))
                    if this.source_name == existing.source_name =>
                {
                    return false
                }
                (FlowTrigger::InputDatasetFlow(this), FlowTrigger::InputDatasetFlow(existing)) => {
                    if this.is_same_key_as(existing) {
                        // We should not be getting the same flow twice!
                        assert_ne!(this.flow_id, existing.flow_id);
                        return false;
                    }
                }
                _ => { /* Continue comparing */ }
            }
        }

        // No similar trigger was found, so it's a truly unique one
        true
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
    source_name: Option<String>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerInputDatasetFlow {
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
    pub flow_id: FlowID,
    pub flow_result: FlowResult,
}

impl FlowTriggerInputDatasetFlow {
    pub fn is_same_key_as(&self, other: &FlowTriggerInputDatasetFlow) -> bool {
        self.flow_type == other.flow_type && self.dataset_id == other.dataset_id
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
