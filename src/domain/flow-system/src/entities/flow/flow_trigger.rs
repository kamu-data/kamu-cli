// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
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
    pub fn trigger_time(&self) -> DateTime<Utc> {
        match self {
            Self::Manual(t) => t.trigger_time,
            Self::AutoPolling(t) => t.trigger_time,
            Self::Push(t) => t.trigger_time,
            Self::InputDatasetFlow(t) => t.trigger_time,
        }
    }

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
            None
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
    pub trigger_time: DateTime<Utc>,
    pub initiator_account_id: AccountID,
    pub initiator_account_name: AccountName,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerAutoPolling {
    pub trigger_time: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerPush {
    // TODO: source (HTTP, MQTT, CMD, ...)
    pub trigger_time: DateTime<Utc>,
    pub source_name: Option<String>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTriggerInputDatasetFlow {
    pub trigger_time: DateTime<Utc>,
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

#[cfg(test)]
mod tests {
    use kamu_core::auth::DEFAULT_ACCOUNT_NAME;
    use lazy_static::lazy_static;
    use opendatafabric::{Multihash, FAKE_ACCOUNT_ID};

    use super::*;

    lazy_static! {
        static ref TEST_DATASET_ID: DatasetID = DatasetID::new_seeded_ed25519(b"test");
        static ref AUTO_POLLING_TRIGGER: FlowTrigger =
            FlowTrigger::AutoPolling(FlowTriggerAutoPolling {
                trigger_time: Utc::now(),
            });
        static ref MANUAL_TRIGGER: FlowTrigger = FlowTrigger::Manual(FlowTriggerManual {
            trigger_time: Utc::now(),
            initiator_account_id: String::from(FAKE_ACCOUNT_ID),
            initiator_account_name: AccountName::new_unchecked(DEFAULT_ACCOUNT_NAME),
        });
        static ref PUSH_SOURCE_TRIGGER: FlowTrigger = FlowTrigger::Push(FlowTriggerPush {
            trigger_time: Utc::now(),
            source_name: None
        });
        static ref INPUT_DATASET_TRIGGER: FlowTrigger =
            FlowTrigger::InputDatasetFlow(FlowTriggerInputDatasetFlow {
                trigger_time: Utc::now(),
                dataset_id: TEST_DATASET_ID.clone(),
                flow_type: DatasetFlowType::Ingest,
                flow_id: FlowID::new(5),
                flow_result: FlowResult::DatasetUpdate(FlowResultDatasetUpdate {
                    old_head: None,
                    new_head: Multihash::from_digest_sha3_256(b"some-slice")
                })
            });
    }

    #[test]
    fn test_is_unique_auto_polling() {
        assert!(AUTO_POLLING_TRIGGER.is_unique_vs(&[]));
        assert!(AUTO_POLLING_TRIGGER.is_unique_vs(&[
            MANUAL_TRIGGER.clone(),
            PUSH_SOURCE_TRIGGER.clone(),
            INPUT_DATASET_TRIGGER.clone()
        ]));

        assert!(!AUTO_POLLING_TRIGGER.is_unique_vs(&[AUTO_POLLING_TRIGGER.clone()]));
    }

    #[test]
    fn test_is_unique_manual() {
        assert!(MANUAL_TRIGGER.is_unique_vs(&[]));
        assert!(MANUAL_TRIGGER.is_unique_vs(&[
            AUTO_POLLING_TRIGGER.clone(),
            PUSH_SOURCE_TRIGGER.clone(),
            INPUT_DATASET_TRIGGER.clone()
        ]));

        assert!(
            MANUAL_TRIGGER.is_unique_vs(&[FlowTrigger::Manual(FlowTriggerManual {
                trigger_time: Utc::now(),
                initiator_account_id: String::from("23456"),
                initiator_account_name: AccountName::new_unchecked("different"),
            })])
        );

        assert!(!MANUAL_TRIGGER.is_unique_vs(&[MANUAL_TRIGGER.clone()]));
    }

    #[test]
    fn test_is_unique_push() {
        assert!(PUSH_SOURCE_TRIGGER.is_unique_vs(&[]));
        assert!(PUSH_SOURCE_TRIGGER.is_unique_vs(&[
            AUTO_POLLING_TRIGGER.clone(),
            MANUAL_TRIGGER.clone(),
            INPUT_DATASET_TRIGGER.clone()
        ]));

        assert!(
            PUSH_SOURCE_TRIGGER.is_unique_vs(&[FlowTrigger::Push(FlowTriggerPush {
                trigger_time: Utc::now(),
                source_name: Some("different".to_string())
            })])
        );

        assert!(!PUSH_SOURCE_TRIGGER.is_unique_vs(&[PUSH_SOURCE_TRIGGER.clone()]));
    }

    #[test]
    fn test_is_unique_input_dataset() {
        assert!(INPUT_DATASET_TRIGGER.is_unique_vs(&[]));
        assert!(INPUT_DATASET_TRIGGER.is_unique_vs(&[
            AUTO_POLLING_TRIGGER.clone(),
            PUSH_SOURCE_TRIGGER.clone(),
            MANUAL_TRIGGER.clone()
        ]));

        // Test unrelated flow for same dataset
        assert!(
            INPUT_DATASET_TRIGGER.is_unique_vs(&[FlowTrigger::InputDatasetFlow(
                FlowTriggerInputDatasetFlow {
                    trigger_time: Utc::now(),
                    dataset_id: TEST_DATASET_ID.clone(),
                    flow_type: DatasetFlowType::Compaction, // unrelated
                    flow_id: FlowID::new(7),
                    flow_result: FlowResult::Empty
                }
            )])
        );

        // Test same flow type for different dataset
        assert!(
            INPUT_DATASET_TRIGGER.is_unique_vs(&[FlowTrigger::InputDatasetFlow(
                FlowTriggerInputDatasetFlow {
                    trigger_time: Utc::now(),
                    dataset_id: DatasetID::new_seeded_ed25519(b"different"),
                    flow_type: DatasetFlowType::Ingest,
                    flow_id: FlowID::new(7),
                    flow_result: FlowResult::Empty
                }
            )])
        );

        assert!(!INPUT_DATASET_TRIGGER.is_unique_vs(&[INPUT_DATASET_TRIGGER.clone()]));
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
