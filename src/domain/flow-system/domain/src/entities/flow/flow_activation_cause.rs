// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_task_system as ts;
use serde::{Deserialize, Serialize};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowActivationCause {
    Manual(FlowActivationCauseManual),
    AutoPolling(FlowActivationCauseAutoPolling),
    Push(FlowActivationCausePush),
    InputDatasetFlow(FlowActivationCauseInputDatasetFlow),
}

impl FlowActivationCause {
    pub fn activation_time(&self) -> DateTime<Utc> {
        match self {
            Self::Manual(t) => t.activation_time,
            Self::AutoPolling(t) => t.activation_time,
            Self::Push(t) => t.activation_time,
            Self::InputDatasetFlow(t) => t.activation_time,
        }
    }

    pub fn initiator_account_id(&self) -> Option<&odf::AccountID> {
        if let Self::Manual(manual) = self {
            Some(&manual.initiator_account_id)
        } else {
            None
        }
    }

    pub fn push_source_name(&self) -> Option<String> {
        if let Self::Push(cause_pish) = self {
            cause_pish.source_name.clone()
        } else {
            None
        }
    }

    pub fn activation_cause_description(&self) -> Option<String> {
        match self {
            Self::Manual(_) => Some("Flow activated manually".to_string()),
            Self::AutoPolling(_) => Some("Flow activated automatically".to_string()),
            Self::Push(cause_push) => match &cause_push.result {
                DatasetPushResult::HttpIngest(_) => {
                    Some("Flow activated by root dataset ingest via http endpoint".to_string())
                }
                DatasetPushResult::SmtpSync(sync_result) => {
                    if let Some(account_name) = sync_result.account_name_maybe.as_ref() {
                        Some(format!(
                            "Flow activated by root dataset ingest via SMTP sync by account: \
                             {account_name}",
                        ))
                    } else {
                        Some(
                            "Flow activated by root dataset ingest via SMTP sync anonymously"
                                .to_string(),
                        )
                    }
                }
            },
            Self::InputDatasetFlow(_) => Some("Flow activated by completed root flow".to_string()),
        }
    }

    /// Checks if new cause is unique compared to the existing causes
    pub fn is_unique_vs(&self, existing_causes: &[FlowActivationCause]) -> bool {
        // Try finding a similar existing causes and abort early, when found
        for existing in existing_causes {
            match (self, existing) {
                (Self::Manual(this), Self::Manual(existing)) if this == existing => return false,
                (Self::AutoPolling(_), Self::AutoPolling(_)) => return false,
                (Self::Push(this), Self::Push(existing))
                    if this.source_name == existing.source_name =>
                {
                    return false;
                }
                (Self::InputDatasetFlow(this), Self::InputDatasetFlow(existing)) => {
                    if this.is_same_key_as(existing) {
                        return false;
                    }
                }
                _ => { /* Continue comparing */ }
            }
        }

        // No similar cause was found, so it's a truly unique one
        true
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowActivationCauseManual {
    pub activation_time: DateTime<Utc>,
    pub initiator_account_id: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type InitiatorIDStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<odf::AccountID, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowActivationCauseAutoPolling {
    pub activation_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowActivationCausePush {
    pub activation_time: DateTime<Utc>,
    pub source_name: Option<String>,
    pub dataset_id: odf::DatasetID,
    pub result: DatasetPushResult,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DatasetPushResult {
    HttpIngest(DatasetPushHttpIngestResult),
    SmtpSync(DatasetPushSmtpSyncResult),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPushHttpIngestResult {
    pub old_head_maybe: Option<odf::Multihash>,
    pub new_head: odf::Multihash,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetPushSmtpSyncResult {
    pub old_head_maybe: Option<odf::Multihash>,
    pub new_head: odf::Multihash,
    pub account_name_maybe: Option<odf::AccountName>,
    pub is_force: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowActivationCauseInputDatasetFlow {
    pub activation_time: DateTime<Utc>,
    pub dataset_id: odf::DatasetID,
    pub flow_type: String,
    pub flow_id: FlowID,
    pub task_result: ts::TaskResult,
}

impl FlowActivationCauseInputDatasetFlow {
    pub fn is_same_key_as(&self, other: &FlowActivationCauseInputDatasetFlow) -> bool {
        self.flow_type == other.flow_type && self.dataset_id == other.dataset_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use kamu_accounts::DEFAULT_ACCOUNT_ID;

    use super::*;

    static TEST_DATASET_ID: LazyLock<odf::DatasetID> =
        LazyLock::new(|| odf::DatasetID::new_seeded_ed25519(b"test"));
    static AUTO_POLLING_ACTIVATION_CAUSE: LazyLock<FlowActivationCause> = LazyLock::new(|| {
        FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
            activation_time: Utc::now(),
        })
    });
    static MANUAL_ACTIVATION_CAUSE: LazyLock<FlowActivationCause> = LazyLock::new(|| {
        FlowActivationCause::Manual(FlowActivationCauseManual {
            activation_time: Utc::now(),
            initiator_account_id: DEFAULT_ACCOUNT_ID.clone(),
        })
    });
    static PUSH_SOURCE_ACTIVATION_CAUSE: LazyLock<FlowActivationCause> = LazyLock::new(|| {
        FlowActivationCause::Push(FlowActivationCausePush {
            activation_time: Utc::now(),
            source_name: None,
            dataset_id: TEST_DATASET_ID.clone(),
            result: DatasetPushResult::HttpIngest(DatasetPushHttpIngestResult {
                old_head_maybe: None,
                new_head: odf::Multihash::from_digest_sha3_256(b"some-slice"),
            }),
        })
    });
    static INPUT_DATASET_ACTIVATION_CAUSE: LazyLock<FlowActivationCause> = LazyLock::new(|| {
        FlowActivationCause::InputDatasetFlow(FlowActivationCauseInputDatasetFlow {
            activation_time: Utc::now(),
            dataset_id: TEST_DATASET_ID.clone(),
            flow_type: "SOME.FLOW.TYPE".to_string(),
            flow_id: FlowID::new(5),
            task_result: ts::TaskResult::empty(),
        })
    });

    #[test]
    fn test_is_unique_auto_polling() {
        assert!(AUTO_POLLING_ACTIVATION_CAUSE.is_unique_vs(&[]));
        assert!(AUTO_POLLING_ACTIVATION_CAUSE.is_unique_vs(&[
            MANUAL_ACTIVATION_CAUSE.clone(),
            PUSH_SOURCE_ACTIVATION_CAUSE.clone(),
            INPUT_DATASET_ACTIVATION_CAUSE.clone()
        ]));

        assert!(
            !AUTO_POLLING_ACTIVATION_CAUSE.is_unique_vs(&[AUTO_POLLING_ACTIVATION_CAUSE.clone()])
        );
    }

    #[test]
    fn test_is_unique_manual() {
        assert!(MANUAL_ACTIVATION_CAUSE.is_unique_vs(&[]));
        assert!(MANUAL_ACTIVATION_CAUSE.is_unique_vs(&[
            AUTO_POLLING_ACTIVATION_CAUSE.clone(),
            PUSH_SOURCE_ACTIVATION_CAUSE.clone(),
            INPUT_DATASET_ACTIVATION_CAUSE.clone()
        ]));

        let initiator_account_id = odf::AccountID::new_seeded_ed25519(b"different");
        assert!(
            MANUAL_ACTIVATION_CAUSE.is_unique_vs(&[FlowActivationCause::Manual(
                FlowActivationCauseManual {
                    activation_time: Utc::now(),
                    initiator_account_id,
                }
            )])
        );

        assert!(!MANUAL_ACTIVATION_CAUSE.is_unique_vs(&[MANUAL_ACTIVATION_CAUSE.clone()]));
    }

    #[test]
    fn test_is_unique_push() {
        assert!(PUSH_SOURCE_ACTIVATION_CAUSE.is_unique_vs(&[]));
        assert!(PUSH_SOURCE_ACTIVATION_CAUSE.is_unique_vs(&[
            AUTO_POLLING_ACTIVATION_CAUSE.clone(),
            MANUAL_ACTIVATION_CAUSE.clone(),
            INPUT_DATASET_ACTIVATION_CAUSE.clone()
        ]));

        assert!(
            PUSH_SOURCE_ACTIVATION_CAUSE.is_unique_vs(&[FlowActivationCause::Push(
                FlowActivationCausePush {
                    activation_time: Utc::now(),
                    source_name: Some("some-source".to_string()),
                    dataset_id: TEST_DATASET_ID.clone(),
                    result: DatasetPushResult::HttpIngest(DatasetPushHttpIngestResult {
                        old_head_maybe: None,
                        new_head: odf::Multihash::from_digest_sha3_256(b"some-slice")
                    }),
                }
            )])
        );

        assert!(
            !PUSH_SOURCE_ACTIVATION_CAUSE.is_unique_vs(&[PUSH_SOURCE_ACTIVATION_CAUSE.clone()])
        );
    }

    #[test]
    fn test_is_unique_input_dataset() {
        assert!(INPUT_DATASET_ACTIVATION_CAUSE.is_unique_vs(&[]));
        assert!(INPUT_DATASET_ACTIVATION_CAUSE.is_unique_vs(&[
            AUTO_POLLING_ACTIVATION_CAUSE.clone(),
            PUSH_SOURCE_ACTIVATION_CAUSE.clone(),
            MANUAL_ACTIVATION_CAUSE.clone()
        ]));

        // Test unrelated flow for same dataset
        assert!(INPUT_DATASET_ACTIVATION_CAUSE.is_unique_vs(&[
            FlowActivationCause::InputDatasetFlow(FlowActivationCauseInputDatasetFlow {
                activation_time: Utc::now(),
                dataset_id: TEST_DATASET_ID.clone(),
                flow_type: "SOME.OTHER.FLOW_TYPE".to_string(), // unrelated
                flow_id: FlowID::new(7),
                task_result: ts::TaskResult::empty()
            })
        ]));

        // Test same flow type for different dataset
        assert!(INPUT_DATASET_ACTIVATION_CAUSE.is_unique_vs(&[
            FlowActivationCause::InputDatasetFlow(FlowActivationCauseInputDatasetFlow {
                activation_time: Utc::now(),
                dataset_id: odf::DatasetID::new_seeded_ed25519(b"different"),
                flow_type: "SOME.FLOW.TYPE".to_string(),
                flow_id: FlowID::new(7),
                task_result: ts::TaskResult::empty()
            })
        ]));

        assert!(
            !INPUT_DATASET_ACTIVATION_CAUSE.is_unique_vs(&[INPUT_DATASET_ACTIVATION_CAUSE.clone()])
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
