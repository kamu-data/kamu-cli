// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowActivationCause {
    Manual(FlowActivationCauseManual),
    AutoPolling(FlowActivationCauseAutoPolling),
    DatasetUpdate(FlowActivationCauseDatasetUpdate),
}

impl FlowActivationCause {
    pub fn activation_time(&self) -> DateTime<Utc> {
        match self {
            Self::Manual(t) => t.activation_time,
            Self::AutoPolling(t) => t.activation_time,
            Self::DatasetUpdate(t) => t.activation_time,
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
        if let Self::DatasetUpdate(cause_update) = self {
            cause_update.push_source_name().cloned()
        } else {
            None
        }
    }

    pub fn activation_cause_description(&self) -> Option<String> {
        match self {
            Self::Manual(_) => Some("Flow activated manually".to_string()),
            Self::AutoPolling(_) => Some("Flow activated automatically".to_string()),
            Self::DatasetUpdate(cause_update) => match &cause_update.source {
                DatasetUpdateSource::UpstreamFlow { .. } => {
                    Some("Flow activated by upstream flow".to_string())
                }
                DatasetUpdateSource::HttpIngest { source_name } => Some(format!(
                    "Flow activated by root dataset ingest via HTTP endpoint: {source_name:?}"
                )),
                DatasetUpdateSource::SmartProtocolPush {
                    account_name,
                    is_force: _,
                } => {
                    if let Some(account_name) = account_name {
                        Some(format!(
                            "Flow activated via Smart Transfer Protocol push by account: \
                             {account_name})"
                        ))
                    } else {
                        Some(
                            "Flow activated via Smart Transfer Protocol push anonymously"
                                .to_string(),
                        )
                    }
                }
            },
        }
    }

    /// Checks if new cause is unique compared to the existing causes
    pub fn is_unique_vs(&self, existing_causes: &[FlowActivationCause]) -> bool {
        // Try finding a similar existing causes and abort early, when found
        for existing in existing_causes {
            match (self, existing) {
                // If both are manual, compare initiator accounts. One cause is enough per account
                (Self::Manual(this), Self::Manual(existing))
                    if this.initiator_account_id == existing.initiator_account_id =>
                {
                    return false;
                }
                // If both are auto-polling, they are the same, one cause is enough
                (Self::AutoPolling(_), Self::AutoPolling(_)) => return false,
                // If both are dataset updates, compare the structures, but any key attribute
                // difference means it's unique
                (Self::DatasetUpdate(this), Self::DatasetUpdate(existing)) => {
                    return this.dataset_id != existing.dataset_id
                        || this.source != existing.source
                        || this.new_head != existing.new_head
                        || this.old_head_maybe != existing.old_head_maybe
                        || this.blocks_added != existing.blocks_added
                        || this.records_added != existing.records_added
                        || this.had_breaking_changes != existing.had_breaking_changes
                        || this.new_watermark != existing.new_watermark;
                }
                _ => { /* Continue comparing */ }
            }
        }

        // No similar cause was found, so it's a truly unique one
        true
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowActivationCauseManual {
    pub activation_time: DateTime<Utc>,
    pub initiator_account_id: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type InitiatorIDStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<odf::AccountID, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowActivationCauseAutoPolling {
    pub activation_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowActivationCauseDatasetUpdate {
    pub activation_time: DateTime<Utc>,
    pub dataset_id: odf::DatasetID,
    pub source: DatasetUpdateSource,
    pub new_head: odf::Multihash,
    pub old_head_maybe: Option<odf::Multihash>,
    pub blocks_added: u64,
    pub records_added: u64,
    pub had_breaking_changes: bool,
    pub new_watermark: Option<DateTime<Utc>>,
}

impl FlowActivationCauseDatasetUpdate {
    pub fn push_source_name(&self) -> Option<&String> {
        match &self.source {
            DatasetUpdateSource::HttpIngest { source_name } => source_name.as_ref(),
            DatasetUpdateSource::SmartProtocolPush { .. }
            | DatasetUpdateSource::UpstreamFlow { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum DatasetUpdateSource {
    UpstreamFlow {
        flow_type: String,
        flow_id: FlowID,
    },
    HttpIngest {
        source_name: Option<String>,
    },
    SmartProtocolPush {
        account_name: Option<odf::AccountName>,
        is_force: bool,
    },
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
        FlowActivationCause::DatasetUpdate(FlowActivationCauseDatasetUpdate {
            activation_time: Utc::now(),
            source: DatasetUpdateSource::HttpIngest {
                source_name: Some("some-source".to_string()),
            },
            dataset_id: TEST_DATASET_ID.clone(),
            old_head_maybe: None,
            new_head: odf::Multihash::from_digest_sha3_256(b"some-slice"),
            blocks_added: 1,
            records_added: 5,
            had_breaking_changes: false,
            new_watermark: None,
        })
    });
    static INPUT_DATASET_ACTIVATION_CAUSE: LazyLock<FlowActivationCause> = LazyLock::new(|| {
        FlowActivationCause::DatasetUpdate(FlowActivationCauseDatasetUpdate {
            activation_time: Utc::now(),
            source: DatasetUpdateSource::UpstreamFlow {
                flow_type: "SOME.FLOW.TYPE".to_string(),
                flow_id: FlowID::new(5),
            },
            dataset_id: TEST_DATASET_ID.clone(),
            old_head_maybe: None,
            new_head: odf::Multihash::from_digest_sha3_256(b"some-other-slice"),
            blocks_added: 2,
            records_added: 3,
            had_breaking_changes: false,
            new_watermark: None,
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
            PUSH_SOURCE_ACTIVATION_CAUSE.is_unique_vs(&[FlowActivationCause::DatasetUpdate(
                FlowActivationCauseDatasetUpdate {
                    activation_time: Utc::now(),
                    source: DatasetUpdateSource::HttpIngest {
                        source_name: Some("some-source".to_string()),
                    },
                    dataset_id: TEST_DATASET_ID.clone(),
                    old_head_maybe: None,
                    new_head: odf::Multihash::from_digest_sha3_256(b"some-slice"),
                    blocks_added: 2,
                    records_added: 5,
                    had_breaking_changes: false,
                    new_watermark: None,
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
            FlowActivationCause::DatasetUpdate(FlowActivationCauseDatasetUpdate {
                activation_time: Utc::now(),
                dataset_id: TEST_DATASET_ID.clone(),
                source: DatasetUpdateSource::UpstreamFlow {
                    flow_type: "SOME.OTHER.FLOW_TYPE".to_string(),
                    flow_id: FlowID::new(7),
                },
                old_head_maybe: None,
                new_head: odf::Multihash::from_digest_sha3_256(b"some-other-slice"),
                blocks_added: 1,
                records_added: 3,
                had_breaking_changes: false,
                new_watermark: None,
            })
        ]));

        // Test same flow type for different dataset
        assert!(INPUT_DATASET_ACTIVATION_CAUSE.is_unique_vs(&[
            FlowActivationCause::DatasetUpdate(FlowActivationCauseDatasetUpdate {
                activation_time: Utc::now(),
                dataset_id: odf::DatasetID::new_seeded_ed25519(b"different"),
                source: DatasetUpdateSource::UpstreamFlow {
                    flow_type: "SOME.FLOW.TYPE".to_string(),
                    flow_id: FlowID::new(8),
                },
                old_head_maybe: None,
                new_head: odf::Multihash::from_digest_sha3_256(b"some-totally-different-slice"),
                blocks_added: 2,
                records_added: 3,
                had_breaking_changes: false,
                new_watermark: None,
            })
        ]));

        assert!(
            !INPUT_DATASET_ACTIVATION_CAUSE.is_unique_vs(&[INPUT_DATASET_ACTIVATION_CAUSE.clone()])
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
