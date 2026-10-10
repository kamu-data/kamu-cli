// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Deserializer, Serialize};

use crate::FlowConfigurationRule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Configuration rule a flow runs with, and where it came from
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfigSnapshot {
    pub rule: FlowConfigurationRule,
    pub origin: FlowConfigSnapshotOrigin,
}

impl FlowConfigSnapshot {
    pub fn configured(rule: FlowConfigurationRule) -> Self {
        Self {
            rule,
            origin: FlowConfigSnapshotOrigin::Configuration,
        }
    }

    pub fn forced(rule: FlowConfigurationRule) -> Self {
        Self {
            rule,
            origin: FlowConfigSnapshotOrigin::Forced,
        }
    }

    pub fn is_forced(&self) -> bool {
        match self.origin {
            FlowConfigSnapshotOrigin::Forced => true,
            FlowConfigSnapshotOrigin::Configuration => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowConfigSnapshotOrigin {
    /// Taken from the flow configuration, so it follows configuration changes
    Configuration,
    /// Passed by the caller of the flow run, so configuration changes keep it
    Forced,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Stored form of a snapshot: events written before snapshots had an origin
/// hold the bare rule, whether it came from the configuration or was forced.
/// Such a snapshot reads as forced, so it stays as frozen as it was written
#[derive(Deserialize)]
#[serde(untagged)]
enum StoredFlowConfigSnapshot {
    Current(FlowConfigSnapshot),
    Legacy(FlowConfigurationRule),
}

impl From<StoredFlowConfigSnapshot> for FlowConfigSnapshot {
    fn from(stored: StoredFlowConfigSnapshot) -> Self {
        match stored {
            StoredFlowConfigSnapshot::Current(snapshot) => snapshot,
            StoredFlowConfigSnapshot::Legacy(rule) => Self::forced(rule),
        }
    }
}

pub(crate) fn deserialize_initial_config_snapshot<'de, D>(
    deserializer: D,
) -> Result<Option<FlowConfigSnapshot>, D::Error>
where
    D: Deserializer<'de>,
{
    let maybe_stored = Option::<StoredFlowConfigSnapshot>::deserialize(deserializer)?;
    Ok(maybe_stored.map(Into::into))
}

pub(crate) fn deserialize_modified_config_snapshot<'de, D>(
    deserializer: D,
) -> Result<FlowConfigSnapshot, D::Error>
where
    D: Deserializer<'de>,
{
    StoredFlowConfigSnapshot::deserialize(deserializer).map(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;

    use crate::{
        FlowActivationCause,
        FlowActivationCauseAutoPolling,
        FlowBinding,
        FlowConfigSnapshot,
        FlowConfigSnapshotModified,
        FlowConfigurationRule,
        FlowEventInitiated,
        FlowID,
        FlowScope,
    };

    fn ingest_rule() -> FlowConfigurationRule {
        FlowConfigurationRule {
            rule_type: "IngestRule".to_string(),
            payload: json!({ "fetch_uncacheable": true, "fetch_next_iteration": false }),
        }
    }

    fn initiated_event(config_snapshot: Option<FlowConfigSnapshot>) -> FlowEventInitiated {
        let now = Utc::now();
        FlowEventInitiated {
            event_time: now,
            flow_id: FlowID::new(1),
            flow_binding: FlowBinding::new("test-flow", FlowScope::make_system_scope()),
            activation_cause: FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
                activation_time: now,
            }),
            config_snapshot,
            retry_policy: None,
        }
    }

    fn modified_event(config_snapshot: FlowConfigSnapshot) -> FlowConfigSnapshotModified {
        FlowConfigSnapshotModified {
            event_time: Utc::now(),
            flow_id: FlowID::new(1),
            flow_binding: FlowBinding::new("test-flow", FlowScope::make_system_scope()),
            config_snapshot,
        }
    }

    /// Replaces the stored snapshot with the bare rule, as written before
    /// snapshots had an origin
    fn with_legacy_snapshot(mut stored: serde_json::Value) -> serde_json::Value {
        stored["config_snapshot"] = serde_json::to_value(ingest_rule()).unwrap();
        stored
    }

    #[test]
    fn test_legacy_initial_snapshot_is_forced() {
        let stored = with_legacy_snapshot(
            serde_json::to_value(initiated_event(Some(FlowConfigSnapshot::configured(
                ingest_rule(),
            ))))
            .unwrap(),
        );

        let event: FlowEventInitiated = serde_json::from_value(stored).unwrap();

        assert_eq!(
            Some(FlowConfigSnapshot::forced(ingest_rule())),
            event.config_snapshot
        );
    }

    #[test]
    fn test_legacy_modified_snapshot_is_forced() {
        let stored = with_legacy_snapshot(
            serde_json::to_value(modified_event(
                FlowConfigSnapshot::configured(ingest_rule()),
            ))
            .unwrap(),
        );

        let event: FlowConfigSnapshotModified = serde_json::from_value(stored).unwrap();

        assert_eq!(
            FlowConfigSnapshot::forced(ingest_rule()),
            event.config_snapshot
        );
    }

    #[test]
    fn test_missing_initial_snapshot_stays_missing() {
        let stored = serde_json::to_value(initiated_event(None)).unwrap();

        let event: FlowEventInitiated = serde_json::from_value(stored).unwrap();

        assert_eq!(None, event.config_snapshot);
    }

    #[test]
    fn test_snapshot_keeps_its_origin_when_stored() {
        for snapshot in [
            FlowConfigSnapshot::configured(ingest_rule()),
            FlowConfigSnapshot::forced(ingest_rule()),
        ] {
            let initiated: FlowEventInitiated = serde_json::from_value(
                serde_json::to_value(initiated_event(Some(snapshot.clone()))).unwrap(),
            )
            .unwrap();
            assert_eq!(Some(snapshot.clone()), initiated.config_snapshot);

            let modified: FlowConfigSnapshotModified = serde_json::from_value(
                serde_json::to_value(modified_event(snapshot.clone())).unwrap(),
            )
            .unwrap();
            assert_eq!(snapshot, modified.config_snapshot);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
