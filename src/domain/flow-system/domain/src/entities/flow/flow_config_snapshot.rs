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
/// hold the bare rule
#[derive(Deserialize)]
#[serde(untagged)]
enum StoredFlowConfigSnapshot {
    Current(FlowConfigSnapshot),
    Legacy(FlowConfigurationRule),
}

impl StoredFlowConfigSnapshot {
    fn into_snapshot(self, legacy_origin: FlowConfigSnapshotOrigin) -> FlowConfigSnapshot {
        match self {
            Self::Current(snapshot) => snapshot,
            Self::Legacy(rule) => FlowConfigSnapshot {
                rule,
                origin: legacy_origin,
            },
        }
    }
}

/// Initial snapshots were only forced by the caller when given explicitly,
/// so a bare rule is treated as taken from the configuration
pub(crate) fn deserialize_initial_config_snapshot<'de, D>(
    deserializer: D,
) -> Result<Option<FlowConfigSnapshot>, D::Error>
where
    D: Deserializer<'de>,
{
    let maybe_stored = Option::<StoredFlowConfigSnapshot>::deserialize(deserializer)?;
    Ok(maybe_stored.map(|stored| stored.into_snapshot(FlowConfigSnapshotOrigin::Configuration)))
}

/// Snapshots were only modified when forced by the caller, so a bare rule is
/// treated as forced
pub(crate) fn deserialize_modified_config_snapshot<'de, D>(
    deserializer: D,
) -> Result<FlowConfigSnapshot, D::Error>
where
    D: Deserializer<'de>,
{
    let stored = StoredFlowConfigSnapshot::deserialize(deserializer)?;
    Ok(stored.into_snapshot(FlowConfigSnapshotOrigin::Forced))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
