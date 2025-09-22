// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_flow_system::{
    EventID,
    FlowBinding,
    FlowProcessEffectiveState,
    FlowProcessState,
    FlowTriggerStopPolicy,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct SqliteFlowProcessStateRowModel {
    pub flow_type: String,
    pub scope_data: serde_json::Value,
    pub paused_manual: i64,
    pub stop_policy_kind: String,
    pub stop_policy_data: Option<serde_json::Value>,
    pub consecutive_failures: i64,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub next_planned_at: Option<DateTime<Utc>>,
    pub effective_state: String,
    pub updated_at: DateTime<Utc>,
    pub last_applied_flow_system_event_id: i64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<SqliteFlowProcessStateRowModel> for FlowProcessState {
    type Error = InternalError;

    fn try_from(row: SqliteFlowProcessStateRowModel) -> Result<Self, Self::Error> {
        use internal_error::ResultIntoInternal;

        let flow_binding = FlowBinding {
            flow_type: row.flow_type,
            scope: serde_json::from_value(row.scope_data).int_err()?,
        };

        let stop_policy = row
            .stop_policy_data
            .map(serde_json::from_value::<FlowTriggerStopPolicy>)
            .transpose()
            .int_err()?
            .unwrap_or_default();

        assert_eq!(
            stop_policy.kind_to_string(),
            row.stop_policy_kind,
            "Inconsistent stop policy kind and data in the database",
        );

        use std::str::FromStr;
        let effective_state =
            FlowProcessEffectiveState::from_str(&row.effective_state).int_err()?;

        Self::rehydrate_from_snapshot(
            flow_binding,
            row.paused_manual != 0,
            stop_policy,
            u32::try_from(row.consecutive_failures).unwrap(),
            row.last_success_at,
            row.last_failure_at,
            row.last_attempt_at,
            row.next_planned_at,
            effective_state,
            row.updated_at,
            EventID::new(row.last_applied_flow_system_event_id),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
