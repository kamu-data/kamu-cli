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
    FlowProcessAutoStopReason,
    FlowProcessEffectiveState,
    FlowProcessState,
    FlowProcessUserIntent,
    FlowTriggerStopPolicy,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct PostgresFlowProcessStateRowModel {
    pub flow_type: String,
    pub scope_data: serde_json::Value,
    pub user_intent: FlowProcessUserIntent,
    pub stop_policy_kind: PostgresFlowStopPolicyKind,
    pub stop_policy_data: Option<serde_json::Value>,
    pub consecutive_failures: i32,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub next_planned_at: Option<DateTime<Utc>>,
    pub paused_at: Option<DateTime<Utc>>,
    pub running_since: Option<DateTime<Utc>>,
    pub auto_stopped_at: Option<DateTime<Utc>>,
    pub effective_state: FlowProcessEffectiveState,
    pub auto_stopped_reason: Option<FlowProcessAutoStopReason>,
    pub updated_at: DateTime<Utc>,
    pub last_applied_flow_system_event_id: i64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TryFrom<PostgresFlowProcessStateRowModel> for FlowProcessState {
    type Error = InternalError;

    fn try_from(row: PostgresFlowProcessStateRowModel) -> Result<Self, Self::Error> {
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

        match stop_policy {
            FlowTriggerStopPolicy::AfterConsecutiveFailures { .. } => assert_eq!(
                row.stop_policy_kind,
                PostgresFlowStopPolicyKind::AfterConsecutiveFailures,
                "Inconsistent stop policy kind and data in the database",
            ),
            FlowTriggerStopPolicy::Never => assert_eq!(
                row.stop_policy_kind,
                PostgresFlowStopPolicyKind::Never,
                "Inconsistent stop policy kind and data in the database",
            ),
        }

        Self::rehydrate_from_snapshot(
            flow_binding,
            row.user_intent,
            stop_policy,
            u32::try_from(row.consecutive_failures).unwrap(),
            row.last_success_at,
            row.last_failure_at,
            row.last_attempt_at,
            row.next_planned_at,
            row.paused_at,
            row.running_since,
            row.auto_stopped_at,
            row.effective_state,
            row.auto_stopped_reason,
            row.updated_at,
            EventID::new(row.last_applied_flow_system_event_id),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, sqlx::Type, Eq, PartialEq)]
#[sqlx(type_name = "flow_stop_policy_kind", rename_all = "snake_case")]
pub enum PostgresFlowStopPolicyKind {
    Never,
    AfterConsecutiveFailures,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
