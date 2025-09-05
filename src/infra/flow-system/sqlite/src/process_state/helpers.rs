// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::*;
use sqlx::SqliteConnection;

use crate::SqliteFlowProcessStateRowModel;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn load_process_state(
    connection_mut: &mut SqliteConnection,
    flow_binding: &FlowBinding,
) -> Result<FlowProcessState, FlowProcessLoadError> {
    let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
    let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

    let maybe_row = sqlx::query_as!(
        SqliteFlowProcessStateRowModel,
        r#"
            SELECT
                paused_manual,
                stop_policy_kind,
                stop_policy_data as "stop_policy_data: _",
                consecutive_failures,
                last_success_at as "last_success_at: _",
                last_failure_at as "last_failure_at: _",
                last_attempt_at as "last_attempt_at: _",
                next_planned_at as "next_planned_at: _",
                effective_state,
                sort_key,
                updated_at as "updated_at: _",
                last_applied_trigger_event_id,
                last_applied_flow_event_id
            FROM flow_process_states
            WHERE
                flow_type = $1 AND scope_data = $2
            "#,
        flow_binding.flow_type,
        scope_data_json,
    )
    .fetch_optional(connection_mut)
    .await
    .int_err()?;

    let row = maybe_row.ok_or_else(|| {
        FlowProcessLoadError::NotFound(FlowProcessNotFoundError {
            flow_binding: flow_binding.clone(),
        })
    })?;

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
    let effective_state = FlowProcessEffectiveState::from_str(&row.effective_state).int_err()?;

    let state = FlowProcessState::from_storage_row(
        flow_binding.clone(),
        row.sort_key,
        row.paused_manual != 0,
        stop_policy,
        u32::try_from(row.consecutive_failures).unwrap(),
        row.last_success_at,
        row.last_failure_at,
        row.last_attempt_at,
        row.next_planned_at,
        effective_state,
        row.updated_at,
        EventID::new(row.last_applied_trigger_event_id),
        EventID::new(row.last_applied_flow_event_id),
    );

    Ok(state)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
