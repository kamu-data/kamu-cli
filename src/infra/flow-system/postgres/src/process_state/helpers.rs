// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::*;
use sqlx::PgConnection;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn load_process_state(
    connection_mut: &mut PgConnection,
    flow_binding: &FlowBinding,
) -> Result<FlowProcessState, FlowProcessLoadError> {
    let scope_data_json = serde_json::to_value(&flow_binding.scope).int_err()?;

    let maybe_row = sqlx::query!(
        r#"
            SELECT
                paused_manual,
                stop_policy_kind as "stop_policy_kind: String",
                stop_policy_data,
                consecutive_failures,
                last_success_at,
                last_failure_at,
                last_attempt_at,
                next_planned_at,
                effective_state as "effective_state: FlowProcessEffectiveState",
                sort_key,
                updated_at,
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

    let state = FlowProcessState::from_storage_row(
        flow_binding.clone(),
        row.sort_key,
        row.paused_manual,
        stop_policy,
        u32::try_from(row.consecutive_failures).unwrap(),
        row.last_success_at,
        row.last_failure_at,
        row.last_attempt_at,
        row.next_planned_at,
        row.effective_state,
        row.updated_at,
        EventID::new(row.last_applied_trigger_event_id),
        EventID::new(row.last_applied_flow_event_id),
    );

    Ok(state)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
