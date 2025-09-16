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
                flow_type,
                scope_data as "scope_data: _",
                paused_manual,
                stop_policy_kind,
                stop_policy_data as "stop_policy_data: _",
                consecutive_failures,
                last_success_at as "last_success_at: _",
                last_failure_at as "last_failure_at: _",
                last_attempt_at as "last_attempt_at: _",
                next_planned_at as "next_planned_at: _",
                effective_state,
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

    Ok(row.try_into()?)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn load_multiple_process_states(
    connection_mut: &mut SqliteConnection,
    flow_bindings: &[FlowBinding],
) -> Result<Vec<(FlowBinding, FlowProcessState)>, InternalError> {
    if flow_bindings.is_empty() {
        return Ok(Vec::new());
    }

    // Prepare the query parameters
    let mut flow_types = Vec::new();
    let mut scope_data_jsons = Vec::new();

    for binding in flow_bindings {
        let scope_json = serde_json::to_value(&binding.scope).int_err()?;
        let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

        flow_types.push(binding.flow_type.clone());
        scope_data_jsons.push(scope_data_json);
    }

    // Build the query with multiple OR conditions
    let mut query_parts = Vec::new();
    for i in 0..flow_bindings.len() {
        query_parts.push(format!(
            "(flow_type = ${} AND scope_data = ${})",
            i * 2 + 1,
            i * 2 + 2
        ));
    }
    let where_clause = query_parts.join(" OR ");

    let query_sql = format!(
        r#"
            SELECT
                flow_type,
                scope_data as "scope_data: _",
                paused_manual,
                stop_policy_kind,
                stop_policy_data as "stop_policy_data: _",
                consecutive_failures,
                last_success_at as "last_success_at: _",
                last_failure_at as "last_failure_at: _",
                last_attempt_at as "last_attempt_at: _",
                next_planned_at as "next_planned_at: _",
                effective_state,
                updated_at as "updated_at: _",
                last_applied_trigger_event_id,
                last_applied_flow_event_id
            FROM flow_process_states
            WHERE {where_clause}
            "#,
    );

    // Build the query with parameters
    let mut query = sqlx::query_as::<_, SqliteFlowProcessStateRowModel>(&query_sql);
    for (flow_type, scope_data_json) in flow_types.iter().zip(scope_data_jsons.iter()) {
        query = query.bind(flow_type).bind(scope_data_json);
    }

    let rows = query.fetch_all(connection_mut).await.int_err()?;

    // Convert rows to process states and reconstruct bindings from database data
    let mut results = Vec::new();
    for row in rows {
        // Reconstruct the binding from the database row
        let binding = FlowBinding {
            flow_type: row.flow_type.clone(),
            scope: serde_json::from_value(row.scope_data.clone()).int_err()?,
        };

        let process_state = row.try_into().int_err()?;
        results.push((binding, process_state));
    }

    Ok(results)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
