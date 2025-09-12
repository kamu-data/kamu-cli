// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use kamu_flow_system::*;
use sqlx::Postgres;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowProcessStateRepository {
    transaction: TransactionRefT<Postgres>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateRepository)]
impl PostgresFlowProcessStateRepository {
    pub fn new(transaction: TransactionRef, time_source: Arc<dyn SystemTimeSource>) -> Self {
        Self {
            transaction: transaction.into(),
            time_source,
        }
    }

    async fn load_process_state(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<FlowProcessState, FlowProcessLoadError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        crate::process_state::helpers::load_process_state(connection_mut, flow_binding).await
    }

    async fn save_process_state(
        &self,
        state: &FlowProcessState,
        expected_last_trigger_event_id: EventID,
        expected_last_flow_event_id: EventID,
    ) -> Result<(), FlowProcessSaveError> {
        let scope_data_json = serde_json::to_value(&state.flow_binding().scope).int_err()?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let result = sqlx::query!(
            r#"
            UPDATE flow_process_states
                SET
                    paused_manual = $1,
                    stop_policy_kind = $2::flow_stop_policy_kind,
                    stop_policy_data = $3,
                    consecutive_failures = $4,
                    last_success_at = $5,
                    last_failure_at = $6,
                    last_attempt_at = $7,
                    next_planned_at = $8,
                    effective_state = $9,
                    updated_at = $10,
                    last_applied_trigger_event_id = $11,
                    last_applied_flow_event_id = $12
                WHERE
                    flow_type = $13 AND scope_data = $14 AND
                    last_applied_trigger_event_id = $15 AND
                    last_applied_flow_event_id = $16
            "#,
            state.paused_manual(),
            state.stop_policy().kind_to_string() as &str,
            serde_json::to_value(state.stop_policy()).int_err()?,
            i32::try_from(state.consecutive_failures()).unwrap(),
            state.last_success_at(),
            state.last_failure_at(),
            state.last_attempt_at(),
            state.next_planned_at(),
            state.effective_state() as FlowProcessEffectiveState,
            state.updated_at(),
            state.last_applied_trigger_event_id().into_inner(),
            state.last_applied_flow_event_id().into_inner(),
            state.flow_binding().flow_type,
            scope_data_json,
            expected_last_trigger_event_id.into_inner(),
            expected_last_flow_event_id.into_inner(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        if result.rows_affected() == 0 {
            return Err(FlowProcessSaveError::ConcurrentModification(
                FlowProcessConcurrentModificationError {
                    flow_binding: state.flow_binding().clone(),
                },
            ));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateRepository for PostgresFlowProcessStateRepository {
    async fn upsert_process_state_on_trigger_event(
        &self,
        trigger_event_id: EventID,
        flow_binding: FlowBinding,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<(), FlowProcessUpsertError> {
        // Load current state
        match self.load_process_state(&flow_binding).await {
            // Got existing row => update it
            Ok(mut process_state) => {
                // Backup current event IDs for concurrency check
                let current_trigger_event_id = process_state.last_applied_trigger_event_id();
                let current_flow_event_id = process_state.last_applied_flow_event_id();

                // Apply updates in memory
                process_state
                    .update_trigger_state(
                        trigger_event_id,
                        self.time_source.now(),
                        paused_manual,
                        stop_policy,
                    )
                    .int_err()?;

                // Try saving back
                match self
                    .save_process_state(
                        &process_state,
                        current_trigger_event_id,
                        current_flow_event_id,
                    )
                    .await
                {
                    Ok(()) => Ok(()),
                    Err(FlowProcessSaveError::ConcurrentModification(e)) => {
                        Err(FlowProcessUpsertError::ConcurrentModification(e))
                    }
                    Err(FlowProcessSaveError::Internal(e)) => {
                        Err(FlowProcessUpsertError::Internal(e))
                    }
                }
            }

            // No existing row, create new
            Err(FlowProcessLoadError::NotFound(_)) => {
                let state = FlowProcessState::new(
                    trigger_event_id,
                    self.time_source.now(),
                    flow_binding,
                    paused_manual,
                    stop_policy,
                );

                let scope_data_json =
                    serde_json::to_value(&state.flow_binding().scope).int_err()?;

                let stop_policy_kind = state.stop_policy().kind_to_string();
                let stop_policy_data = serde_json::to_value(state.stop_policy()).int_err()?;

                let mut tr = self.transaction.lock().await;
                let connection_mut = tr.connection_mut().await?;

                let result = sqlx::query!(
                    r#"
                    INSERT INTO flow_process_states (
                        scope_data,
                        flow_type,
                        paused_manual,
                        stop_policy_kind,
                        stop_policy_data,
                        consecutive_failures,
                        last_success_at,
                        last_failure_at,
                        last_attempt_at,
                        next_planned_at,
                        effective_state,
                        updated_at,
                        last_applied_trigger_event_id,
                        last_applied_flow_event_id
                    )
                    VALUES ($1, $2, $3, $4::flow_stop_policy_kind, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (flow_type, scope_data) DO NOTHING
                    "#,
                    scope_data_json,
                    state.flow_binding().flow_type,
                    state.paused_manual(),
                    stop_policy_kind as &str,
                    stop_policy_data,
                    i32::try_from(state.consecutive_failures()).unwrap(),
                    state.last_success_at(),
                    state.last_failure_at(),
                    state.last_attempt_at(),
                    state.next_planned_at(),
                    state.effective_state() as FlowProcessEffectiveState,
                    state.updated_at(),
                    state.last_applied_trigger_event_id().into_inner(),
                    state.last_applied_flow_event_id().into_inner(),
                )
                .execute(connection_mut)
                .await
                .int_err()?;

                if result.rows_affected() == 0 {
                    return Err(FlowProcessUpsertError::ConcurrentModification(
                        FlowProcessConcurrentModificationError {
                            flow_binding: state.flow_binding().clone(),
                        },
                    ));
                }

                Ok(())
            }

            // Bad error
            Err(FlowProcessLoadError::Internal(e)) => Err(FlowProcessUpsertError::Internal(e)),
        }
    }

    async fn apply_flow_result(
        &self,
        flow_event_id: EventID,
        flow_binding: &FlowBinding,
        success: bool,
        event_time: DateTime<Utc>,
    ) -> Result<(), FlowProcessFlowEventError> {
        // Load current state
        let mut process_state = match self.load_process_state(flow_binding).await {
            Ok(state) => state,
            Err(FlowProcessLoadError::NotFound(_)) => {
                // Skip this flow, must be a manual launch without any trigger existing
                return Ok(());
            }
            Err(FlowProcessLoadError::Internal(e)) => {
                return Err(FlowProcessFlowEventError::Internal(e));
            }
        };

        // Backup current event IDs for concurrency check
        let current_trigger_event_id = process_state.last_applied_trigger_event_id();
        let current_flow_event_id = process_state.last_applied_flow_event_id();

        // Apply updates in memory
        if success {
            process_state
                .on_success(flow_event_id, self.time_source.now(), event_time)
                .int_err()?;
        } else {
            process_state
                .on_failure(flow_event_id, self.time_source.now(), event_time)
                .int_err()?;
        }

        // Try saving back
        match self
            .save_process_state(
                &process_state,
                current_trigger_event_id,
                current_flow_event_id,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(FlowProcessSaveError::ConcurrentModification(e)) => {
                Err(FlowProcessFlowEventError::ConcurrentModification(e))
            }
            Err(FlowProcessSaveError::Internal(e)) => Err(FlowProcessFlowEventError::Internal(e)),
        }
    }

    async fn on_flow_scheduled(
        &self,
        flow_event_id: EventID,
        flow_binding: &FlowBinding,
        planned_at: DateTime<Utc>,
    ) -> Result<(), FlowProcessFlowEventError> {
        // Load current state
        let mut process_state = match self.load_process_state(flow_binding).await {
            Ok(state) => state,
            Err(FlowProcessLoadError::NotFound(_)) => {
                // Skip this flow, must be a manual launch without any trigger existing
                return Ok(());
            }
            Err(FlowProcessLoadError::Internal(e)) => {
                return Err(FlowProcessFlowEventError::Internal(e));
            }
        };

        // Backup current event IDs for concurrency check
        let current_trigger_event_id = process_state.last_applied_trigger_event_id();
        let current_flow_event_id = process_state.last_applied_flow_event_id();

        // Apply flow scheduling
        process_state
            .on_scheduled(flow_event_id, self.time_source.now(), planned_at)
            .int_err()?;

        // Try saving back
        match self
            .save_process_state(
                &process_state,
                current_trigger_event_id,
                current_flow_event_id,
            )
            .await
        {
            Ok(()) => Ok(()),
            Err(FlowProcessSaveError::ConcurrentModification(e)) => {
                Err(FlowProcessFlowEventError::ConcurrentModification(e))
            }
            Err(FlowProcessSaveError::Internal(e)) => Err(FlowProcessFlowEventError::Internal(e)),
        }
    }

    async fn delete_process_states_by_scope(
        &self,
        scope: &FlowScope,
    ) -> Result<(), FlowProcessDeleteError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let scope_data_json = serde_json::to_value(scope).int_err()?;

        sqlx::query!(
            r#"
            DELETE FROM flow_process_states
                WHERE scope_data = $1
            "#,
            scope_data_json,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
