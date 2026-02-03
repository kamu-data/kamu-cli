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
use database_common::TransactionRefT;
use dill::{component, interface};
use kamu_flow_system::*;
use sqlx::Postgres;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateRepository)]
pub struct PostgresFlowProcessStateRepository {
    transaction: TransactionRefT<Postgres>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PostgresFlowProcessStateRepository {
    async fn create_process_state(
        &self,
        process_state: &FlowProcessState,
    ) -> Result<(), FlowProcessSaveError> {
        let scope_data_json =
            serde_json::to_value(&process_state.flow_binding().scope).int_err()?;

        let stop_policy_kind = process_state.stop_policy().kind_to_string();
        let stop_policy_data = serde_json::to_value(process_state.stop_policy()).int_err()?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            INSERT INTO flow_process_states (
                scope_data,
                flow_type,
                user_intent,
                stop_policy_kind,
                stop_policy_data,
                consecutive_failures,
                last_success_at,
                last_failure_at,
                last_attempt_at,
                next_planned_at,
                paused_at,
                running_since,
                auto_stopped_at,
                effective_state,
                auto_stopped_reason,
                updated_at,
                last_applied_flow_system_event_id
            )
            VALUES ($1, $2, $3::flow_process_user_intent, $4::flow_stop_policy_kind, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14::flow_process_effective_state, $15::flow_process_auto_stop_reason, $16, $17)
            ON CONFLICT (flow_type, scope_data) DO NOTHING
            "#,
            scope_data_json,
            process_state.flow_binding().flow_type,
            process_state.user_intent() as FlowProcessUserIntent,
            stop_policy_kind as &str,
            stop_policy_data,
            i32::try_from(process_state.consecutive_failures()).unwrap(),
            process_state.last_success_at(),
            process_state.last_failure_at(),
            process_state.last_attempt_at(),
            process_state.next_planned_at(),
            process_state.paused_at(),
            process_state.running_since(),
            process_state.auto_stopped_at(),
            process_state.effective_state() as FlowProcessEffectiveState,
            process_state.auto_stopped_reason() as Option<FlowProcessAutoStopReason>,
            process_state.updated_at(),
            process_state.last_applied_event_id().into_inner(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        if result.rows_affected() == 0 {
            return Err(FlowProcessSaveError::ConcurrentModification(
                FlowProcessConcurrentModificationError {
                    flow_binding: process_state.flow_binding().clone(),
                },
            ));
        }

        Ok(())
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
        expected_last_event_id: EventID,
    ) -> Result<(), FlowProcessSaveError> {
        let scope_data_json = serde_json::to_value(&state.flow_binding().scope).int_err()?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let result = sqlx::query!(
            r#"
            UPDATE flow_process_states
                SET
                    user_intent = $1::flow_process_user_intent,
                    stop_policy_kind = $2::flow_stop_policy_kind,
                    stop_policy_data = $3,
                    consecutive_failures = $4,
                    last_success_at = $5,
                    last_failure_at = $6,
                    last_attempt_at = $7,
                    next_planned_at = $8,
                    paused_at = $9,
                    running_since = $10,
                    auto_stopped_at = $11,
                    effective_state = $12,
                    auto_stopped_reason = $13,
                    updated_at = $14,
                    last_applied_flow_system_event_id = $15
                WHERE
                    flow_type = $16 AND scope_data = $17 AND
                    last_applied_flow_system_event_id = $18
            "#,
            state.user_intent() as FlowProcessUserIntent,
            state.stop_policy().kind_to_string() as &str,
            serde_json::to_value(state.stop_policy()).int_err()?,
            i32::try_from(state.consecutive_failures()).unwrap(),
            state.last_success_at(),
            state.last_failure_at(),
            state.last_attempt_at(),
            state.next_planned_at(),
            state.paused_at(),
            state.running_since(),
            state.auto_stopped_at(),
            state.effective_state() as FlowProcessEffectiveState,
            state.auto_stopped_reason() as Option<FlowProcessAutoStopReason>,
            state.updated_at(),
            state.last_applied_event_id().into_inner(),
            state.flow_binding().flow_type,
            scope_data_json,
            expected_last_event_id.into_inner(),
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

    async fn upsert_process_state_for_flow_event(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<FlowProcessState, FlowProcessFlowEventError> {
        let process_state = match self.load_process_state(flow_binding).await {
            Ok(state) => state,
            Err(FlowProcessLoadError::NotFound(_)) => {
                // Auto-create a process state if it doesn't exist yet (manual launch)
                let new_process_state =
                    FlowProcessState::unconfigured(self.time_source.now(), flow_binding.clone());
                self.create_process_state(&new_process_state).await?;
                new_process_state
            }
            Err(FlowProcessLoadError::Internal(e)) => {
                return Err(FlowProcessFlowEventError::Internal(e));
            }
        };

        Ok(process_state)
    }

    async fn save_process_state_from_flow_event(
        &self,
        state: &FlowProcessState,
        expected_last_event_id: EventID,
    ) -> Result<(), FlowProcessFlowEventError> {
        match self.save_process_state(state, expected_last_event_id).await {
            Ok(()) => Ok(()),
            Err(FlowProcessSaveError::ConcurrentModification(e)) => {
                Err(FlowProcessFlowEventError::ConcurrentModification(e))
            }
            Err(FlowProcessSaveError::Internal(e)) => Err(FlowProcessFlowEventError::Internal(e)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateRepository for PostgresFlowProcessStateRepository {
    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding, ?paused_manual, ?stop_policy))]
    async fn upsert_process_state_on_trigger_event(
        &self,
        event_id: EventID,
        flow_binding: FlowBinding,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<FlowProcessState, FlowProcessUpsertError> {
        // Load current state
        match self.load_process_state(&flow_binding).await {
            // Got existing row => update it
            Ok(mut process_state) => {
                // Backup current event ID for concurrency check
                let current_event_id = process_state.last_applied_event_id();

                // Apply updates in memory
                process_state
                    .update_trigger_state(
                        event_id,
                        self.time_source.now(),
                        paused_manual,
                        stop_policy,
                    )
                    .int_err()?;

                // Try saving back
                match self
                    .save_process_state(&process_state, current_event_id)
                    .await
                {
                    Ok(()) => Ok(process_state),
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
                let user_intent = if paused_manual {
                    FlowProcessUserIntent::Paused
                } else {
                    FlowProcessUserIntent::Enabled
                };
                let process_state = FlowProcessState::new(
                    event_id,
                    self.time_source.now(),
                    flow_binding,
                    user_intent,
                    stop_policy,
                );

                self.create_process_state(&process_state).await?;
                Ok(process_state)
            }

            // Bad error
            Err(FlowProcessLoadError::Internal(e)) => Err(FlowProcessUpsertError::Internal(e)),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
    async fn apply_flow_result(
        &self,
        event_id: EventID,
        flow_binding: &FlowBinding,
        flow_outcome: &FlowOutcome,
        event_time: DateTime<Utc>,
    ) -> Result<FlowProcessState, FlowProcessFlowEventError> {
        // Acquire state
        let mut process_state = self
            .upsert_process_state_for_flow_event(flow_binding)
            .await?;

        // Backup current event ID for concurrency check
        let current_event_id = process_state.last_applied_event_id();

        // Apply updates in memory
        process_state
            .on_flow_outcome(event_id, self.time_source.now(), event_time, flow_outcome)
            .int_err()?;

        // Try saving back
        self.save_process_state_from_flow_event(&process_state, current_event_id)
            .await?;
        Ok(process_state)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
    async fn on_flow_scheduled(
        &self,
        event_id: EventID,
        flow_binding: &FlowBinding,
        planned_at: DateTime<Utc>,
    ) -> Result<FlowProcessState, FlowProcessFlowEventError> {
        // Acquire state
        let mut process_state = self
            .upsert_process_state_for_flow_event(flow_binding)
            .await?;

        // Backup current event ID for concurrency check
        let current_event_id = process_state.last_applied_event_id();

        // Apply flow scheduling
        process_state
            .on_scheduled(event_id, self.time_source.now(), planned_at)
            .int_err()?;

        // Try saving back
        self.save_process_state_from_flow_event(&process_state, current_event_id)
            .await?;
        Ok(process_state)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
    async fn on_flow_task_running(
        &self,
        event_id: EventID,
        flow_binding: &FlowBinding,
        started_at: DateTime<Utc>,
    ) -> Result<FlowProcessState, FlowProcessFlowEventError> {
        // Acquire state
        let mut process_state = self
            .upsert_process_state_for_flow_event(flow_binding)
            .await?;

        // Backup current event ID for concurrency check
        let current_event_id = process_state.last_applied_event_id();

        // Apply flow task running
        process_state
            .on_running(event_id, self.time_source.now(), started_at)
            .int_err()?;

        // Try saving back
        self.save_process_state_from_flow_event(&process_state, current_event_id)
            .await?;
        Ok(process_state)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?scope))]
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
