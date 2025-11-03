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
use sqlx::Sqlite;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateRepository)]
pub struct SqliteFlowProcessStateRepository {
    transaction: TransactionRefT<Sqlite>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SqliteFlowProcessStateRepository {
    async fn create_process_state(
        &self,
        process_state: &FlowProcessState,
    ) -> Result<(), FlowProcessSaveError> {
        let scope_json = serde_json::to_value(&process_state.flow_binding().scope).int_err()?;
        let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

        let stop_policy_kind = process_state.stop_policy().kind_to_string();
        let stop_policy_json = serde_json::to_value(process_state.stop_policy()).int_err()?;
        let stop_policy_data = canonical_json::to_string(&stop_policy_json).int_err()?;

        let effective_state_str = process_state.effective_state().to_string();

        let flow_type = &process_state.flow_binding().flow_type;
        let user_intent_str = process_state.user_intent().to_string();
        let consecutive_failures = i32::try_from(process_state.consecutive_failures()).unwrap();
        let last_success_at = process_state.last_success_at();
        let last_failure_at = process_state.last_failure_at();
        let last_attempt_at = process_state.last_attempt_at();
        let next_planned_at = process_state.next_planned_at();
        let auto_stopped_at = process_state.auto_stopped_at();
        let auto_stopped_reason = process_state.auto_stopped_reason().map(|r| r.to_string());
        let updated_at = process_state.updated_at();
        let last_applied_flow_system_event_id = process_state.last_applied_event_id().into_inner();

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
                auto_stopped_at,
                effective_state,
                auto_stopped_reason,
                updated_at,
                last_applied_flow_system_event_id
            )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (flow_type, scope_data) DO NOTHING
            "#,
            scope_data_json,
            flow_type,
            user_intent_str,
            stop_policy_kind,
            stop_policy_data,
            consecutive_failures,
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
            auto_stopped_at,
            effective_state_str,
            auto_stopped_reason,
            updated_at,
            last_applied_flow_system_event_id,
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
        let scope_json = serde_json::to_value(&state.flow_binding().scope).int_err()?;
        let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

        let effective_state_str = state.effective_state().to_string();

        let user_intent_str = state.user_intent().to_string();
        let stop_policy_kind = state.stop_policy().kind_to_string();
        let stop_policy_json = serde_json::to_value(state.stop_policy()).int_err()?;
        let stop_policy_data = canonical_json::to_string(&stop_policy_json).int_err()?;
        let consecutive_failures = i32::try_from(state.consecutive_failures()).unwrap();
        let last_success_at = state.last_success_at();
        let last_failure_at = state.last_failure_at();
        let last_attempt_at = state.last_attempt_at();
        let next_planned_at = state.next_planned_at();
        let auto_stopped_at = state.auto_stopped_at();
        let auto_stopped_reason = state.auto_stopped_reason().map(|r| r.to_string());
        let updated_at = state.updated_at();
        let last_applied_flow_system_event_id = state.last_applied_event_id().into_inner();
        let flow_type = &state.flow_binding().flow_type;
        let expected_event_id = expected_last_event_id.into_inner();

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let result = sqlx::query!(
            r#"
            UPDATE flow_process_states
                SET
                    user_intent = $1,
                    stop_policy_kind = $2,
                    stop_policy_data = $3,
                    consecutive_failures = $4,
                    last_success_at = $5,
                    last_failure_at = $6,
                    last_attempt_at = $7,
                    next_planned_at = $8,
                    auto_stopped_at = $9,
                    effective_state = $10,
                    auto_stopped_reason = $11,
                    updated_at = $12,
                    last_applied_flow_system_event_id = $13
                WHERE
                    flow_type = $14 AND scope_data = $15 AND
                    last_applied_flow_system_event_id = $16
            "#,
            user_intent_str,
            stop_policy_kind,
            stop_policy_data,
            consecutive_failures,
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
            auto_stopped_at,
            effective_state_str,
            auto_stopped_reason,
            updated_at,
            last_applied_flow_system_event_id,
            flow_type,
            scope_data_json,
            expected_event_id,
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
impl FlowProcessStateRepository for SqliteFlowProcessStateRepository {
    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
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

        let scope_json = serde_json::to_value(scope).int_err()?;
        let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

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
