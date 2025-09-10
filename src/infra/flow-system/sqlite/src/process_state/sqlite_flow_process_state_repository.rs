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
use sqlx::Sqlite;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowProcessStateRepository {
    transaction: TransactionRefT<Sqlite>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateRepository)]
impl SqliteFlowProcessStateRepository {
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
        let scope_json = serde_json::to_value(&state.flow_binding().scope).int_err()?;
        let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

        let effective_state_str = state.effective_state().to_string();

        let paused_manual = i32::from(state.paused_manual());
        let stop_policy_kind = state.stop_policy().kind_to_string();
        let stop_policy_json = serde_json::to_value(state.stop_policy()).int_err()?;
        let stop_policy_data = canonical_json::to_string(&stop_policy_json).int_err()?;
        let consecutive_failures = i32::try_from(state.consecutive_failures()).unwrap();
        let last_success_at = state.last_success_at();
        let last_failure_at = state.last_failure_at();
        let last_attempt_at = state.last_attempt_at();
        let next_planned_at = state.next_planned_at();
        let sort_key = state.sort_key();
        let updated_at = state.updated_at();
        let last_applied_trigger_event_id = state.last_applied_trigger_event_id().into_inner();
        let last_applied_flow_event_id = state.last_applied_flow_event_id().into_inner();
        let flow_type = &state.flow_binding().flow_type;
        let expected_trigger_id = expected_last_trigger_event_id.into_inner();
        let expected_flow_id = expected_last_flow_event_id.into_inner();

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;
        let result = sqlx::query!(
            r#"
            UPDATE flow_process_states
                SET
                    paused_manual = $1,
                    stop_policy_kind = $2,
                    stop_policy_data = $3,
                    consecutive_failures = $4,
                    last_success_at = $5,
                    last_failure_at = $6,
                    last_attempt_at = $7,
                    next_planned_at = $8,
                    effective_state = $9,
                    sort_key = $10,
                    updated_at = $11,
                    last_applied_trigger_event_id = $12,
                    last_applied_flow_event_id = $13
                WHERE
                    flow_type = $14 AND scope_data = $15 AND
                    last_applied_trigger_event_id = $16 AND
                    last_applied_flow_event_id = $17
            "#,
            paused_manual,
            stop_policy_kind,
            stop_policy_data,
            consecutive_failures,
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
            effective_state_str,
            sort_key,
            updated_at,
            last_applied_trigger_event_id,
            last_applied_flow_event_id,
            flow_type,
            scope_data_json,
            expected_trigger_id,
            expected_flow_id,
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
impl FlowProcessStateRepository for SqliteFlowProcessStateRepository {
    async fn insert_process(
        &self,
        flow_binding: FlowBinding,
        sort_key: String,
        paused_manual: bool,
        stop_policy: FlowTriggerStopPolicy,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessInsertError> {
        let state = FlowProcessState::new(
            self.time_source.now(),
            flow_binding,
            sort_key,
            paused_manual,
            stop_policy,
            trigger_event_id,
        );

        let scope_json = serde_json::to_value(&state.flow_binding().scope).int_err()?;
        let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

        let stop_policy_kind = state.stop_policy().kind_to_string();
        let stop_policy_json = serde_json::to_value(state.stop_policy()).int_err()?;
        let stop_policy_data = canonical_json::to_string(&stop_policy_json).int_err()?;

        let effective_state_str = state.effective_state().to_string();

        let flow_type = &state.flow_binding().flow_type;
        let paused_manual_int = i32::from(state.paused_manual());
        let consecutive_failures = i32::try_from(state.consecutive_failures()).unwrap();
        let last_success_at = state.last_success_at();
        let last_failure_at = state.last_failure_at();
        let last_attempt_at = state.last_attempt_at();
        let next_planned_at = state.next_planned_at();
        let sort_key_ref = state.sort_key();
        let updated_at = state.updated_at();
        let last_applied_trigger_event_id = state.last_applied_trigger_event_id().into_inner();
        let last_applied_flow_event_id = state.last_applied_flow_event_id().into_inner();

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
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
                sort_key,
                updated_at,
                last_applied_trigger_event_id,
                last_applied_flow_event_id
            )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            ON CONFLICT (flow_type, scope_data) DO NOTHING
            "#,
            scope_data_json,
            flow_type,
            paused_manual_int,
            stop_policy_kind,
            stop_policy_data,
            consecutive_failures,
            last_success_at,
            last_failure_at,
            last_attempt_at,
            next_planned_at,
            effective_state_str,
            sort_key_ref,
            updated_at,
            last_applied_trigger_event_id,
            last_applied_flow_event_id,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_trigger_state(
        &self,
        flow_binding: FlowBinding,
        paused_manual: Option<bool>,
        stop_policy: Option<FlowTriggerStopPolicy>,
        trigger_event_id: EventID,
    ) -> Result<(), FlowProcessUpdateError> {
        // Load current state
        let mut process_state = match self.load_process_state(&flow_binding).await {
            Ok(state) => state,
            Err(FlowProcessLoadError::NotFound(_)) => {
                return Err(FlowProcessUpdateError::NotFound(FlowProcessNotFoundError {
                    flow_binding,
                }));
            }
            Err(FlowProcessLoadError::Internal(e)) => {
                return Err(FlowProcessUpdateError::Internal(e));
            }
        };

        // Backup current event IDs for concurrency check
        let current_trigger_event_id = process_state.last_applied_trigger_event_id();
        let current_flow_event_id = process_state.last_applied_flow_event_id();

        // Apply updates in memory
        process_state
            .update_trigger_state(
                self.time_source.now(),
                paused_manual,
                stop_policy,
                trigger_event_id,
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
                Err(FlowProcessUpdateError::ConcurrentModification(e))
            }
            Err(FlowProcessSaveError::Internal(e)) => Err(FlowProcessUpdateError::Internal(e)),
        }
    }

    async fn apply_flow_result(
        &self,
        flow_binding: FlowBinding,
        success: bool,
        event_time: DateTime<Utc>,
        next_planned_at: Option<DateTime<Utc>>,
        flow_event_id: EventID,
    ) -> Result<(), FlowProcessUpdateError> {
        // Load current state
        let mut process_state = match self.load_process_state(&flow_binding).await {
            Ok(state) => state,
            Err(FlowProcessLoadError::NotFound(_)) => {
                return Err(FlowProcessUpdateError::NotFound(FlowProcessNotFoundError {
                    flow_binding,
                }));
            }
            Err(FlowProcessLoadError::Internal(e)) => {
                return Err(FlowProcessUpdateError::Internal(e));
            }
        };

        // Backup current event IDs for concurrency check
        let current_trigger_event_id = process_state.last_applied_trigger_event_id();
        let current_flow_event_id = process_state.last_applied_flow_event_id();

        // Apply updates in memory
        if success {
            process_state
                .on_success(
                    self.time_source.now(),
                    event_time,
                    next_planned_at,
                    flow_event_id,
                )
                .int_err()?;
        } else {
            process_state
                .on_failure(
                    self.time_source.now(),
                    event_time,
                    next_planned_at,
                    flow_event_id,
                )
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
                Err(FlowProcessUpdateError::ConcurrentModification(e))
            }
            Err(FlowProcessSaveError::Internal(e)) => Err(FlowProcessUpdateError::Internal(e)),
        }
    }

    async fn delete_process(
        &self,
        flow_binding: FlowBinding,
    ) -> Result<(), FlowProcessDeleteError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
        let scope_data_json = canonical_json::to_string(&scope_json).int_err()?;

        let result = sqlx::query!(
            r#"
            DELETE FROM flow_process_states
                WHERE flow_type = $1 AND scope_data = $2
            "#,
            flow_binding.flow_type,
            scope_data_json,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        // Check if any rows were affected
        if result.rows_affected() == 0 {
            return Err(FlowProcessDeleteError::NotFound(FlowProcessNotFoundError {
                flow_binding,
            }));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
