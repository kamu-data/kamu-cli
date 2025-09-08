// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use database_common::{TransactionRef, TransactionRefT, sqlite_generate_placeholders_list};
use dill::{Singleton, component, interface, scope};
use kamu_flow_system::*;
use sqlx::Sqlite;

use crate::SqliteFlowProcessStateRowModel;
use crate::helpers::{form_scope_query_condition_values, generate_scope_query_condition_clauses};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowProcessStateQuery {
    transaction: TransactionRefT<Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateQuery)]
#[scope(Singleton)]
impl SqliteFlowProcessStateQuery {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateQuery for SqliteFlowProcessStateQuery {
    async fn try_get_process_state(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowProcessState>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        match crate::process_state::helpers::load_process_state(connection_mut, flow_binding).await
        {
            Ok(state) => Ok(Some(state)),
            Err(FlowProcessLoadError::NotFound(_)) => Ok(None),
            Err(FlowProcessLoadError::Internal(e)) => Err(e),
        }
    }

    async fn list_processes(
        &self,
        filter: FlowProcessListFilter<'_>,
        _order: FlowProcessOrder,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<FlowProcessState>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let (scope_conditions, flow_types_parameter_index) =
            generate_scope_query_condition_clauses(&filter.scope, 5 /* 4 params + 1 */);

        let scope_values = form_scope_query_condition_values(filter.scope);

        let effective_state_parameter_index =
            flow_types_parameter_index + filter.for_flow_types.map(<[&str]>::len).unwrap_or(0);

        let sql = format!(
            r#"
            SELECT
                flow_type,
                scope_data,
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
            FROM flow_process_status
                WHERE
                    ({scope_conditions})
                    ($3 = 0 OR $3 IN ({})) AND
                    ($4 = 0 OR $4 IN ({})) AND
                ORDER BY last_attempt_at DESC
                LIMIT $1 OFFSET $2
            "#,
            filter
                .for_flow_types
                .as_ref()
                .map(|flow_types| sqlite_generate_placeholders_list(
                    flow_types.len(),
                    NonZeroUsize::new(flow_types_parameter_index).unwrap()
                ))
                .unwrap_or_default(),
            filter
                .effective_state_in
                .as_ref()
                .map(|states| sqlite_generate_placeholders_list(
                    states.len(),
                    NonZeroUsize::new(effective_state_parameter_index).unwrap()
                ))
                .unwrap_or_default(),
        );

        let mut query = sqlx::query_as::<_, SqliteFlowProcessStateRowModel>(&sql)
            .bind(i64::try_from(limit).unwrap())
            .bind(i64::try_from(offset).unwrap())
            .bind(i32::from(filter.for_flow_types.is_some()))
            .bind(i32::from(filter.effective_state_in.is_some()));

        for scope_value in &scope_values {
            query = query.bind(scope_value);
        }

        if let Some(flow_types) = filter.for_flow_types {
            for flow_type in flow_types {
                query = query.bind(flow_type);
            }
        }

        if let Some(effective_states) = filter.effective_state_in {
            for state in effective_states {
                query = query.bind(state);
            }
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;
        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            let state = row.try_into()?;
            results.push(state);
        }
        Ok(results)
    }

    /// Compute rollup for matching rows.
    async fn rollup_by_scope(
        &self,
        flow_scope_query: FlowScopeQuery,
        for_flow_types: Option<&[&'static str]>,
        effective_state_in: Option<&[FlowProcessEffectiveState]>,
    ) -> Result<FlowProcessGroupRollup, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let (scope_conditions, flow_types_parameter_index) =
            generate_scope_query_condition_clauses(&flow_scope_query, 3 /* 2 params + 1 */);

        let scope_values = form_scope_query_condition_values(flow_scope_query);

        let effective_state_parameter_index =
            flow_types_parameter_index + for_flow_types.map(<[&str]>::len).unwrap_or(0);

        let sql = format!(
            r#"
            SELECT
                COUNT(*) AS total,
                IFNULL(SUM(CASE WHEN effective_state='active' THEN 1 ELSE 0 END),0) AS active,
                IFNULL(SUM(CASE WHEN effective_state='failing' THEN 1 ELSE 0 END),0) AS failing,
                IFNULL(SUM(CASE WHEN effective_state='paused_manual' THEN 1 ELSE 0 END),0) AS paused,
                IFNULL(SUM(CASE WHEN effective_state='stopped_auto' THEN 1 ELSE 0 END),0) AS stopped,
                IFNULL(MAX(consecutive_failures),0) AS worst
            FROM flow_process_states
                WHERE
                    ({scope_conditions}) AND
                    ($1 = 0 OR $1 IN ({})) AND
                    ($2 = 0 OR $2 IN ({})) AND
            "#,
            for_flow_types
                .as_ref()
                .map(|flow_types| sqlite_generate_placeholders_list(
                    flow_types.len(),
                    NonZeroUsize::new(flow_types_parameter_index).unwrap()
                ))
                .unwrap_or_default(),
            effective_state_in
                .as_ref()
                .map(|states| sqlite_generate_placeholders_list(
                    states.len(),
                    NonZeroUsize::new(effective_state_parameter_index).unwrap()
                ))
                .unwrap_or_default(),
        );

        let mut query = sqlx::query_as::<_, FlowProcessGroupRollupRowModel>(&sql)
            .bind(i32::from(for_flow_types.is_some()))
            .bind(i32::from(effective_state_in.is_some()));

        for scope_value in &scope_values {
            query = query.bind(scope_value);
        }

        if let Some(flow_types) = for_flow_types {
            for flow_type in flow_types {
                query = query.bind(flow_type);
            }
        }

        if let Some(effective_states) = effective_state_in {
            for state in effective_states {
                query = query.bind(state);
            }
        }

        let row = query.fetch_one(connection_mut).await.int_err()?;
        Ok(row.try_into()?)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
