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
use dill::{component, interface};
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
impl SqliteFlowProcessStateQuery {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    fn generate_ordering_predicate(order: FlowProcessOrder) -> String {
        let direction = if order.desc { "DESC" } else { "ASC" };
        let default_tiebreaker = "last_attempt_at DESC NULLS LAST, flow_type ASC";

        match order.field {
            FlowProcessOrderField::LastAttemptAt => {
                format!("last_attempt_at {direction} NULLS LAST, flow_type ASC",)
            }
            FlowProcessOrderField::NextPlannedAt => {
                format!("next_planned_at {direction} NULLS LAST, {default_tiebreaker}",)
            }
            FlowProcessOrderField::LastFailureAt => {
                format!("last_failure_at {direction} NULLS LAST, {default_tiebreaker}",)
            }
            FlowProcessOrderField::ConsecutiveFailures => {
                format!("consecutive_failures {direction}, {default_tiebreaker}",)
            }
            FlowProcessOrderField::EffectiveState => {
                let main_ordering = r#"
                        CASE effective_state
                            WHEN 'stopped_auto'  THEN 0
                            WHEN 'failing'       THEN 1
                            WHEN 'paused_manual' THEN 2
                            WHEN 'active'        THEN 3
                            ELSE 3
                        END
                    "#
                .to_string();
                format!("{main_ordering} {direction}, {default_tiebreaker}",)
            }
            FlowProcessOrderField::FlowType => {
                format!("flow_type {direction}, last_attempt_at DESC NULLS LAST",)
            }
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
        order: FlowProcessOrder,
        limit: usize,
        offset: usize,
    ) -> Result<FlowProcessStateListing, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let (scope_conditions, flow_types_parameter_index) =
            generate_scope_query_condition_clauses(&filter.scope, 9 /* 8 params + 1 */);

        let scope_values = form_scope_query_condition_values(filter.scope.clone());

        let effective_state_parameter_index =
            flow_types_parameter_index + filter.for_flow_types.map(<[&str]>::len).unwrap_or(0);

        // First, get the total count (same filters, no ordering/pagination)
        let count_sql = format!(
            r#"
            SELECT COUNT(*) AS total_count
            FROM flow_process_states
                WHERE
                    ({scope_conditions}) AND
                    ($1 = 0 OR flow_type IN ({})) AND
                    ($2 = 0 OR effective_state IN ({})) AND
                    ($3 IS NULL OR $4 IS NULL OR (last_attempt_at BETWEEN $3 AND $4)) AND
                    ($5 IS NULL OR last_failure_at >= $5) AND
                    ($6 IS NULL OR next_planned_at < $6) AND
                    ($7 IS NULL OR next_planned_at > $7) AND
                    ($8 IS NULL OR consecutive_failures >= $8)
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

        let mut count_query = sqlx::query_scalar::<sqlx::Sqlite, i64>(&count_sql)
            .bind(i32::from(
                filter.for_flow_types.is_some_and(|fts| !fts.is_empty()),
            ))
            .bind(i32::from(
                filter.effective_state_in.is_some_and(|es| !es.is_empty()),
            ))
            .bind(filter.last_attempt_between.map(|r| r.0))
            .bind(filter.last_attempt_between.map(|r| r.1))
            .bind(filter.last_failure_since)
            .bind(filter.next_planned_before)
            .bind(filter.next_planned_after)
            .bind(filter.min_consecutive_failures.map(i64::from));

        for scope_value in &scope_values {
            count_query = count_query.bind(scope_value);
        }

        if let Some(flow_types) = filter.for_flow_types {
            for flow_type in flow_types {
                count_query = count_query.bind(flow_type);
            }
        }

        if let Some(effective_states) = filter.effective_state_in {
            for state in effective_states {
                count_query = count_query.bind(state);
            }
        }

        let total_count = count_query
            .fetch_one(&mut *connection_mut)
            .await
            .int_err()?;

        // Then get the paginated results
        let ordering_predicate = Self::generate_ordering_predicate(order);

        // For the list query, adjust parameter indices for limit/offset
        let (list_scope_conditions, list_flow_types_parameter_index) =
            generate_scope_query_condition_clauses(&filter.scope, 11 /* 10 params + 1 */);

        let list_effective_state_parameter_index =
            list_flow_types_parameter_index + filter.for_flow_types.map(<[&str]>::len).unwrap_or(0);

        let list_sql = format!(
            r#"
            SELECT
                flow_type,
                scope_data,
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
            FROM flow_process_states
                WHERE
                    ({list_scope_conditions}) AND
                    ($3 = 0 OR flow_type IN ({})) AND
                    ($4 = 0 OR effective_state IN ({})) AND
                    ($5 IS NULL OR $6 IS NULL OR (last_attempt_at BETWEEN $5 AND $6)) AND
                    ($7 IS NULL OR last_failure_at >= $7) AND
                    ($8 IS NULL OR next_planned_at < $8) AND
                    ($9 IS NULL OR next_planned_at > $9) AND
                    ($10 IS NULL OR consecutive_failures >= $10)
                ORDER BY {ordering_predicate}
                LIMIT $1 OFFSET $2
            "#,
            filter
                .for_flow_types
                .as_ref()
                .map(|flow_types| sqlite_generate_placeholders_list(
                    flow_types.len(),
                    NonZeroUsize::new(list_flow_types_parameter_index).unwrap()
                ))
                .unwrap_or_default(),
            filter
                .effective_state_in
                .as_ref()
                .map(|states| sqlite_generate_placeholders_list(
                    states.len(),
                    NonZeroUsize::new(list_effective_state_parameter_index).unwrap()
                ))
                .unwrap_or_default(),
        );

        let mut list_query =
            sqlx::query_as::<sqlx::Sqlite, SqliteFlowProcessStateRowModel>(&list_sql)
                .bind(i64::try_from(limit).unwrap())
                .bind(i64::try_from(offset).unwrap())
                .bind(i32::from(
                    filter.for_flow_types.is_some_and(|fts| !fts.is_empty()),
                ))
                .bind(i32::from(
                    filter.effective_state_in.is_some_and(|es| !es.is_empty()),
                ))
                .bind(filter.last_attempt_between.map(|r| r.0))
                .bind(filter.last_attempt_between.map(|r| r.1))
                .bind(filter.last_failure_since)
                .bind(filter.next_planned_before)
                .bind(filter.next_planned_after)
                .bind(filter.min_consecutive_failures.map(i64::from));

        for scope_value in &scope_values {
            list_query = list_query.bind(scope_value);
        }

        if let Some(flow_types) = filter.for_flow_types {
            for flow_type in flow_types {
                list_query = list_query.bind(flow_type);
            }
        }

        if let Some(effective_states) = filter.effective_state_in {
            for state in effective_states {
                list_query = list_query.bind(state);
            }
        }

        let rows = list_query.fetch_all(connection_mut).await.int_err()?;
        let mut processes = Vec::with_capacity(rows.len());
        for row in rows {
            let state = row.try_into()?;
            processes.push(state);
        }

        Ok(FlowProcessStateListing {
            processes,
            total_count: usize::try_from(total_count).unwrap(),
        })
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
                    ($1 = 0 OR flow_type IN ({})) AND
                    ($2 = 0 OR effective_state IN ({}))
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
