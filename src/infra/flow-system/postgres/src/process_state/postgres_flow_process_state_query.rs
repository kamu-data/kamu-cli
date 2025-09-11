// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use kamu_flow_system::*;
use sqlx::Postgres;

use crate::PostgresFlowProcessStateRowModel;
use crate::helpers::{form_scope_query_condition_values, generate_scope_query_condition_clauses};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowProcessStateQuery {
    transaction: TransactionRefT<Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateQuery)]
impl PostgresFlowProcessStateQuery {
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
                format!("effective_state {direction}, {default_tiebreaker}",)
            }
            FlowProcessOrderField::FlowType => {
                format!("flow_type {direction}, last_attempt_at DESC NULLS LAST",)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateQuery for PostgresFlowProcessStateQuery {
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

        // Scope conditions
        let (scope_conditions, _next) =
            generate_scope_query_condition_clauses(&filter.scope, 8 /* 7 params + 1 */);
        let scope_values = form_scope_query_condition_values(filter.scope.clone());

        // Normalize optional array parameters to None if empty
        let maybe_flow_types = filter
            .for_flow_types
            .filter(|flow_types| !flow_types.is_empty());

        // Normalize optional array parameters to None if empty
        let maybe_effective_states = filter
            .effective_state_in
            .filter(|states| !states.is_empty());

        // Range as 2-element array
        let maybe_last_attempt_between =
            filter.last_attempt_between.map(|(start, end)| [start, end]);

        // First, get the total count (same filters, no ordering/pagination)
        let count_sql = format!(
            r#"
            SELECT COUNT(*)::bigint AS total_count
            FROM flow_process_states
                WHERE
                    ({scope_conditions}) AND
                    ($1::text[] IS NULL OR flow_type = ANY($1)) AND
                    ($2::flow_process_effective_state[] IS NULL OR effective_state = ANY($2)) AND
                    ($3::timestamptz[] IS NULL OR (last_attempt_at BETWEEN $3[1] AND $3[2])) AND
                    ($4 IS NULL OR last_failure_at >= $4) AND
                    ($5 IS NULL OR next_planned_at < $5) AND
                    ($6 IS NULL OR next_planned_at > $6) AND
                    ($7 IS NULL OR consecutive_failures >= $7)
            "#
        );

        let mut count_query = sqlx::query_scalar::<_, i64>(&count_sql)
            .bind(maybe_flow_types)
            .bind(maybe_effective_states)
            .bind(maybe_last_attempt_between)
            .bind(filter.last_failure_since)
            .bind(filter.next_planned_before)
            .bind(filter.next_planned_after)
            .bind(filter.min_consecutive_failures.map(i64::from));

        for arr in &scope_values {
            count_query = count_query.bind(arr);
        }

        let total_count = count_query
            .fetch_one(&mut *connection_mut)
            .await
            .int_err()?;

        // Then get the paginated results
        // For the list query, scope parameters start at index 11 (8 filter params +
        // limit + offset + 1)
        let (scope_conditions_list, _next) = generate_scope_query_condition_clauses(
            &filter.scope,
            10, /* 7 params + limit + offset + 1 */
        );

        // ORDER BY clauses
        let ordering_predicate = Self::generate_ordering_predicate(order);

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
                    ({scope_conditions_list}) AND
                    ($3::text[] IS NULL OR flow_type = ANY($3)) AND
                    ($4::flow_process_effective_state[] IS NULL OR effective_state = ANY($4)) AND
                    ($5::timestamptz[] IS NULL OR (last_attempt_at BETWEEN $5[1] AND $5[2])) AND
                    ($6 IS NULL OR last_failure_at >= $6) AND
                    ($7 IS NULL OR next_planned_at < $7) AND
                    ($8 IS NULL OR next_planned_at > $8) AND
                    ($9 IS NULL OR consecutive_failures >= $9)
                ORDER BY {ordering_predicate}
                LIMIT $1 OFFSET $2
            "#
        );

        let mut list_query = sqlx::query_as::<_, PostgresFlowProcessStateRowModel>(&list_sql)
            .bind(i64::try_from(limit).unwrap())
            .bind(i64::try_from(offset).unwrap())
            .bind(maybe_flow_types)
            .bind(maybe_effective_states)
            .bind(maybe_last_attempt_between)
            .bind(filter.last_failure_since)
            .bind(filter.next_planned_before)
            .bind(filter.next_planned_after)
            .bind(filter.min_consecutive_failures.map(i64::from));

        for arr in &scope_values {
            list_query = list_query.bind(arr);
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

        let (scope_conditions, _next) =
            generate_scope_query_condition_clauses(&flow_scope_query, 3);
        let scope_values = form_scope_query_condition_values(flow_scope_query);

        // Normalize optional array parameters to None if empty
        let maybe_flow_types = for_flow_types.filter(|flow_types| !flow_types.is_empty());

        // Normalize optional array parameters to None if empty
        let maybe_effective_states = effective_state_in.filter(|states| !states.is_empty());

        let sql = format!(
            r#"
            SELECT
                COUNT(*)::bigint                                                    AS total,
                COALESCE(SUM((effective_state = 'active')::int), 0)::bigint         AS active,
                COALESCE(SUM((effective_state = 'failing')::int), 0)::bigint        AS failing,
                COALESCE(SUM((effective_state = 'paused_manual')::int), 0)::bigint  AS paused,
                COALESCE(SUM((effective_state = 'stopped_auto')::int), 0)::bigint   AS stopped,
                COALESCE(MAX(consecutive_failures), 0)::bigint                      AS worst
            FROM flow_process_states
                WHERE
                    ({scope_conditions}) AND
                    ($1::text[] IS NULL OR flow_type = ANY($1)) AND
                    ($2::flow_process_effective_state[] IS NULL OR effective_state = ANY($2))
            "#
        );

        let mut query = sqlx::query_as::<_, FlowProcessGroupRollupRowModel>(&sql)
            .bind(maybe_flow_types as Option<&[&str]>)
            .bind(maybe_effective_states as Option<&[FlowProcessEffectiveState]>);

        for arr in &scope_values {
            query = query.bind(arr);
        }

        let row = query.fetch_one(connection_mut).await.int_err()?;
        Ok(row.try_into()?)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
