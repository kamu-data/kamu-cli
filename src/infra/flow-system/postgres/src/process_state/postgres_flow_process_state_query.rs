// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{Singleton, component, interface, scope};
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
#[scope(Singleton)]
impl PostgresFlowProcessStateQuery {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
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
        _order: FlowProcessOrder,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<FlowProcessState>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let (scope_conditions, _next) =
            generate_scope_query_condition_clauses(&filter.scope, 5 /* 4 params + 1 */);
        let scope_values = form_scope_query_condition_values(filter.scope);

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
                    AND ($3::text[] IS NULL OR flow_type = ANY($3))
                    AND ($4::flow_process_effective_state[] IS NULL OR effective_state = ANY($4))
                ORDER BY last_attempt_at DESC
                LIMIT $1 OFFSET $2
            "#
        );

        let mut query = sqlx::query_as::<_, PostgresFlowProcessStateRowModel>(&sql)
            .bind(i64::try_from(limit).unwrap())
            .bind(i64::try_from(offset).unwrap())
            .bind(filter.for_flow_types as Option<&[&str]>)
            .bind(filter.effective_state_in as Option<&[FlowProcessEffectiveState]>);

        for arr in &scope_values {
            query = query.bind(arr);
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

        let (scope_conditions, _next) =
            generate_scope_query_condition_clauses(&flow_scope_query, 3);
        let scope_values = form_scope_query_condition_values(flow_scope_query);

        let sql = format!(
            r#"
            SELECT
                COUNT(*)::bigint                                                    AS total,
                COALESCE(SUM((effective_state = 'active')::int), 0)::bigint         AS active,
                COALESCE(SUM((effective_state = 'failing')::int), 0)::bigint        AS failing,
                COALESCE(SUM((effective_state = 'paused_manual')::int), 0)::bigint  AS paused,
                COALESCE(SUM((effective_state = 'stopped_auto')::int), 0)::bigint   AS stopped,
                COALESCE(MAX(consecutive_failures), 0)::bigint                      AS worst
            FROM flow_process_status
                WHERE
                    ({scope_conditions})
                    AND ($1::text[] IS NULL OR flow_type = ANY($1))
                    AND ($2::flow_process_effective_state[] IS NULL OR effective_state = ANY($2))
            "#
        );

        let mut query = sqlx::query_as::<_, FlowProcessGroupRollupRowModel>(&sql)
            .bind(for_flow_types as Option<&[&str]>)
            .bind(effective_state_in as Option<&[FlowProcessEffectiveState]>);

        for arr in &scope_values {
            query = query.bind(arr);
        }

        let row = query.fetch_one(connection_mut).await.int_err()?;
        Ok(row.try_into()?)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
