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
        _filter: &FlowProcessListFilter<'_>,
        _order: FlowProcessOrder,
        _limit: usize,
        _offset: usize,
    ) -> Result<Vec<FlowProcessState>, InternalError> {
        unimplemented!()
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
