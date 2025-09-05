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
