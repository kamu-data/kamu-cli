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
        _flow_scope_query: &FlowScopeQuery,
        _for_flow_types: Option<&[&'static str]>,
        _effective_state_in: Option<&[FlowProcessEffectiveState]>,
    ) -> Result<FlowProcessGroupRollup, InternalError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
