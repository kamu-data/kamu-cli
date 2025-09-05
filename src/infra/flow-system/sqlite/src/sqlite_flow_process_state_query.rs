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
use sqlx::Sqlite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowProcessStateQuery {
    _transaction: TransactionRefT<Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateQuery)]
#[scope(Singleton)]
impl SqliteFlowProcessStateQuery {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            _transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateQuery for SqliteFlowProcessStateQuery {
    async fn try_get_process_state(
        &self,
        _flow_binding: &FlowBinding,
    ) -> Result<Option<FlowProcessState>, InternalError> {
        unimplemented!()
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
