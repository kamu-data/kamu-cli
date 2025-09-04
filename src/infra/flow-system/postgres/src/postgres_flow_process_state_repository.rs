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

pub struct PostgresFlowProcessStateRepository {
    _transaction: TransactionRefT<Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowProcessStateRepository)]
#[scope(Singleton)]
impl PostgresFlowProcessStateRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            _transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateRepository for PostgresFlowProcessStateRepository {
    async fn insert_process(
        &self,
        _flow_binding: FlowBinding,
        _paused_manual: bool,
        _stop_policy: FlowTriggerStopPolicy,
        _trigger_event_id: i64,
    ) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn update_trigger_state(
        &self,
        _flow_binding: FlowBinding,
        _paused_manual: Option<bool>,
        _stop_policy: Option<FlowTriggerStopPolicy>,
        _trigger_event_id: i64,
    ) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn apply_flow_result(
        &self,
        _flow_binding: FlowBinding,
        _success: bool,
        _event_time: chrono::DateTime<chrono::Utc>,
        _flow_event_id: i64,
    ) -> Result<(), InternalError> {
        unimplemented!()
    }

    async fn delete_process(&self, _flow_binding: FlowBinding) -> Result<(), InternalError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
