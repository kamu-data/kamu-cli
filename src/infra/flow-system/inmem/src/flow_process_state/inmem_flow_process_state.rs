// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::*;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowProcessState {
    _state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn FlowProcessStateRepository)]
#[interface(dyn FlowProcessStateQuery)]
#[scope(Singleton)]
impl InMemoryFlowProcessState {
    pub fn new() -> Self {
        Self {
            _state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    // TODO
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateQuery for InMemoryFlowProcessState {
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
        _for_flow_types: Option<&[&str]>,
        _effective_state_in: Option<&[FlowProcessEffectiveState]>,
    ) -> Result<FlowProcessGroupRollup, InternalError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowProcessStateRepository for InMemoryFlowProcessState {
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
