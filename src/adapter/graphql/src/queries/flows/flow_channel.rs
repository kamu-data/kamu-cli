// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowChannelGroup {
    _flow_scope_query: fs::FlowScopeQuery,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl FlowChannelGroup {
    #[graphql(skip)]
    pub fn new(flow_scope_query: fs::FlowScopeQuery) -> Self {
        Self {
            _flow_scope_query: flow_scope_query,
        }
    }

    #[allow(clippy::unused_async)]
    async fn overall_status(&self) -> FlowPeriodicProcessStatus {
        // TODO
        FlowPeriodicProcessStatus::Active
    }

    #[allow(clippy::unused_async)]
    async fn rollup(&self) -> FlowChannelGroupRollup {
        // TODO
        FlowChannelGroupRollup {
            active: 0,
            failing: 0,
            paused: 0,
            stopped: 0,
        }
    }

    #[allow(clippy::unused_async)]
    async fn channels(&self) -> Result<Vec<FlowChannel>> {
        // TODO
        Ok(vec![])
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowChannelGroupRollup {
    active: usize,
    failing: usize,
    paused: usize,
    stopped: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowChannel {
    name: String,
    trigger: fs::FlowTriggerState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl FlowChannel {
    #[graphql(skip)]
    #[allow(dead_code)]
    pub fn new(name: String, trigger: fs::FlowTriggerState) -> Self {
        Self { name, trigger }
    }

    #[allow(clippy::unused_async)]
    pub async fn name(&self) -> String {
        self.name.clone()
    }

    #[allow(clippy::unused_async)]
    async fn flow_trigger(&self) -> Option<FlowTrigger> {
        Some(self.trigger.clone().into())
    }

    #[allow(clippy::unused_async)]
    async fn runtime_state(&self) -> FlowPeriodicProcessState {
        // TODO: reuse flow-binding state
        FlowPeriodicProcessState {
            effective_status: FlowPeriodicProcessStatus::Active,
            consecutive_failures: 0,
            last_success_at: None,
            last_attempt_at: None,
            last_failure_at: None,
            next_planned_at: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
