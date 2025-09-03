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
use crate::queries::periodic_process_state;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Copy, Clone, PartialEq, Eq)]
pub struct FlowChannelGroupRollup {
    pub active: u32,
    pub failing: u32,
    pub paused: u32,
    pub stopped: u32,
    pub worst_consecutive_failures: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowChannel {
    id: String,
    name: String,
    channel_type: FlowChannelType,
    flow_trigger: fs::FlowTriggerState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl FlowChannel {
    #[graphql(skip)]
    #[allow(dead_code)]
    pub fn new(
        id: String,
        name: String,
        channel_type: FlowChannelType,
        flow_trigger: fs::FlowTriggerState,
    ) -> Self {
        Self {
            id,
            name,
            channel_type,
            flow_trigger,
        }
    }

    #[allow(clippy::unused_async)]
    pub async fn id(&self) -> String {
        self.id.clone()
    }

    #[allow(clippy::unused_async)]
    pub async fn name(&self) -> String {
        self.name.clone()
    }

    #[allow(clippy::unused_async)]
    pub async fn channel_type(&self) -> FlowChannelType {
        self.channel_type
    }

    #[allow(clippy::unused_async)]
    async fn flow_trigger(&self) -> FlowTrigger {
        self.flow_trigger.clone().into()
    }

    async fn runtime_state(&self, ctx: &Context<'_>) -> Result<FlowPeriodicProcessState> {
        periodic_process_state(ctx, &self.flow_trigger).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Enum, Copy, Clone, PartialEq, Eq)]
pub(crate) enum FlowChannelType {
    Webhook,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
