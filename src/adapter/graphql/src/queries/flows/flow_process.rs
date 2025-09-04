// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{self as fs};

use crate::prelude::*;
use crate::queries::flow_process_runtime_state;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowProcess {
    flow_trigger: fs::FlowTriggerState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl FlowProcess {
    #[graphql(skip)]
    pub fn new(flow_trigger: fs::FlowTriggerState) -> Self {
        Self { flow_trigger }
    }

    async fn flow_type(&self) -> DatasetFlowType {
        decode_dataset_flow_type(&self.flow_trigger.flow_binding.flow_type)
    }

    async fn flow_trigger(&self) -> FlowTrigger {
        self.flow_trigger.clone().into()
    }

    #[tracing::instrument(level = "debug", name = "Gql::FlowProcess::runtime_state", skip_all)]
    async fn runtime_state(&self, ctx: &Context<'_>) -> Result<FlowProcessRuntimeState> {
        flow_process_runtime_state(ctx, &self.flow_trigger).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
