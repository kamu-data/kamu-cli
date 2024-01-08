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

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Flow {
    flow_state: fs::FlowState,
}

#[Object]
impl Flow {
    #[graphql(skip)]
    pub fn new(flow_state: fs::FlowState) -> Self {
        Self { flow_state }
    }

    /// Unique identifier of the flow
    async fn flow_id(&self) -> FlowID {
        self.flow_state.flow_id.into()
    }

    /// Key of the flow
    async fn flow_key(&self) -> FlowKey {
        self.flow_state.flow_key.clone().into()
    }

    /// Status of the flow
    async fn status(&self) -> FlowStatus {
        self.flow_state.status().into()
    }

    /// Outcome of the flow
    async fn outcome(&self) -> Option<FlowOutcome> {
        self.flow_state.outcome.map(|o| o.into())
    }
}

///////////////////////////////////////////////////////////////////////////////
