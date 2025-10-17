// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_flow_system::{FlowAgentLoopSynchronizer, FlowSystemEventAgent};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub(crate) struct FlowAgentTestLoopSynchronizer {
    flow_system_event_agent: Arc<dyn FlowSystemEventAgent>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowAgentLoopSynchronizer for FlowAgentTestLoopSynchronizer {
    async fn synchronize_execution_loop(&self) -> Result<(), internal_error::InternalError> {
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
