// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_utils::BackgroundAgent;
use kamu_flow_system::FlowSystemEventAgent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn BackgroundAgent)]
#[dill::interface(dyn FlowSystemEventAgent)]
#[dill::scope(dill::Singleton)]
pub struct FlowSystemEventAgentImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl BackgroundAgent for FlowSystemEventAgentImpl {
    fn agent_name(&self) -> &'static str {
        "dev.kamu.domain.flow-system.FlowSystemEventAgent"
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn run(&self) -> Result<(), internal_error::InternalError> {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            tracing::debug!("FlowSystemEventAgentImpl::run() heartbeat");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowSystemEventAgent for FlowSystemEventAgentImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
