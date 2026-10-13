// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use async_utils::BackgroundAgent;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_SYSTEM_EVENT_AGENT_NAME: &str = "dev.kamu.domain.flow-system.FlowSystemEventAgent";

#[async_trait::async_trait]
pub trait FlowSystemEventAgent: BackgroundAgent {
    /// Handle any remaining events
    /// Only use this for tests!
    async fn catchup_remaining_events(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowSystemEventAgentConfig {
    /// How many events are applied to a projection per transaction
    pub batch_size: NonZeroUsize,
}

impl FlowSystemEventAgentConfig {
    // Same reasoning as for the outbox: each batch is one transaction, which on
    // Sqlite holds the pool's only connection, so keep it short
    pub fn local_default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(20).unwrap(),
        }
    }

    // Postgres pools connections, so larger batches mostly save round trips
    // when a projector catches up on a backlog
    pub fn production_default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(100).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
