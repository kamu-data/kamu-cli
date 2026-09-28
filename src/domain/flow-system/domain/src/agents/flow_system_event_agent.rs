// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_utils::BackgroundAgent;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSystemEventAgent: BackgroundAgent {
    /// Handle any remaining events
    /// Only use this for tests!
    async fn catchup_remaining_events(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowSystemEventAgentConfig {
    pub batch_size: usize,
}

impl FlowSystemEventAgentConfig {
    pub fn local_default() -> Self {
        Self { batch_size: 20 }
    }

    pub fn production_default() -> Self {
        Self { batch_size: 100 }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
