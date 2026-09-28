// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use async_utils::BackgroundAgent;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskAgent: BackgroundAgent {
    /// Runs single task only, blocks until it is available (for tests only!)
    async fn run_single_task(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskAgentConfig {
    /// Minimal interval to collect further wakeup notifications after the
    /// first one arrives
    pub min_debounce_interval: Duration,

    /// Maximal time to wait for a wakeup notification before re-checking the
    /// task queue anyway
    pub max_listening_timeout: Duration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
