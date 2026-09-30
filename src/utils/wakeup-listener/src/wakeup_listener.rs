// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Lets an agent sleep until its data *might* have changed, instead of polling.
/// Serves a single agent: changes since its previous call are never missed,
/// but wakeups may be spurious.
///
/// Also reports the liveness of the agent's loop: a heartbeat is recorded when
/// the listener is created and around every wait.
#[async_trait::async_trait]
pub trait WakeupListener: Send + Sync {
    /// Proves the agent's loop alive while it works through a backlog without
    /// waiting. Agents call it after every processed batch
    fn heartbeat(&self);

    /// Block until there *might* be new data, or timeout elapses.
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<WakeHint, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum WakeHint {
    /// Timeout elapsed without any signal
    Timeout,

    /// A signal was received: new data might be available
    Signaled,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
