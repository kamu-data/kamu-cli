// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
pub use tokio_util::sync::CancellationToken;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A long-running loop hosted by the server for the whole process lifetime.
///
/// The host restarts an agent whose `run` fails, panics or returns before a
/// shutdown, unless its failure is fatal.
#[async_trait::async_trait]
pub trait BackgroundAgent: Sync + Send {
    /// Returns the name of the agent for debugging purposes
    fn agent_name(&self) -> &'static str;

    /// Runs the main loop until `shutdown` is cancelled.
    ///
    /// The agent checks the token between steps and stops waiting for work
    /// when it is cancelled, then returns `Ok`. A step in progress is not
    /// interrupted by the agent; the host gives it a bounded time.
    async fn run(&self, shutdown: CancellationToken) -> Result<(), InternalError>;

    /// Whether a failure of this agent must stop the host instead of being
    /// retried
    fn is_failure_fatal(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
