// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Background work that a caller not running the agents completes on demand,
/// such as the delivery of Outbox messages: the settle phase of
/// [`crate::BackgroundWorkFinalizer`] drains every registered one in turn
/// until none has work left
pub trait BackgroundWorkDrain: Send + Sync {
    fn new_drain_session(&self) -> Result<Box<dyn BackgroundWorkDrainSession + '_>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The drains of one finalization, which may keep state between them
#[async_trait::async_trait]
pub trait BackgroundWorkDrainSession: Send {
    /// Does the work pending now
    async fn drain(&mut self) -> Result<BackgroundWorkDrainOutcome, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackgroundWorkDrainOutcome {
    /// Nothing was done since the session's previous drain, by this drain or
    /// by a runner of the same work elsewhere, such as its agent
    Idle,
    /// Work was done, which may have made other work due
    Worked,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
