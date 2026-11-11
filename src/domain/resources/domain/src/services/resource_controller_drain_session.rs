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

/// Drains of every registered controller, made on demand. A caller alternates
/// them with the work that makes resources actionable, such as delivering
/// outbox messages, until a drain finds nothing to do.
#[async_trait::async_trait]
pub trait ResourceControllerDrainSession: Send {
    /// Processes every resource that needs attention now, controller by
    /// controller, and returns how many `process` calls it made. Does not wait
    /// for deadlines: a resource waiting for time stays for a later drain.
    ///
    /// A resource the controller never settles is held back for the rest of
    /// the session, so the drains of one session end up processing nothing.
    async fn drain_due_resources(&mut self) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
