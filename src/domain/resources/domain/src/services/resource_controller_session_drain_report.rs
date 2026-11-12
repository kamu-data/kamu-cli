// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What one drain of a session did
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResourceControllerSessionDrainReport {
    /// `process` calls the drain made
    pub processed: usize,
    /// `process` calls other drains of the controllers, such as the agent's,
    /// made since the session's previous drain or its start. Their messages
    /// may not be delivered yet.
    pub processed_by_other_drains: usize,
}

impl ResourceControllerSessionDrainReport {
    /// Whether no drain of the controllers did any work since the session's
    /// previous drain. Drains with nothing to do keep it idle, so that
    /// sessions running at once do not keep each other going.
    pub fn is_idle(&self) -> bool {
        self.processed == 0 && self.processed_by_other_drains == 0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
