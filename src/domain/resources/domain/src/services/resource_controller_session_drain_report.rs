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
    /// Whether the drain waited for another drain of a controller, such as
    /// the agent's, whose work may have posted messages not delivered yet
    pub waited_for_another_drain: bool,
}

impl ResourceControllerSessionDrainReport {
    /// Whether neither this drain nor one it waited for did any work
    pub fn is_idle(&self) -> bool {
        self.processed == 0 && !self.waited_for_another_drain
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
