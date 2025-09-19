// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSystemEventStore: Send + Sync {
    /// Block until there *might* be new work, or timeout.
    async fn wait_wake(&self, timeout: Duration) -> FlowSystemEventStoreWakeReason;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FlowSystemEventStoreWakeReason {
    NewWork,
    Timeout,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
