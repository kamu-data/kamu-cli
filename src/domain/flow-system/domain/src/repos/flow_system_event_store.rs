// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use event_sourcing::EventID;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSystemEventStore: Send + Sync {
    /// Block until there *might* be new work, or timeout.
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<FlowSystemEventStoreWakeHint, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FlowSystemEventStoreWakeHint {
    /// Timeout elapsed without new events
    Timeout,

    /// New events detected with upper bound
    NewEvents { upper_event_id_bound: EventID },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
