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

use crate::FlowSystemEvent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSystemEventBridge: Send + Sync {
    /// Block until there *might* be new events, or timeout elapses.
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<FlowSystemEventStoreWakeHint, InternalError>;

    /// Fetch next batch for the given projector; order by global id.
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
        loopback_offset: usize,
        maybe_event_id_bounds_hint: Option<(EventID, EventID)>,
    ) -> Result<Vec<FlowSystemEvent>, InternalError>;

    /// Mark these events as applied for this projector (should be idempotent!).
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        event_ids: &[EventID],
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FlowSystemEventStoreWakeHint {
    /// Timeout elapsed without new events
    Timeout,

    /// New events detected with lower/upper bounds
    NewEvents {
        lower_event_id_bound: EventID,
        upper_event_id_bound: EventID,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
