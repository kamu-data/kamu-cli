// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventID;
use internal_error::InternalError;
use wakeup_listener::WakeupListener;

use crate::FlowSystemEvent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSystemEventBridge: Send + Sync {
    /// Creates a listener handle for the flow system event agent, which keeps
    /// it for its lifetime: handles are cheap, but a shared one would lose
    /// wakeups. Its heartbeat is labelled with the flow system event
    /// agent's name
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener>;

    /// Fetch next batch for the given projector; order by global id.
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<FlowSystemEvent>, InternalError>;

    /// Mark these events as applied for this projector (should be idempotent!).
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        event_ids_with_tx_ids: &[(EventID, i64)],
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
