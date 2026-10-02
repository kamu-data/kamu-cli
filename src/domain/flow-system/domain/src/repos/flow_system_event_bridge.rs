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
    /// The agent's own listener handle, kept for its lifetime: a shared one
    /// would lose wakeups. Its heartbeat is labelled with the agent's name
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener>;

    /// Fetch next batch for the given projector; order by global id.
    async fn fetch_next_batch(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<FlowSystemEvent>, InternalError>;

    /// Whether committed events exist that fetches do not return yet, because a
    /// transaction that started earlier is still running
    async fn has_held_back_events(
        &self,
        transaction_catalog: &dill::Catalog,
    ) -> Result<bool, InternalError>;

    /// Mark these events as applied for this projector (should be idempotent!).
    async fn mark_applied(
        &self,
        transaction_catalog: &dill::Catalog,
        projector_name: &'static str,
        event_ids_with_tx_ids: &[(EventID, i64)],
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
