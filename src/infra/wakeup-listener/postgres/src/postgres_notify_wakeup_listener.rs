// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::FutureExt as _;
use internal_error::InternalError;
use tokio::sync::Notify;
use wakeup_listener::{WakeHint, WakeupListener};

use crate::PostgresNotificationHub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Waits for Postgres `NOTIFY` signals on the given channel, via the shared
/// [`PostgresNotificationHub`]. The channel is expected to be notified by
/// triggers on the watched tables.
pub struct PostgresNotifyWakeupListener {
    hub: Arc<PostgresNotificationHub>,
    channel_name: &'static str,
    // Subscribed lazily, so that only components which actually wait occupy a slot
    slot: OnceLock<Arc<Notify>>,
}

impl PostgresNotifyWakeupListener {
    pub fn new(hub: Arc<PostgresNotificationHub>, channel_name: &'static str) -> Self {
        Self {
            hub,
            channel_name,
            slot: OnceLock::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WakeupListener for PostgresNotifyWakeupListener {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<WakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let slot = self
            .slot
            .get_or_init(|| self.hub.subscribe(self.channel_name));

        if tokio::time::timeout(timeout, slot.notified())
            .await
            .is_err()
        {
            return Ok(WakeHint::Timeout);
        }

        // Let a burst of notifications coalesce into this wakeup
        let remaining_after_debounce = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .saturating_sub(min_debounce_interval);
        if !min_debounce_interval.is_zero() && !remaining_after_debounce.is_zero() {
            tokio::time::sleep(min_debounce_interval).await;
            let _ = slot.notified().now_or_never();
        }

        Ok(WakeHint::Signaled)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
