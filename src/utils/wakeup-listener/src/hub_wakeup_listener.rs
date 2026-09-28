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

use crate::{WakeHint, WakeupHub, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A lightweight handle listening to one channel of a shared [`WakeupHub`]
pub struct HubWakeupListener<H: WakeupHub> {
    hub: Arc<H>,
    channel: H::Channel,
    // Subscribed lazily, so that only components which actually wait occupy a slot
    slot: OnceLock<Arc<Notify>>,
}

impl<H: WakeupHub> HubWakeupListener<H> {
    pub fn new(hub: Arc<H>, channel: H::Channel) -> Self {
        Self {
            hub,
            channel,
            slot: OnceLock::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<H: WakeupHub> WakeupListener for HubWakeupListener<H> {
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<WakeHint, InternalError> {
        let deadline = tokio::time::Instant::now() + timeout;
        let slot = self.slot.get_or_init(|| self.hub.subscribe(self.channel));

        if tokio::time::timeout(timeout, slot.notified())
            .await
            .is_err()
        {
            return Ok(WakeHint::Timeout);
        }

        // Let a burst of signals coalesce into this wakeup
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
