// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use tokio::sync::Notify;
use wakeup_listener::{WakeupHub, WakeupListenerMetrics, WakeupSubscribers};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Routes explicit signals from in-memory stores to listener handles, by
/// channel name. See `docs/internal/wakeup-listeners.md`.
pub struct InMemoryWakeupHub {
    subscribers: WakeupSubscribers<&'static str>,
    metrics: Arc<WakeupListenerMetrics>,
}

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
impl InMemoryWakeupHub {
    pub fn new(metrics: Arc<WakeupListenerMetrics>) -> Self {
        Self {
            subscribers: WakeupSubscribers::new(),
            metrics,
        }
    }

    /// Called by stores after a successful write
    pub fn signal(&self, channel: &'static str) {
        self.subscribers.signal(channel);
    }
}

impl WakeupHub for InMemoryWakeupHub {
    type Channel = &'static str;

    fn subscribe(&self, channel: &'static str) -> Arc<Notify> {
        let slot = self.subscribers.add(channel);

        // Subscription is lazy, and a signal to a channel without subscribers is
        // not kept, so a write made before this call would otherwise be missed
        slot.notify_one();

        slot
    }

    fn metrics(&self) -> &WakeupListenerMetrics {
        &self.metrics
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
