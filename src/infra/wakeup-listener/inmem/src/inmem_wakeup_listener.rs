// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use internal_error::InternalError;
use tokio::sync::Notify;
use wakeup_listener::{WakeHint, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wakes up on explicit `signal()` calls, for in-memory storages.
pub struct InMemoryWakeupListener {
    notify: Notify,
}

impl InMemoryWakeupListener {
    pub fn new() -> Self {
        Self {
            notify: Notify::new(),
        }
    }

    pub fn signal(&self) {
        self.notify.notify_waiters();
        // Also store a permit, so a signal raised while nobody waits is not lost
        self.notify.notify_one();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WakeupListener for InMemoryWakeupListener {
    async fn wait_wake(
        &self,
        timeout: Duration,
        _min_debounce_interval: Duration,
    ) -> Result<WakeHint, InternalError> {
        match tokio::time::timeout(timeout, self.notify.notified()).await {
            Ok(()) => Ok(WakeHint::Signaled),
            Err(_elapsed) => Ok(WakeHint::Timeout),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
