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
use tokio::sync::broadcast;
use wakeup_listener::{WakeHint, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wakes up on explicit `signal()` calls, for in-memory storages.
pub struct InMemoryWakeupListener {
    tx: tokio::sync::broadcast::Sender<()>,
}

impl InMemoryWakeupListener {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(1024);

        Self { tx }
    }

    pub fn signal(&self) {
        // Wake up all listeners
        // We ignore errors here because if there are no listeners, that's fine
        let _ = self.tx.send(());
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
        // Subscribe to the signals broadcast channel
        let mut rx = self.tx.subscribe();

        // Wait until a signal arrives or timeout elapses
        // For testing purposes, we keep this simple without complex backoff strategies
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Ok(())) => {
                // Signal received
                Ok(WakeHint::Signaled)
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // Sender has been dropped, which should never happen in this case
                unreachable!("InMemoryWakeupListener: broadcast channel closed");
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                // We lagged behind, but that's fine, just indicate a signal was received
                Ok(WakeHint::Signaled)
            }
            Err(_elapsed) => {
                // Timeout elapsed
                Ok(WakeHint::Timeout)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
