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
use messaging_outbox::{MessageStoreWakeHint, MessageStoreWakeupDetector};
use tokio::sync::broadcast;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryMessageStoreWakeupDetector {
    tx: tokio::sync::broadcast::Sender<()>,
}

impl InMemoryMessageStoreWakeupDetector {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(1024);

        Self { tx }
    }

    pub fn notify_new_message_arrived(&self) {
        // Notify all listeners that a new message has arrived
        // We ignore errors here because if there are no listeners, that's fine
        let _ = self.tx.send(());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageStoreWakeupDetector for InMemoryMessageStoreWakeupDetector {
    async fn wait_wake(
        &self,
        timeout: Duration,
        _min_debounce_interval: Duration,
    ) -> Result<MessageStoreWakeHint, InternalError> {
        // Subscribe to messages broadcast channel
        let mut rx = self.tx.subscribe();

        // Wait until a new message arrives or timeout elapses
        // For testing purposes, we keep this simple without complex backoff strategies
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Ok(())) => {
                // New message arrived
                Ok(MessageStoreWakeHint::NewMessages)
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // Sender has been dropped, which should never happen in this case
                unreachable!("InMemoryMessageStoreWakeupDetector: broadcast channel closed");
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                // We lagged behind, but that's fine, just indicate new messages are available
                Ok(MessageStoreWakeHint::NewMessages)
            }
            Err(_elapsed) => {
                // Timeout elapsed
                Ok(MessageStoreWakeHint::Timeout)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
