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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MessageStoreWakeupDetector: Send + Sync {
    /// Block until there *might* be new message, or timeout elapses.
    async fn wait_wake(
        &self,
        timeout: Duration,
        min_debounce_interval: Duration,
    ) -> Result<MessageStoreWakeHint, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum MessageStoreWakeHint {
    /// Timeout elapsed without new messages
    Timeout,

    /// New messages detected
    NewMessages,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
