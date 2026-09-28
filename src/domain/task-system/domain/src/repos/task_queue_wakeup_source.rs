// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use messaging_outbox::MessageStoreWakeupDetector;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Signals the task agent that new tasks might have been queued,
/// so that it does not have to poll the task queue continuously
pub trait TaskQueueWakeupSource: Send + Sync {
    /// Provides task queue wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
