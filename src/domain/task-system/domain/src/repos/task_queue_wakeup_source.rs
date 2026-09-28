// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use wakeup_listener::WakeupListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Signals the task agent that new tasks might have been queued
pub trait TaskQueueWakeupSource: Send + Sync {
    fn wakeup_listener(&self) -> &dyn WakeupListener;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
