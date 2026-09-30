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
    /// The agent's own listener handle, kept for its lifetime: a shared one
    /// would lose wakeups. Its heartbeat is labelled with the agent's name
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
