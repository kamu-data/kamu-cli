// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu_task_system::TaskQueueWakeupSource;
use kamu_wakeup_listener_inmem::InMemoryWakeupListener;
use wakeup_listener::WakeupListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryTaskQueueWakeupSource {
    wakeup_listener: InMemoryWakeupListener,
}

#[component(pub)]
#[interface(dyn TaskQueueWakeupSource)]
#[scope(Singleton)]
impl InMemoryTaskQueueWakeupSource {
    pub fn new() -> Self {
        Self {
            wakeup_listener: InMemoryWakeupListener::new(),
        }
    }

    pub fn notify_task_queued(&self) {
        self.wakeup_listener.signal();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskQueueWakeupSource for InMemoryTaskQueueWakeupSource {
    fn wakeup_listener(&self) -> &dyn WakeupListener {
        &self.wakeup_listener
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
