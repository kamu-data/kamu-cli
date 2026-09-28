// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_task_system::TaskQueueWakeupSource;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use wakeup_listener::{HubWakeupListener, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Signaled by `InMemoryTaskEventStore` when a task becomes queued
pub(crate) const TASKS_QUEUED_CHANNEL: &str = "tasks_queued";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryTaskQueueWakeupSource {
    hub: Arc<InMemoryWakeupHub>,
}

#[component(pub)]
#[interface(dyn TaskQueueWakeupSource)]
#[scope(Agnostic)]
impl InMemoryTaskQueueWakeupSource {
    pub fn new(hub: Arc<InMemoryWakeupHub>) -> Self {
        Self { hub }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskQueueWakeupSource for InMemoryTaskQueueWakeupSource {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.hub.clone(),
            TASKS_QUEUED_CHANNEL,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
