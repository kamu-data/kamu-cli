// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_task_system::TaskQueueWakeupSource;
use kamu_wakeup_listener_sqlite::SqlitePollingWakeupListener;
use wakeup_listener::WakeupListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteTaskQueueWakeupSource {
    wakeup_listener: SqlitePollingWakeupListener,
}

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn TaskQueueWakeupSource)]
impl SqliteTaskQueueWakeupSource {
    pub fn new(pool: Arc<sqlx::SqlitePool>) -> Self {
        Self {
            // Note: any new task event wakes up the agent, not only queueing ones.
            // This is acceptable, as the agent will simply re-check the queue
            wakeup_listener: SqlitePollingWakeupListener::new(
                pool,
                "SELECT MAX(event_id) FROM task_events",
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskQueueWakeupSource for SqliteTaskQueueWakeupSource {
    fn wakeup_listener(&self) -> &dyn WakeupListener {
        &self.wakeup_listener
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
