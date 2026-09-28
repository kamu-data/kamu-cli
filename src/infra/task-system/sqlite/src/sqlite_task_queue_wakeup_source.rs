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
use kamu_wakeup_listener_sqlite::{SqlitePollingChannel, SqlitePollingHub};
use wakeup_listener::{HubWakeupListener, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const POLLING_CHANNEL: SqlitePollingChannel = SqlitePollingChannel {
    name: "tasks_queued",
    // Descending scan by primary key finds the latest match within a few rows
    max_id_query: r#"
        SELECT (
            SELECT event_id FROM task_events
                WHERE event_type IN ('TaskEventCreated', 'TaskEventRequeued')
                ORDER BY event_id DESC
                LIMIT 1
        )
    "#,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteTaskQueueWakeupSource {
    hub: Arc<SqlitePollingHub>,
}

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn TaskQueueWakeupSource)]
impl SqliteTaskQueueWakeupSource {
    pub fn new(hub: Arc<SqlitePollingHub>) -> Self {
        Self { hub }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskQueueWakeupSource for SqliteTaskQueueWakeupSource {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(self.hub.clone(), POLLING_CHANNEL))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
