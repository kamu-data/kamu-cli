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
            wakeup_listener: SqlitePollingWakeupListener::new(
                pool,
                // Descending scan by primary key finds the latest match within a few rows
                r#"
                SELECT (
                    SELECT event_id FROM task_events
                        WHERE event_type IN ('TaskEventCreated', 'TaskEventRequeued')
                        ORDER BY event_id DESC
                        LIMIT 1
                )
                "#,
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
