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
use kamu_wakeup_listener_postgres::PostgresNotifyWakeupListener;
use wakeup_listener::WakeupListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "tasks_queued";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresTaskQueueWakeupSource {
    wakeup_listener: PostgresNotifyWakeupListener,
}

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn TaskQueueWakeupSource)]
impl PostgresTaskQueueWakeupSource {
    pub fn new(pool: Arc<sqlx::PgPool>) -> Self {
        Self {
            wakeup_listener: PostgresNotifyWakeupListener::new(pool, NOTIFY_CHANNEL_NAME),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskQueueWakeupSource for PostgresTaskQueueWakeupSource {
    fn wakeup_listener(&self) -> &dyn WakeupListener {
        &self.wakeup_listener
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
