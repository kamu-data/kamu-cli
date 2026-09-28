// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use database_common::SqliteTransactionManager;
use database_common_macros::transactional_method1;
use dill::{Catalog, CatalogBuilder};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_task_system::*;
use kamu_task_system_sqlite::{SqliteTaskEventStore, SqliteTaskQueueWakeupSource};
use sqlx::SqlitePool;
use wakeup_listener::WakeHint;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_wakes_up_only_when_task_is_queued(sqlite_pool: SqlitePool) {
    let harness = SqliteTaskQueueWakeupHarness::new(sqlite_pool);

    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    let task_id = harness.new_task_id().await;
    let last_event_id = harness
        .save_event(
            task_id,
            None,
            TaskEventCreated {
                event_time: Utc::now(),
                task_id,
                logical_plan: LogicalPlanProbe::default().into_logical_plan(),
                metadata: None,
            }
            .into(),
        )
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);

    // A task starts running: not a reason to wake up
    let last_event_id = harness
        .save_event(
            task_id,
            Some(last_event_id),
            TaskEventRunning {
                event_time: Utc::now(),
                task_id,
            }
            .into(),
        )
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);

    // A requeue (e.g. crash recovery) must wake up the agent
    let last_event_id = harness
        .save_event(
            task_id,
            Some(last_event_id),
            TaskEventRequeued {
                event_time: Utc::now(),
                task_id,
            }
            .into(),
        )
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Signaled);

    // A task runs again and finishes: not a reason to wake up
    let last_event_id = harness
        .save_event(
            task_id,
            Some(last_event_id),
            TaskEventRunning {
                event_time: Utc::now(),
                task_id,
            }
            .into(),
        )
        .await;
    harness
        .save_event(
            task_id,
            Some(last_event_id),
            TaskEventFinished {
                event_time: Utc::now(),
                task_id,
                outcome: TaskOutcome::Success(TaskResult::empty()),
            }
            .into(),
        )
        .await;
    assert_matches!(harness.wait_wake().await, WakeHint::Timeout);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteTaskQueueWakeupHarness {
    catalog: Catalog,
    wakeup_source: Arc<dyn TaskQueueWakeupSource>,
}

impl SqliteTaskQueueWakeupHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteTaskEventStore>();
        catalog_builder.add::<SqliteTaskQueueWakeupSource>();

        let catalog = catalog_builder.build();

        // Keep a single instance, as it owns the listening connection
        let wakeup_source = catalog.get_one().unwrap();

        Self {
            catalog,
            wakeup_source,
        }
    }

    async fn wait_wake(&self) -> WakeHint {
        self.wakeup_source
            .wakeup_listener()
            .wait_wake(Duration::from_millis(500), Duration::from_millis(10))
            .await
            .unwrap()
    }

    async fn new_task_id(&self) -> TaskID {
        self.new_task_id_impl().await.unwrap()
    }

    async fn save_event(
        &self,
        task_id: TaskID,
        maybe_prev_stored_event_id: Option<EventID>,
        event: TaskEvent,
    ) -> EventID {
        self.save_event_impl(task_id, maybe_prev_stored_event_id, event)
            .await
            .unwrap()
    }

    #[transactional_method1(task_event_store: Arc<dyn TaskEventStore>)]
    async fn new_task_id_impl(&self) -> Result<TaskID, InternalError> {
        task_event_store.new_task_id().await
    }

    #[transactional_method1(task_event_store: Arc<dyn TaskEventStore>)]
    async fn save_event_impl(
        &self,
        task_id: TaskID,
        maybe_prev_stored_event_id: Option<EventID>,
        event: TaskEvent,
    ) -> Result<EventID, InternalError> {
        task_event_store
            .save_events(&task_id, maybe_prev_stored_event_id, vec![event])
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
