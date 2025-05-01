// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use kamu_task_system::{
    LogicalPlan,
    LogicalPlanProbe,
    TaskMetadata,
    TaskScheduler,
    TaskState,
    TaskStatus,
};
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use time_source::SystemTimeSourceStub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_creates_task() {
    let task_sched = create_task_scheduler();

    let logical_plan_expected: LogicalPlan = LogicalPlanProbe {
        ..LogicalPlanProbe::default()
    }
    .into();

    let metadata_expected = TaskMetadata::from(vec![("foo", "x"), ("bar", "y")]);

    let task_state_actual = task_sched
        .create_task(
            logical_plan_expected.clone(),
            Some(metadata_expected.clone()),
        )
        .await
        .unwrap();

    assert_matches!(task_state_actual, TaskState {
        attempts,
        logical_plan,
        metadata,
        cancellation_requested_at: None,
        ..
    } if attempts.is_empty() && logical_plan == logical_plan_expected && metadata == metadata_expected );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_queues_tasks() {
    let task_sched = create_task_scheduler();

    let maybe_task_0 = task_sched.try_take().await.unwrap();
    assert!(maybe_task_0.is_none());

    let task_id_1 = task_sched
        .create_task(
            LogicalPlanProbe {
                ..LogicalPlanProbe::default()
            }
            .into(),
            None,
        )
        .await
        .unwrap()
        .task_id;

    let task_id_2 = task_sched
        .create_task(
            LogicalPlanProbe {
                ..LogicalPlanProbe::default()
            }
            .into(),
            None,
        )
        .await
        .unwrap()
        .task_id;

    let maybe_task_1 = task_sched.try_take().await.unwrap();
    assert!(maybe_task_1.is_some_and(|t| t.task_id == task_id_1));

    let maybe_task_2 = task_sched.try_take().await.unwrap();
    assert!(maybe_task_2.is_some_and(|t| t.task_id == task_id_2));

    let maybe_task_3 = task_sched.try_take().await.unwrap();
    assert!(maybe_task_3.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_taken_task_is_running() {
    let task_sched = create_task_scheduler();

    let task_id_1 = task_sched
        .create_task(
            LogicalPlanProbe {
                ..LogicalPlanProbe::default()
            }
            .into(),
            None,
        )
        .await
        .unwrap()
        .task_id;

    let task_id_2 = task_sched
        .create_task(
            LogicalPlanProbe {
                ..LogicalPlanProbe::default()
            }
            .into(),
            None,
        )
        .await
        .unwrap()
        .task_id;

    let task_1 = task_sched.get_task(task_id_1).await.unwrap();
    let task_2 = task_sched.get_task(task_id_2).await.unwrap();
    assert_eq!(task_1.status(), TaskStatus::Queued);
    assert_eq!(task_2.status(), TaskStatus::Queued);

    task_sched.try_take().await.unwrap();

    let task_1 = task_sched.get_task(task_id_1).await.unwrap();
    let task_2 = task_sched.get_task(task_id_2).await.unwrap();
    assert_eq!(task_1.status(), TaskStatus::Running);
    assert_eq!(task_2.status(), TaskStatus::Queued);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_cancellation() {
    let task_sched = create_task_scheduler();

    let task_id_1 = task_sched
        .create_task(
            LogicalPlanProbe {
                ..LogicalPlanProbe::default()
            }
            .into(),
            None,
        )
        .await
        .unwrap()
        .task_id;

    let task_id_2 = task_sched
        .create_task(
            LogicalPlanProbe {
                ..LogicalPlanProbe::default()
            }
            .into(),
            None,
        )
        .await
        .unwrap()
        .task_id;

    task_sched.cancel_task(task_id_1).await.unwrap();

    let maybe_task = task_sched.try_take().await.unwrap();
    assert!(maybe_task.is_some_and(|t| t.task_id == task_id_2));

    let maybe_another_task = task_sched.try_take().await.unwrap();
    assert!(maybe_another_task.is_none());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_task_scheduler() -> impl TaskScheduler {
    let task_event_store = Arc::new(InMemoryTaskEventStore::new());
    let time_source = Arc::new(SystemTimeSourceStub::new());
    TaskSchedulerImpl::new(task_event_store, time_source)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
