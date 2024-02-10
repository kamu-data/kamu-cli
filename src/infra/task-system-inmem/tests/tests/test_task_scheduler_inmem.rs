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

use kamu_core::SystemTimeSourceStub;
use kamu_task_system_inmem::domain::*;
use kamu_task_system_inmem::*;

#[test_log::test(tokio::test)]
async fn test_creates_task() {
    let event_store = Arc::new(TaskSystemEventStoreInMemory::new());
    let time_source = Arc::new(SystemTimeSourceStub::new());
    let task_sched = TaskSchedulerInMemory::new(event_store, time_source);

    let logical_plan_expected: LogicalPlan = Probe { ..Probe::default() }.into();

    let task_state_actual = task_sched
        .create_task(logical_plan_expected.clone())
        .await
        .unwrap();

    assert_matches!(task_state_actual, TaskState {
        status: TaskStatus::Queued,
        cancellation_requested: false,
        logical_plan,
        ran_at: None,
        cancellation_requested_at: None,
        finished_at: None,
        ..
    } if logical_plan == logical_plan_expected);
}

#[test_log::test(tokio::test)]
async fn test_queues_tasks() {
    let event_store = Arc::new(TaskSystemEventStoreInMemory::new());
    let time_source = Arc::new(SystemTimeSourceStub::new());
    let task_sched = TaskSchedulerInMemory::new(event_store, time_source);

    let task_id_1 = task_sched
        .create_task(Probe { ..Probe::default() }.into())
        .await
        .unwrap()
        .task_id;

    let task_id_2 = task_sched
        .create_task(Probe { ..Probe::default() }.into())
        .await
        .unwrap()
        .task_id;

    assert_eq!(task_sched.try_take().await.unwrap(), Some(task_id_1));
    assert_eq!(task_sched.try_take().await.unwrap(), Some(task_id_2));
    assert_eq!(task_sched.try_take().await.unwrap(), None);
}

#[test_log::test(tokio::test)]
async fn test_task_cancellation() {
    let event_store = Arc::new(TaskSystemEventStoreInMemory::new());
    let time_source = Arc::new(SystemTimeSourceStub::new());
    let task_sched = TaskSchedulerInMemory::new(event_store, time_source);

    let task_id_1 = task_sched
        .create_task(Probe { ..Probe::default() }.into())
        .await
        .unwrap()
        .task_id;

    let task_id_2 = task_sched
        .create_task(Probe { ..Probe::default() }.into())
        .await
        .unwrap()
        .task_id;

    task_sched.cancel_task(task_id_1).await.unwrap();

    assert_eq!(task_sched.try_take().await.unwrap(), Some(task_id_2));
    assert_eq!(task_sched.try_take().await.unwrap(), None);
}
