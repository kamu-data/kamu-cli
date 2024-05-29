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

use database_common::FakeDatabasePlugin;
use dill::CatalogBuilder;
use kamu_core::SystemTimeSourceStub;
use kamu_task_system::{LogicalPlan, Probe, TaskScheduler, TaskState, TaskStatus};
use kamu_task_system_inmem::TaskSystemEventStoreInMemory;
use kamu_task_system_services::TaskSchedulerImpl;

////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_creates_task() {
    prepare_task_scheduler(|task_sched| async move {
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
    })
    .await;
}

#[test_log::test(tokio::test)]
async fn test_queues_tasks() {
    prepare_task_scheduler(|task_sched| async move {
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
    })
    .await;
}

#[test_log::test(tokio::test)]
async fn test_task_cancellation() {
    prepare_task_scheduler(|task_sched| async move {
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
    })
    .await;
}

////////////////////////////////////////////////////////////////////////////////

async fn prepare_task_scheduler<F, R>(callback: F)
where
    F: FnOnce(TaskSchedulerImpl) -> R + Send,
    R: std::future::Future<Output = ()> + Send,
{
    let mut catalog_builder = CatalogBuilder::new();

    FakeDatabasePlugin::init_database_components(&mut catalog_builder);

    let catalog = catalog_builder
        .add::<TaskSystemEventStoreInMemory>()
        .build();

    let time_source = Arc::new(SystemTimeSourceStub::new());
    let task_scheduler = TaskSchedulerImpl::new(time_source, catalog);

    callback(task_scheduler).await;
}

////////////////////////////////////////////////////////////////////////////////
