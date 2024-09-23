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

use database_common::NoOpDatabasePlugin;
use dill::{Catalog, CatalogBuilder};
use kamu_task_system::*;
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::*;
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pre_run_requeues_running_tasks() {
    let harness = TaskExecutorHarness::new(MockOutbox::new(), MockTaskLogicalPlanRunner::new());

    // Schedule 3 tasks
    let task_id_1 = harness.schedule_probe_task().await;
    let task_id_2 = harness.schedule_probe_task().await;
    let task_id_3 = harness.schedule_probe_task().await;

    // Make 2 of 3 Running
    let task_1 = harness.try_take_task().await;
    let task_2 = harness.try_take_task().await;
    assert_matches!(task_1, Some(t) if t.task_id == task_id_1);
    assert_matches!(task_2, Some(t) if t.task_id == task_id_2);

    // 1, 2 Running  while 3 should be Queued
    let task_1 = harness.get_task(task_id_1).await;
    let task_2 = harness.get_task(task_id_2).await;
    let task_3 = harness.get_task(task_id_3).await;
    assert_eq!(task_1.status(), TaskStatus::Running);
    assert_eq!(task_2.status(), TaskStatus::Running);
    assert_eq!(task_3.status(), TaskStatus::Queued);

    // A pre-run must convert all Running into Queued
    harness.task_executor.pre_run().await.unwrap();

    // 1, 2, 3 - Queued
    let task_1 = harness.get_task(task_id_1).await;
    let task_2 = harness.get_task(task_id_2).await;
    let task_3 = harness.get_task(task_id_3).await;
    assert_eq!(task_1.status(), TaskStatus::Queued);
    assert_eq!(task_2.status(), TaskStatus::Queued);
    assert_eq!(task_3.status(), TaskStatus::Queued);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_run_single_task() {
    // Expect the only task to notify about Running and Finished transitions
    let mut mock_outbox = MockOutbox::new();
    TaskExecutorHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(0));

    // Expect logical plan runner to run probe
    let mut mock_plan_runner = MockTaskLogicalPlanRunner::new();
    TaskExecutorHarness::add_run_probe_plan_expectations(
        &mut mock_plan_runner,
        Probe::default(),
        1,
    );

    // Schedule the only task
    let harness = TaskExecutorHarness::new(mock_outbox, mock_plan_runner);
    let task_id = harness.schedule_probe_task().await;
    let task = harness.get_task(task_id).await;
    assert_eq!(task.status(), TaskStatus::Queued);

    // Run execution loop
    harness.task_executor.run_single_task().await.unwrap();

    // Check the task has Finished status at the end
    let task = harness.get_task(task_id).await;
    assert_eq!(task.status(), TaskStatus::Finished);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_run_two_of_three_tasks() {
    // Expect 2 of 3 tasks to notify about Running and Finished transitions
    let mut mock_outbox = MockOutbox::new();
    TaskExecutorHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(0));
    TaskExecutorHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(1));

    // Expect logical plan runner to run probe twice
    let mut mock_plan_runner = MockTaskLogicalPlanRunner::new();
    TaskExecutorHarness::add_run_probe_plan_expectations(
        &mut mock_plan_runner,
        Probe::default(),
        2,
    );

    // Schedule 3 tasks
    let harness = TaskExecutorHarness::new(mock_outbox, mock_plan_runner);
    let task_id_1 = harness.schedule_probe_task().await;
    let task_id_2 = harness.schedule_probe_task().await;
    let task_id_3 = harness.schedule_probe_task().await;

    // All 3 must be in Queued state before runs
    let task_1 = harness.get_task(task_id_1).await;
    let task_2 = harness.get_task(task_id_2).await;
    let task_3 = harness.get_task(task_id_3).await;
    assert_eq!(task_1.status(), TaskStatus::Queued);
    assert_eq!(task_2.status(), TaskStatus::Queued);
    assert_eq!(task_3.status(), TaskStatus::Queued);

    // Run execution loop twice
    harness.task_executor.run_single_task().await.unwrap();
    harness.task_executor.run_single_task().await.unwrap();

    // Check the 2 tasks Finished, 3rd is still Queued
    let task_1 = harness.get_task(task_id_1).await;
    let task_2 = harness.get_task(task_id_2).await;
    let task_3 = harness.get_task(task_id_3).await;
    assert_eq!(task_1.status(), TaskStatus::Finished);
    assert_eq!(task_2.status(), TaskStatus::Finished);
    assert_eq!(task_3.status(), TaskStatus::Queued);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TaskExecutorHarness {
    _catalog: Catalog,
    task_executor: Arc<dyn TaskExecutor>,
    task_scheduler: Arc<dyn TaskScheduler>,
}

impl TaskExecutorHarness {
    pub fn new(mock_outbox: MockOutbox, mock_plan_runner: MockTaskLogicalPlanRunner) -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<TaskExecutorImpl>()
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskEventStore>()
            .add_value(mock_plan_runner)
            .bind::<dyn TaskLogicalPlanRunner, MockTaskLogicalPlanRunner>()
            .add_value(mock_outbox)
            .bind::<dyn Outbox, MockOutbox>()
            .add::<SystemTimeSourceDefault>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let task_executor = catalog.get_one().unwrap();
        let task_scheduler = catalog.get_one().unwrap();

        Self {
            _catalog: catalog,
            task_executor,
            task_scheduler,
        }
    }

    async fn schedule_probe_task(&self) -> TaskID {
        self.task_scheduler
            .create_task(Probe { ..Probe::default() }.into(), None)
            .await
            .unwrap()
            .task_id
    }

    async fn try_take_task(&self) -> Option<Task> {
        self.task_scheduler.try_take().await.unwrap()
    }

    async fn get_task(&self, task_id: TaskID) -> TaskState {
        self.task_scheduler.get_task(task_id).await.unwrap()
    }

    fn add_outbox_task_expectations(mock_outbox: &mut MockOutbox, a_task_id: TaskID) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR),
                function(move |message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<TaskProgressMessage>(message_as_json.clone()),
                        Ok(TaskProgressMessage::Running(TaskProgressMessageRunning {
                            task_id,
                            ..
                        })) if task_id == a_task_id
                    )
                }),
            )
            .times(1)
            .returning(|_, _| Ok(()));

        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR),
                function(move |message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<TaskProgressMessage>(message_as_json.clone()),
                        Ok(TaskProgressMessage::Finished(TaskProgressMessageFinished {
                            task_id,
                            ..
                        })) if task_id == a_task_id
                    )
                }),
            )
            .times(1)
            .returning(|_, _| Ok(()));
    }

    fn add_run_probe_plan_expectations(
        mock_plan_runner: &mut MockTaskLogicalPlanRunner,
        probe: Probe,
        times: usize,
    ) {
        mock_plan_runner
            .expect_run_plan()
            .with(eq(LogicalPlan::Probe(probe)))
            .times(times)
            .returning(|_| Ok(TaskOutcome::Success(TaskResult::Empty)));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TaskLogicalPlanRunner {}

    #[async_trait::async_trait]
    impl TaskLogicalPlanRunner for TaskLogicalPlanRunner {
        async fn run_plan(&self, logical_plan: &LogicalPlan) -> Result<TaskOutcome, InternalError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
