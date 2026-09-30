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

use database_common::NoOpDatabasePlugin;
use dill::{Catalog, CatalogBuilder};
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{DidGeneratorDefault, TenancyConfig};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::{DatasetEnvVarServiceNull, DependencyGraphServiceImpl};
use kamu_task_system::*;
use kamu_task_system_inmem::{InMemoryTaskEventStore, InMemoryTaskQueueWakeupSource};
use kamu_task_system_services::*;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;
use wakeup_listener::{WakeupListenerConfig, WakeupListenerMetrics};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pre_run_requeues_running_tasks() {
    let mock_task_planner = MockTaskDefinitionPlanner::new();
    let mock_task_runner = MockTaskRunner::new();

    let harness = TaskAgentHarness::new(MockOutbox::new(), mock_task_planner, mock_task_runner);

    // Schedule 3 tasks
    let task_id_1 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;
    let task_id_2 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;
    let task_id_3 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;

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

    // A recovery must convert all Running into Queued
    init_on_startup::run_startup_jobs(&harness.catalog)
        .await
        .unwrap();

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
async fn test_pre_run_requeues_running_tasks_across_pages() {
    let mock_task_planner = MockTaskDefinitionPlanner::new();
    let mock_task_runner = MockTaskRunner::new();

    let harness = TaskAgentHarness::new(MockOutbox::new(), mock_task_planner, mock_task_runner);

    // More running tasks than a recovery page holds
    let mut task_ids = Vec::new();
    for _ in 0..250 {
        task_ids.push(
            harness
                .schedule_probe_task(LogicalPlanProbe::default())
                .await,
        );
        harness.try_take_task().await.unwrap();
    }

    init_on_startup::run_startup_jobs(&harness.catalog)
        .await
        .unwrap();

    for task_id in task_ids {
        assert_eq!(
            harness.get_task(task_id).await.status(),
            TaskStatus::Queued,
            "{task_id}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pre_run_finishes_cancelled_running_tasks() {
    let harness = TaskAgentHarness::new(
        TaskAgentHarness::outbox_expecting_cancelled_task(TaskID::new(0)),
        MockTaskDefinitionPlanner::new(),
        MockTaskRunner::new(),
    );

    let task_id_1 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;
    let task_id_2 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;
    assert_eq!(task_id_1, TaskID::new(0));

    // Both running, then the 1st cancelled: its run cannot be interrupted
    harness.try_take_task().await.unwrap();
    harness.try_take_task().await.unwrap();
    harness.cancel_task(task_id_1).await;
    assert_eq!(
        harness.get_task(task_id_1).await.status(),
        TaskStatus::Running
    );

    // Interrupted by a restart: the cancelled one is finished, the other requeued
    init_on_startup::run_startup_jobs(&harness.catalog)
        .await
        .unwrap();

    let task_1 = harness.get_task(task_id_1).await;
    assert_eq!(task_1.status(), TaskStatus::Finished);
    assert_eq!(task_1.outcome, Some(TaskOutcome::Cancelled));
    assert_eq!(
        harness.get_task(task_id_2).await.status(),
        TaskStatus::Queued
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_run_single_task() {
    // Expect the only task to notify about Running and Finished transitions
    let mut mock_outbox = MockOutbox::new();
    TaskAgentHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(0));

    // Schedule the only task
    let harness = TaskAgentHarness::new(
        mock_outbox,
        MockTaskDefinitionPlanner::new(),
        MockTaskRunner::new(),
    );
    let task_id = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;
    let task = harness.get_task(task_id).await;
    assert_eq!(task.status(), TaskStatus::Queued);

    // Run execution loop
    harness.task_agent.run_single_task().await.unwrap();

    // Check the task has Finished status at the end
    let task = harness.get_task(task_id).await;
    assert_eq!(task.status(), TaskStatus::Finished);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_run_two_of_three_tasks() {
    // Expect 2 of 3 tasks to notify about Running and Finished transitions
    let mut mock_outbox = MockOutbox::new();
    TaskAgentHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(0));
    TaskAgentHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(1));

    // Schedule 3 tasks (use real probe planner/runner registered in harness)
    let harness = TaskAgentHarness::new(
        mock_outbox,
        MockTaskDefinitionPlanner::new(),
        MockTaskRunner::new(),
    );
    let task_id_1 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;
    let task_id_2 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;
    let task_id_3 = harness
        .schedule_probe_task(LogicalPlanProbe::default())
        .await;

    // All 3 must be in Queued state before runs
    let task_1 = harness.get_task(task_id_1).await;
    let task_2 = harness.get_task(task_id_2).await;
    let task_3 = harness.get_task(task_id_3).await;
    assert_eq!(task_1.status(), TaskStatus::Queued);
    assert_eq!(task_2.status(), TaskStatus::Queued);
    assert_eq!(task_3.status(), TaskStatus::Queued);

    // Run execution loop twice
    harness.task_agent.run_single_task().await.unwrap();
    harness.task_agent.run_single_task().await.unwrap();

    // Check the 2 tasks Finished, 3rd is still Queued
    let task_1 = harness.get_task(task_id_1).await;
    let task_2 = harness.get_task(task_id_2).await;
    let task_3 = harness.get_task(task_id_3).await;
    assert_eq!(task_1.status(), TaskStatus::Finished);
    assert_eq!(task_2.status(), TaskStatus::Finished);
    assert_eq!(task_3.status(), TaskStatus::Queued);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_agent_wakes_up_when_task_is_queued() {
    // Expect the only task to notify about Running and Finished transitions
    let mut mock_outbox = MockOutbox::new();
    TaskAgentHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(0));

    let harness = TaskAgentHarness::new(
        mock_outbox,
        MockTaskDefinitionPlanner::new(),
        MockTaskRunner::new(),
    );

    // Start the agent on an empty queue, then schedule a task a bit later,
    // when the agent is already waiting for a wakeup
    let (run_result, task_id) = tokio::time::timeout(LISTENING_TIMEOUT / 10, async {
        tokio::join!(harness.task_agent.run_single_task(), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            harness
                .schedule_probe_task(LogicalPlanProbe::default())
                .await
        })
    })
    .await
    .expect("Agent must be woken up by the queued task, not by the listening timeout");
    run_result.unwrap();

    // Check the task has Finished status at the end
    let task = harness.get_task(task_id).await;
    assert_eq!(task.status(), TaskStatus::Finished);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_metrics_track_outcomes_and_running_task() {
    let mut mock_outbox = MockOutbox::new();
    TaskAgentHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(0));
    TaskAgentHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(1));

    let harness = TaskAgentHarness::new(
        mock_outbox,
        MockTaskDefinitionPlanner::new(),
        MockTaskRunner::new(),
    );
    harness
        .schedule_probe_task(LogicalPlanProbe {
            busy_time: Some(Duration::from_millis(200)),
            ..LogicalPlanProbe::default()
        })
        .await;
    harness
        .schedule_probe_task(LogicalPlanProbe {
            end_with_outcome: Some(TaskOutcome::Failed(TaskError::empty_recoverable())),
            ..LogicalPlanProbe::default()
        })
        .await;
    assert!(!harness.is_task_running());

    // The running task is visible while it runs
    let (run_result, was_task_running) =
        tokio::join!(harness.task_agent.run_single_task(), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            harness.is_task_running()
        });
    run_result.unwrap();
    assert!(was_task_running);
    assert!(!harness.is_task_running());

    harness.task_agent.run_single_task().await.unwrap();

    assert_eq!(harness.finished_tasks("success"), 1);
    assert_eq!(harness.finished_tasks("failed"), 1);
    assert_eq!(harness.finished_tasks("cancelled"), 0);
    assert_eq!(harness.task_queue_wait_samples(), 2);
    assert!(!harness.is_task_running());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const LISTENING_TIMEOUT: Duration = Duration::from_mins(1);

struct TaskAgentHarness {
    _tempdir: TempDir,
    catalog: Catalog,
    task_agent: Arc<dyn TaskAgent>,
    task_scheduler: Arc<dyn TaskScheduler>,
    task_agent_metrics: Arc<TaskAgentMetrics>,
}

impl TaskAgentHarness {
    pub fn new(
        mock_outbox: MockOutbox,
        mock_task_planner: MockTaskDefinitionPlanner,
        mock_task_runner: MockTaskRunner,
    ) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let repos_dir = tempdir.path().join("repos");
        std::fs::create_dir(&repos_dir).unwrap();

        let mut b = CatalogBuilder::new();
        b.add::<TaskAgentImpl>()
            .add::<TaskAgentMetrics>()
            .add::<DidGeneratorDefault>()
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskEventStore>()
            .add::<InMemoryWakeupHub>()
            .add::<WakeupListenerMetrics>()
            .add::<InMemoryTaskQueueWakeupSource>()
            .add_value(mock_outbox)
            .add_value(mock_task_runner)
            .bind::<dyn TaskRunner, MockTaskRunner>()
            .add_value(mock_task_planner)
            .bind::<dyn TaskDefinitionPlanner, MockTaskDefinitionPlanner>()
            .bind::<dyn Outbox, MockOutbox>()
            .add::<SystemTimeSourceDefault>()
            .add::<PullRequestPlannerImpl>()
            .add::<CompactionPlannerImpl>()
            .add::<ResetPlannerImpl>()
            .add::<TransformRequestPlannerImpl>()
            .add::<SyncRequestBuilder>()
            .add::<DatasetFactoryImpl>()
            .add::<RemoteAliasesRegistryImpl>()
            .add_value(RemoteReposDir::new(repos_dir))
            .add::<RemoteRepositoryRegistryImpl>()
            .add::<RemoteAliasResolverImpl>()
            .add_value(IpfsGateway::default())
            .add_value(IpfsClient::default())
            .add::<odf::dataset::DummyOdfServerAccessTokenResolver>()
            .add::<DatasetEnvVarServiceNull>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<DatasetRegistrySoloUnitBridge>()
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_value(WakeupListenerConfig {
                min_debounce_interval: Duration::from_millis(10),
                // Long enough to make sure tests never rely on listening timeouts
                max_listening_timeout: LISTENING_TIMEOUT,
            })
            .add::<ProbeTaskPlanner>()
            .add::<ProbeTaskRunner>()
            .add_value(SecretsEncryptionConfig::sample());

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let task_agent = catalog.get_one().unwrap();
        let task_scheduler = catalog.get_one().unwrap();
        let task_agent_metrics = catalog.get_one().unwrap();

        Self {
            _tempdir: tempdir,
            catalog,
            task_agent,
            task_scheduler,
            task_agent_metrics,
        }
    }

    fn finished_tasks(&self, outcome: &str) -> u64 {
        self.task_agent_metrics
            .task_duration_seconds
            .with_label_values(&[LogicalPlanProbe::TYPE_ID, outcome])
            .get_sample_count()
    }

    fn task_queue_wait_samples(&self) -> u64 {
        self.task_agent_metrics
            .task_queue_wait_seconds
            .get_sample_count()
    }

    fn is_task_running(&self) -> bool {
        self.task_agent_metrics
            .running_task_started_timestamp_seconds
            .with_label_values(&[MAIN_TASK_EXECUTOR])
            .get()
            > 0.0
    }

    async fn schedule_probe_task(&self, probe_plan: LogicalPlanProbe) -> TaskID {
        let probe_plan = probe_plan.into_logical_plan();

        self.task_scheduler
            .create_task(probe_plan, None)
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

    async fn cancel_task(&self, task_id: TaskID) {
        self.task_scheduler.cancel_task(task_id).await.unwrap();
    }

    fn outbox_expecting_cancelled_task(a_task_id: TaskID) -> MockOutbox {
        let mut mock_outbox = MockOutbox::new();
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_TASK_AGENT),
                function(move |message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<TaskProgressMessage>(message_as_json.clone()),
                        Ok(TaskProgressMessage::Finished(TaskProgressMessageFinished {
                            task_id,
                            outcome: TaskOutcome::Cancelled,
                            ..
                        })) if task_id == a_task_id
                    )
                }),
                eq(1),
            )
            .times(1)
            .returning(|_, _, _| Ok(()));
        mock_outbox
    }

    fn add_outbox_task_expectations(mock_outbox: &mut MockOutbox, a_task_id: TaskID) {
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_TASK_AGENT),
                function(move |message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<TaskProgressMessage>(message_as_json.clone()),
                        Ok(TaskProgressMessage::Running(TaskProgressMessageRunning {
                            task_id,
                            ..
                        })) if task_id == a_task_id
                    )
                }),
                eq(1),
            )
            .times(1)
            .returning(|_, _, _| Ok(()));

        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_TASK_AGENT),
                function(move |message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<TaskProgressMessage>(message_as_json.clone()),
                        Ok(TaskProgressMessage::Finished(TaskProgressMessageFinished {
                            task_id,
                            ..
                        })) if task_id == a_task_id
                    )
                }),
                eq(1),
            )
            .times(1)
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TaskRunner {}

    #[async_trait::async_trait]
    impl TaskRunner for TaskRunner {
        async fn run_task(&self, task_definition: TaskDefinition) -> Result<TaskOutcome, InternalError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TaskDefinitionPlanner {}

    #[async_trait::async_trait]
    impl TaskDefinitionPlanner for TaskDefinitionPlanner {
        async fn prepare_task_definition(
            &self,
            task_id: TaskID,
            logical_plan: &LogicalPlan,
        ) -> Result<TaskDefinition, InternalError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
