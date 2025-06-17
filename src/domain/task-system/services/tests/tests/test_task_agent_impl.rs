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
use kamu::utils::ipfs_wrapper::IpfsClient;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{DidGeneratorDefault, TenancyConfig};
use kamu_datasets::DatasetEnvVarsConfig;
use kamu_datasets_inmem::{InMemoryDatasetDependencyRepository, InMemoryDatasetEnvVarRepository};
use kamu_datasets_services::{DatasetEnvVarServiceImpl, DependencyGraphServiceImpl};
use kamu_task_system::*;
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::*;
use messaging_outbox::{MockOutbox, Outbox};
use mockall::predicate::{eq, function};
use odf::dataset::{DatasetFactoryImpl, IpfsGateway};
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pre_run_requeues_running_tasks() {
    let mut mock_task_planner = MockTaskDefinitionPlanner::new();
    let mut mock_task_runner = MockTaskRunner::new();
    TaskAgentHarness::add_supported_task_types_expectations(
        &mut mock_task_planner,
        &mut mock_task_runner,
    );

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
async fn test_run_single_task() {
    // Expect the only task to notify about Running and Finished transitions
    let mut mock_outbox = MockOutbox::new();
    TaskAgentHarness::add_outbox_task_expectations(&mut mock_outbox, TaskID::new(0));

    // Expect logical plan runner to run probe
    let mut mock_task_planner = MockTaskDefinitionPlanner::new();
    let mut mock_task_runner = MockTaskRunner::new();
    TaskAgentHarness::add_supported_task_types_expectations(
        &mut mock_task_planner,
        &mut mock_task_runner,
    );
    TaskAgentHarness::add_plan_probe_plan_expectations(
        &mut mock_task_planner,
        LogicalPlanProbe::default(),
        1,
    );
    TaskAgentHarness::add_run_probe_plan_expectations(
        &mut mock_task_runner,
        LogicalPlanProbe::default(),
        1,
    );

    // Schedule the only task
    let harness = TaskAgentHarness::new(mock_outbox, mock_task_planner, mock_task_runner);
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

    // Expect logical plan runner to run probe twice
    let mut mock_task_planner = MockTaskDefinitionPlanner::new();
    let mut mock_task_runner = MockTaskRunner::new();
    TaskAgentHarness::add_supported_task_types_expectations(
        &mut mock_task_planner,
        &mut mock_task_runner,
    );
    TaskAgentHarness::add_plan_probe_plan_expectations(
        &mut mock_task_planner,
        LogicalPlanProbe::default(),
        2,
    );
    TaskAgentHarness::add_run_probe_plan_expectations(
        &mut mock_task_runner,
        LogicalPlanProbe::default(),
        2,
    );

    // Schedule 3 tasks
    let harness = TaskAgentHarness::new(mock_outbox, mock_task_planner, mock_task_runner);
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

struct TaskAgentHarness {
    _tempdir: TempDir,
    catalog: Catalog,
    task_agent: Arc<dyn TaskAgent>,
    task_scheduler: Arc<dyn TaskScheduler>,
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
            .add::<DidGeneratorDefault>()
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskEventStore>()
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
            .add::<DatasetEnvVarServiceImpl>()
            .add::<InMemoryDatasetEnvVarRepository>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder(
                datasets_dir,
            ))
            .add::<DatasetRegistrySoloUnitBridge>()
            .add::<odf::dataset::DatasetLfsBuilderDefault>()
            .add_value(CurrentAccountSubject::new_test())
            .add_value(TenancyConfig::SingleTenant)
            .add_value(TaskAgentConfig::new(chrono::Duration::seconds(1)))
            .add_value(DatasetEnvVarsConfig::sample());

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        let task_agent = catalog.get_one().unwrap();
        let task_scheduler = catalog.get_one().unwrap();

        Self {
            _tempdir: tempdir,
            catalog,
            task_agent,
            task_scheduler,
        }
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

    fn add_supported_task_types_expectations(
        mock_task_planner: &mut MockTaskDefinitionPlanner,
        mock_task_runner: &mut MockTaskRunner,
    ) {
        mock_task_planner
            .expect_supported_logic_plan_type()
            .return_const(LogicalPlanProbe::TYPE_ID);

        mock_task_runner
            .expect_supported_task_type()
            .return_const(TaskDefinitionProbe::TASK_TYPE);
    }

    fn add_plan_probe_plan_expectations(
        mock_task_planner: &mut MockTaskDefinitionPlanner,
        probe: LogicalPlanProbe,
        times: usize,
    ) {
        let probe_clone = probe.clone();

        mock_task_planner
            .expect_prepare_task_definition()
            .withf(move |_task_id, plan| {
                plan.plan_type == LogicalPlanProbe::TYPE_ID
                    && LogicalPlanProbe::from_logical_plan(plan).unwrap() == probe_clone
            })
            .times(times)
            .returning(move |_, _| {
                Ok(TaskDefinition::new(TaskDefinitionProbe {
                    probe: probe.clone(),
                }))
            });
    }

    fn add_run_probe_plan_expectations(
        mock_task_runner: &mut MockTaskRunner,
        probe: LogicalPlanProbe,
        times: usize,
    ) {
        let probe_plan = probe.clone();

        mock_task_runner
            .expect_run_task()
            .withf(move |td| {
                td.downcast_ref::<TaskDefinitionProbe>()
                    .is_some_and(|task_probe| task_probe.probe == probe_plan)
            })
            .times(times)
            .returning(move |_| {
                Ok(probe
                    .end_with_outcome
                    .clone()
                    .unwrap_or(TaskOutcome::Success(TaskResult::empty())))
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TaskRunner {}

    #[async_trait::async_trait]
    impl TaskRunner for TaskRunner {
        fn supported_task_type(&self) -> &'static str;

        async fn run_task(&self, task_definition: TaskDefinition) -> Result<TaskOutcome, InternalError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub TaskDefinitionPlanner {}

    #[async_trait::async_trait]
    impl TaskDefinitionPlanner for TaskDefinitionPlanner {
        fn supported_logic_plan_type(&self) -> &'static str;

        async fn prepare_task_definition(
            &self,
            task_id: TaskID,
            logical_plan: &LogicalPlan,
        ) -> Result<TaskDefinition, InternalError>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
