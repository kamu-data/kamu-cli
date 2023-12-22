// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, DurationRound, Utc};
use dill::*;
use event_bus::{AsyncEventHandler, EventBus};
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_task_system::*;
use kamu_task_system_inmem::{TaskSchedulerInMemory, TaskSystemEventStoreInMemory};
use opendatafabric::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_read_initial_config_and_queue_properly() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(30).into(),
        )
        .await;

    let bar_id = harness.create_root_dataset("bar").await;
    harness
        .set_dataset_flow_schedule(
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(45).into(),
        )
        .await;

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    let _ = tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = tokio::time::sleep(std::time::Duration::from_millis(60)) => Ok(()),
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();

    let foo_flow_key = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();

    let state = test_flow_listener.state.lock().unwrap();
    assert_eq!(3, state.snapshots.len());

    let start_moment = state.snapshots[0].0;
    let foo_moment = state.snapshots[1].0;
    let bar_moment = state.snapshots[2].0;

    assert_eq!(start_time, start_moment);
    assert_eq!((foo_moment - start_moment), Duration::milliseconds(30)); // planned time for "foo"
    assert_eq!((bar_moment - start_moment), Duration::milliseconds(45)); // planned time for "bar"

    assert_flow_test_checks(&[
        // Snapshot 0: after initial queueing
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (&bar_flow_key, FlowStatus::Queued, None),
            ],
        },
        // Snapshot 1: period passed for 'foo', but not yet for 'bar'
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (&bar_flow_key, FlowStatus::Queued, None),
            ],
        },
        // Snapshot 2: period passed for 'foo' and for 'bar'
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (&bar_flow_key, FlowStatus::Scheduled, None),
            ],
        },
    ]);

    let task_ids = harness.snapshot_all_current_task_ids().await;
    assert_eq!(task_ids.len(), 2);

    let task_test_checks = [
        TaskTestCheck {
            task_id: task_ids[0],
            flow_key: &foo_flow_key,
            dataset_id: &foo_id,
        },
        TaskTestCheck {
            task_id: task_ids[1],
            flow_key: &bar_flow_key,
            dataset_id: &bar_id,
        },
    ];

    for test_check in task_test_checks {
        let task = harness
            .task_scheduler
            .get_task(test_check.task_id)
            .await
            .unwrap();
        let flow_final_state = state
            .snapshots
            .last()
            .unwrap()
            .1
            .get(test_check.flow_key)
            .unwrap();
        let flow_task_id = *flow_final_state.task_ids.first().unwrap();
        expect_running_dataset_update_task(&task, test_check.dataset_id, flow_task_id);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    // Note: only "foo" has auto-schedule, "bar" hasn't
    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(30).into(),
        )
        .await;

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key: FlowKey = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    let _ = tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // Sleep < "foo" period
            tokio::time::sleep(std::time::Duration::from_millis(18)).await;
            let new_time = start_time + Duration::milliseconds(18);
            harness.trigger_manual_flow(new_time, foo_flow_key.clone()).await; // "foo" pending already
            harness.trigger_manual_flow(new_time, bar_flow_key.clone()).await; // "bar" not queued, starts soon

            // Wake up after foo scheduling
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            let new_time = new_time + Duration::milliseconds(20);
            harness.trigger_manual_flow(new_time, foo_flow_key.clone()).await; // "foo" pending already, even running
            harness.trigger_manual_flow(new_time, bar_flow_key.clone()).await; // "bar" pending already, event running

            // Make sure nothing got scheduled in near time
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

         } => Ok(()),
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();

    let state = test_flow_listener.state.lock().unwrap();
    assert_eq!(3, state.snapshots.len());

    let start_moment = state.snapshots[0].0;
    let bar_moment = state.snapshots[1].0;
    let foo_moment = state.snapshots[2].0;

    assert_eq!(start_moment, start_time);
    assert_eq!((bar_moment - start_moment), Duration::milliseconds(20)); // next slot after 18ms trigger with 5ms align
    assert_eq!((foo_moment - start_moment), Duration::milliseconds(30)); // 30ms as planned

    assert_flow_test_checks(&[
        // Snapshot 0: after initial queueing, no "bar", only "foo"
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![(&foo_flow_key, FlowStatus::Queued, None)],
        },
        // Snapshot 1: "bar" had manual trigger
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (&bar_flow_key, FlowStatus::Scheduled, None),
            ],
        },
        // Snapshot 2: period passed for 'foo'
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (&bar_flow_key, FlowStatus::Scheduled, None),
            ],
        },
    ]);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    harness
        .set_dataset_flow_schedule(
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(30).into(),
        )
        .await;
    harness
        .set_dataset_flow_schedule(
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(40).into(),
        )
        .await;

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key: FlowKey = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();

    // Run scheduler concurrently with manual triggers script
    let _ = tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // Sleep < "foo" period
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            harness.delete_dataset(&foo_id).await;
            test_flow_listener
                .snapshot_flows(start_time + Duration::milliseconds(15))
                .await;

            // Wake up after bar scheduling
            tokio::time::sleep(std::time::Duration::from_millis(38)).await;
            harness.delete_dataset(&bar_id).await;
            test_flow_listener
                .snapshot_flows(start_time + Duration::milliseconds(55))
                .await;

            // Make sure nothing got scheduled in near time
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

         } => Ok(()),
    }
    .unwrap();

    let state = test_flow_listener.state.lock().unwrap();
    assert_eq!(4, state.snapshots.len());

    let start_moment = state.snapshots[0].0;
    let foo_del_moment = state.snapshots[1].0;
    let bar_sch_moment = state.snapshots[2].0;
    let bar_del_moment = state.snapshots[3].0;

    assert_eq!(start_moment, start_time);
    assert_eq!((foo_del_moment - start_moment), Duration::milliseconds(15));
    assert_eq!((bar_sch_moment - start_moment), Duration::milliseconds(40));
    assert_eq!((bar_del_moment - start_moment), Duration::milliseconds(55));

    assert_flow_test_checks(&[
        // Snapshot 0: after initial queueing
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (&bar_flow_key, FlowStatus::Queued, None),
            ],
        },
        // Snapshot 1: "foo" delete moment
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
            patterns: vec![
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Aborted),
                ),
                (&bar_flow_key, FlowStatus::Queued, None),
            ],
        },
        // Snapshot 2: period passed for 'bar'
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Aborted),
                ),
                (&bar_flow_key, FlowStatus::Scheduled, None),
            ],
        },
        // Snapshot 3: "bar" delete moment
        FlowTestCheck {
            snapshot: &state.snapshots[3].1,
            patterns: vec![
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Aborted),
                ),
                (
                    &bar_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Aborted),
                ),
            ],
        },
    ]);
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO:
//  - completing task with: success/failure/cancel
//  - scheduling next auto-trigger when task completes
//  - scheduling derived datasets after parent dataset succeeds
//  - cancelling queued/scheduled flow (at flow level, not at task level)
//  - flow config paused/resumed/modified when already queued/scheduled

/////////////////////////////////////////////////////////////////////////////////////////

struct FlowTestCheck<'a> {
    snapshot: &'a HashMap<FlowKey, FlowState>,
    patterns: Vec<(&'a FlowKey, FlowStatus, Option<FlowOutcome>)>,
}

fn assert_flow_test_checks<'a>(flow_test_checks: &[FlowTestCheck<'a>]) {
    for test_check in flow_test_checks {
        assert_eq!(test_check.snapshot.len(), test_check.patterns.len());
        for pattern in test_check.patterns.iter() {
            let flow_state = test_check.snapshot.get(pattern.0).unwrap();
            assert_eq!(flow_state.status(), pattern.1);
            assert_eq!(flow_state.outcome, pattern.2);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TaskTestCheck<'a> {
    task_id: TaskID,
    flow_key: &'a FlowKey,
    dataset_id: &'a DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

fn expect_running_dataset_update_task(
    task_state: &TaskState,
    dataset_id: &DatasetID,
    expected_task_id: TaskID,
) {
    assert_matches!(
        task_state,
        TaskState {
            task_id,
            status: TaskStatus::Running,
            logical_plan: LogicalPlan::UpdateDataset(UpdateDataset { dataset_id: actual_dataset_id }),
            cancellation_requested: false,
            created_at: _,
            ran_at: _,
            cancellation_requested_at: _,
            finished_at: _,
        } if actual_dataset_id == dataset_id && *task_id == expected_task_id
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TestFlowSystemListener {
    flow_service: Arc<dyn FlowService>,
    state: Arc<Mutex<TestFlowSystemListenerState>>,
}

#[derive(Default)]
struct TestFlowSystemListenerState {
    snapshots: Vec<(DateTime<Utc>, HashMap<FlowKey, FlowState>)>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn AsyncEventHandler<FlowServiceEventConfigurationLoaded>)]
#[interface(dyn AsyncEventHandler<FlowServiceEventExecutedTimeSlot>)]
impl TestFlowSystemListener {
    fn new(flow_service: Arc<dyn FlowService>) -> Self {
        Self {
            flow_service,
            state: Arc::new(Mutex::new(TestFlowSystemListenerState::default())),
        }
    }

    async fn snapshot_flows(&self, event_time: DateTime<Utc>) {
        use futures::TryStreamExt;
        let flows: Vec<_> = self
            .flow_service
            .list_all_flows()
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let mut flow_states_map = HashMap::new();
        for flow in flows {
            flow_states_map.insert(flow.flow_key.clone(), flow);
        }

        let mut state = self.state.lock().unwrap();
        state.snapshots.push((event_time, flow_states_map));
    }
}

#[async_trait::async_trait]
impl AsyncEventHandler<FlowServiceEventConfigurationLoaded> for TestFlowSystemListener {
    async fn handle(
        &self,
        event: &FlowServiceEventConfigurationLoaded,
    ) -> Result<(), InternalError> {
        self.snapshot_flows(event.event_time).await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncEventHandler<FlowServiceEventExecutedTimeSlot> for TestFlowSystemListener {
    async fn handle(&self, event: &FlowServiceEventExecutedTimeSlot) -> Result<(), InternalError> {
        self.snapshot_flows(event.event_time).await;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

const SCHEDULING_ALIGNMENT_MS: i64 = 5;

/////////////////////////////////////////////////////////////////////////////////////////

struct FlowHarness {
    _tmp_dir: tempfile::TempDir,
    catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_service: Arc<dyn FlowService>,
    task_scheduler: Arc<dyn TaskScheduler>,
}

impl FlowHarness {
    fn new() -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = tmp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add_value(FlowServiceRunConfig::new(Duration::milliseconds(
                SCHEDULING_ALIGNMENT_MS,
            )))
            .add::<FlowServiceInMemory>()
            .add::<FlowEventStoreInMem>()
            .add::<FlowConfigurationServiceInMemory>()
            .add::<FlowConfigurationEventStoreInMem>()
            .add::<SystemTimeSourceDefault>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(CurrentAccountSubject::new_test())
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .add::<TaskSchedulerInMemory>()
            .add::<TaskSystemEventStoreInMemory>()
            .add::<TestFlowSystemListener>()
            .build();

        let flow_service = catalog.get_one::<dyn FlowService>().unwrap();
        let flow_configuration_service = catalog.get_one::<dyn FlowConfigurationService>().unwrap();
        let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
        let task_scheduler = catalog.get_one::<dyn TaskScheduler>().unwrap();

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_service,
            flow_configuration_service,
            dataset_repo,
            task_scheduler,
        }
    }

    async fn create_root_dataset(&self, dataset_name: &str) -> DatasetID {
        let result = self
            .dataset_repo
            .create_dataset_from_snapshot(
                None,
                MetadataFactory::dataset_snapshot()
                    .name(DatasetName::new_unchecked(dataset_name))
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap();

        result.dataset_handle.id
    }

    async fn delete_dataset(&self, dataset_id: &DatasetID) {
        // Eagerly push dependency graph initialization before deletes.
        // It's ignored, if requested 2nd time
        let dependency_graph_service = self
            .catalog
            .get_one::<dyn DependencyGraphService>()
            .unwrap();
        let dependency_graph_repository =
            DependencyGraphRepositoryInMemory::new(self.dataset_repo.clone());
        dependency_graph_service
            .eager_initialization(&dependency_graph_repository)
            .await
            .unwrap();

        // Do the actual deletion
        self.dataset_repo
            .delete_dataset(&(dataset_id.as_local_ref()))
            .await
            .unwrap();
    }

    async fn set_dataset_flow_schedule(
        &self,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        schedule: Schedule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::Schedule(schedule),
            )
            .await
            .unwrap();
    }

    async fn snapshot_all_current_task_ids(&self) -> Vec<TaskID> {
        let mut task_ids = Vec::new();
        while let Some(task_id) = self.task_scheduler.try_take().await.unwrap() {
            task_ids.push(task_id);
        }
        task_ids
    }

    async fn trigger_manual_flow(&self, trigger_time: DateTime<Utc>, flow_key: FlowKey) {
        self.flow_service
            .trigger_manual_flow(
                trigger_time,
                flow_key,
                FAKE_ACCOUNT_ID.to_string(),
                AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME),
            )
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
