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

use chrono::{DateTime, Duration, Utc};
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
async fn test_read_initial_config() {
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

    let _ = tokio::select! {
        res = harness.flow_service.run() => res.int_err(),
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

    assert!(start_moment < foo_moment && foo_moment < bar_moment);
    assert_eq!((foo_moment - start_moment), Duration::milliseconds(30));
    assert_eq!((bar_moment - start_moment), Duration::milliseconds(45));

    let flow_test_checks = [
        // Snapshot 0: after initial queueing
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued),
                (&bar_flow_key, FlowStatus::Queued),
            ],
        },
        // Snapshot 1: period passed for 'foo', but not yet for 'bar'
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled),
                (&bar_flow_key, FlowStatus::Queued),
            ],
        },
        // Snapshot 2: period passed for 'foo' and for 'bar'
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled),
                (&bar_flow_key, FlowStatus::Scheduled),
            ],
        },
    ];

    for test_check in flow_test_checks {
        assert_eq!(test_check.snapshot.len(), test_check.patterns.len());
        for pattern in test_check.patterns {
            let flow_state = test_check.snapshot.get(pattern.0).unwrap();
            assert_eq!(flow_state.status(), pattern.1);
        }
    }

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

struct FlowTestCheck<'a> {
    snapshot: &'a HashMap<FlowKey, FlowState>,
    patterns: Vec<(&'a FlowKey, FlowStatus)>,
}

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
            .add_value(FlowServiceRunConfig::new(Duration::milliseconds(5)))
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
}

/////////////////////////////////////////////////////////////////////////////////////////
