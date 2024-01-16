// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
            Utc::now(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(60).into(),
        )
        .await;

    let bar_id = harness.create_root_dataset("bar").await;
    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(90).into(),
        )
        .await;

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = tokio::time::sleep(std::time::Duration::from_millis(120)) => Ok(()),
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
    assert_eq!(foo_moment - start_moment, Duration::milliseconds(60)); // planned time for "foo"
    assert_eq!(bar_moment - start_moment, Duration::milliseconds(90)); // planned time for "bar"

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
            Utc::now(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(60).into(),
        )
        .await;

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key: FlowKey = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // Sleep < "foo" period
            tokio::time::sleep(std::time::Duration::from_millis(40)).await;
            let new_time = start_time + Duration::milliseconds(40);
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
    assert_eq!(bar_moment - start_moment, Duration::milliseconds(40)); // next slot after 40ms trigger with 10ms align
    assert_eq!(foo_moment - start_moment, Duration::milliseconds(60)); // 60ms as planned

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
async fn test_dataset_flow_configuration_paused_resumed_modified() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id: DatasetID = harness.create_root_dataset("bar").await;
    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(50).into(),
        )
        .await;
    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(40).into(),
        )
        .await;

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key: FlowKey = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // Sleep < "foo"/"bar" period
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            harness.pause_dataset_flow(start_time + Duration::milliseconds(25), foo_id.clone(), DatasetFlowType::Ingest).await;
            harness.pause_dataset_flow(start_time + Duration::milliseconds(25), bar_id.clone(), DatasetFlowType::Ingest).await;
            test_flow_listener
                .snapshot_flows(start_time + Duration::milliseconds(25))
                .await;

            // Wake up after initially planned "bar" and "foo" scheduling
            tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            harness.resume_dataset_flow(start_time + Duration::milliseconds(55), foo_id.clone(), DatasetFlowType::Ingest).await;
            harness.set_dataset_flow_schedule(start_time + Duration::milliseconds(55), bar_id.clone(), DatasetFlowType::Ingest, Duration::milliseconds(30).into()).await;

            test_flow_listener
                .snapshot_flows(start_time + Duration::milliseconds(55))
                .await;

            // "foo" will get rescheduled in 50 ms, "bar" in 30ms, leave extra for stabilization
            tokio::time::sleep(std::time::Duration::from_millis(70)).await;
         } => Ok(()),
    }
    .unwrap();

    let state = test_flow_listener.state.lock().unwrap();
    assert_eq!(5, state.snapshots.len());

    let start_moment = state.snapshots[0].0;
    let pause_moment = state.snapshots[1].0;
    let resume_moment = state.snapshots[2].0;
    let bar_sch_moment = state.snapshots[3].0;
    let foo_sch_moment = state.snapshots[4].0;

    assert_eq!(start_moment, start_time);
    assert_eq!(pause_moment - start_moment, Duration::milliseconds(25));
    assert_eq!(resume_moment - start_moment, Duration::milliseconds(55));
    assert_eq!(bar_sch_moment - start_moment, Duration::milliseconds(90));
    assert_eq!(foo_sch_moment - start_moment, Duration::milliseconds(110));

    assert_flow_test_checks(&[
        // Snapshot 0: after initial queueing
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (&bar_flow_key, FlowStatus::Queued, None),
            ],
        },
        // Snapshot 1: "foo" paused, "bar" paused
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
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
        // Snapshot 2: "foo" resumed, "bar" resumed
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (&bar_flow_key, FlowStatus::Queued, None),
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
        // Snapshot 3: "bar" scheduled
        FlowTestCheck {
            snapshot: &state.snapshots[3].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (&bar_flow_key, FlowStatus::Scheduled, None),
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
        // Snapshot 4: "foo" scheduled
        FlowTestCheck {
            snapshot: &state.snapshots[4].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (&bar_flow_key, FlowStatus::Scheduled, None),
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

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(50).into(),
        )
        .await;
    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(70).into(),
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
    tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // Sleep < "foo" period
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            harness.delete_dataset(&foo_id).await;
            test_flow_listener
                .snapshot_flows(start_time + Duration::milliseconds(25))
                .await;

            // Wake up after bar scheduling
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            harness.delete_dataset(&bar_id).await;
            test_flow_listener
                .snapshot_flows(start_time + Duration::milliseconds(75))
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
    assert_eq!(foo_del_moment - start_moment, Duration::milliseconds(25));
    assert_eq!(bar_sch_moment - start_moment, Duration::milliseconds(70));
    assert_eq!(bar_del_moment - start_moment, Duration::milliseconds(75));

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

#[test_log::test(tokio::test)]
async fn test_cron_task_completions_trigger_next_loop_on_success() {
    let harness = FlowHarness::new();
    let foo_id = harness.create_root_dataset("foo").await;
    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(30).into(),
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(40).into(),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();

    // Obtain access to event bus
    let event_bus = harness.catalog.get_one::<EventBus>().unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // Each of 3 datasets should be scheduled after this time
            let mut next_time = start_time + Duration::milliseconds(1100);
            tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

            // Plan different task execution outcomes for each dataset
            let mut planned_outcomes = HashMap::new();
            planned_outcomes.insert(&foo_id, TaskOutcome::Success);

            // Determine which task state belongs to which dataset
            let scheduled_tasks = harness.take_scheduled_tasks().await;
            let mut scheduled_tasks_by_dataset_id = HashMap::new();
            for scheduled_task in scheduled_tasks {
                let task_dataset_id = match &scheduled_task.logical_plan {
                    LogicalPlan::UpdateDataset(lp) => lp.dataset_id.clone(),
                    _ => unreachable!()
                };
                scheduled_tasks_by_dataset_id.insert(task_dataset_id, scheduled_task);
            };

            // Send task running and finished events for each dataset with certain interval
            let dataset_task = scheduled_tasks_by_dataset_id.get(&foo_id).unwrap();
            event_bus.dispatch_event(TaskEventRunning {
                event_time: next_time,
                task_id: dataset_task.task_id,
            }).await.unwrap();
            event_bus.dispatch_event(TaskEventFinished {
                event_time: next_time,
                task_id: dataset_task.task_id,
                outcome: *(planned_outcomes.get(&foo_id).unwrap())
            }).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            next_time += Duration::milliseconds(10);

            // Let the succeeded dataset to schedule another update. 40s + 20s max waiting
            tokio::time::sleep(std::time::Duration::from_millis(70)).await;

         } => Ok(()),
    }
    .unwrap();

    let state = test_flow_listener.state.lock().unwrap();
    assert_eq!(4, state.snapshots.len());

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();

    assert_flow_test_checks(&[
        // Snapshot 0: after initial queueing
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![(&foo_flow_key, FlowStatus::Queued, None)],
        },
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
            patterns: vec![(&foo_flow_key, FlowStatus::Scheduled, None)],
        },
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
            ],
        },
        FlowTestCheck {
            snapshot: &state.snapshots[3].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
            ],
        },
    ]);
}

#[test_log::test(tokio::test)]
async fn test_task_completions_trigger_next_loop_on_success() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;
    let baz_id = harness.create_root_dataset("baz").await;

    for dataset_id in [&foo_id, &bar_id, &baz_id] {
        harness
            .set_dataset_flow_schedule(
                Utc::now(),
                dataset_id.clone(),
                DatasetFlowType::Ingest,
                Duration::milliseconds(40).into(),
            )
            .await;
    }

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();

    // Obtain access to event bus
    let event_bus = harness.catalog.get_one::<EventBus>().unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // Each of 3 datasets should be scheduled after this time
            let mut next_time = start_time + Duration::milliseconds(60);
            tokio::time::sleep(std::time::Duration::from_millis(60)).await;

            // Plan different task execution outcomes for each dataset
            let mut planned_outcomes = HashMap::new();
            planned_outcomes.insert(&foo_id, TaskOutcome::Success);
            planned_outcomes.insert(&bar_id, TaskOutcome::Failed);
            planned_outcomes.insert(&baz_id, TaskOutcome::Cancelled);

            // Determine which task state belongs to which dataset
            let scheduled_tasks = harness.take_scheduled_tasks().await;
            let mut scheduled_tasks_by_dataset_id = HashMap::new();
            for scheduled_task in scheduled_tasks {
                let task_dataset_id = match &scheduled_task.logical_plan {
                    LogicalPlan::UpdateDataset(lp) => lp.dataset_id.clone(),
                    _ => unreachable!()
                };
                scheduled_tasks_by_dataset_id.insert(task_dataset_id, scheduled_task);
            };

            // Send task running & finished events for each dataset with certain interval
            for dataset_id in [&foo_id, &bar_id, &baz_id] {
                let dataset_task = scheduled_tasks_by_dataset_id.get(dataset_id).unwrap();
                event_bus.dispatch_event(TaskEventRunning {
                    event_time: next_time,
                    task_id: dataset_task.task_id,
                }).await.unwrap();
                event_bus.dispatch_event(TaskEventFinished {
                    event_time: next_time,
                    task_id: dataset_task.task_id,
                    outcome: *(planned_outcomes.get(dataset_id).unwrap())
                }).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                next_time += Duration::milliseconds(10);
            }

            // Let the succeeded dataset to schedule another update. 40s + 20s max waiting
            tokio::time::sleep(std::time::Duration::from_millis(70)).await;

         } => Ok(()),
    }
    .unwrap();

    let state = test_flow_listener.state.lock().unwrap();
    assert_eq!(6, state.snapshots.len());

    let start_moment = state.snapshots[0].0;
    let schedule_moment = state.snapshots[1].0;
    let finish_foo_moment = state.snapshots[2].0;
    let finish_bar_moment = state.snapshots[3].0;
    let finish_baz_moment = state.snapshots[4].0;
    let reschedule_foo_moment = state.snapshots[5].0;

    assert_eq!(start_moment, start_time);
    assert_eq!(schedule_moment - start_moment, Duration::milliseconds(40));
    assert_eq!(finish_foo_moment - start_moment, Duration::milliseconds(60));
    assert_eq!(finish_bar_moment - start_moment, Duration::milliseconds(70));
    assert_eq!(finish_baz_moment - start_moment, Duration::milliseconds(80));
    assert_eq!(
        reschedule_foo_moment - start_moment,
        Duration::milliseconds(100)
    );

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key: FlowKey = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();
    let baz_flow_key: FlowKey = FlowKeyDataset::new(baz_id.clone(), DatasetFlowType::Ingest).into();

    assert_flow_test_checks(&[
        // Snapshot 0: after initial queueing
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (&bar_flow_key, FlowStatus::Queued, None),
                (&baz_flow_key, FlowStatus::Queued, None),
            ],
        },
        // Snapshot 1: all 3 are scheduled
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (&bar_flow_key, FlowStatus::Scheduled, None),
                (&baz_flow_key, FlowStatus::Scheduled, None),
            ],
        },
        // Snapshot 2: "foo" finished, enqueued for round 2
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
                (&bar_flow_key, FlowStatus::Scheduled, None),
                (&baz_flow_key, FlowStatus::Scheduled, None),
            ],
        },
        // Snapshot 3: "bar" finished with Fail
        FlowTestCheck {
            snapshot: &state.snapshots[3].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
                (
                    &bar_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Failed),
                ),
                (&baz_flow_key, FlowStatus::Scheduled, None),
            ],
        },
        // Snapshot 4: "baz" finished with Cancel
        FlowTestCheck {
            snapshot: &state.snapshots[4].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
                (
                    &bar_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Failed),
                ),
                (
                    &baz_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Cancelled),
                ),
            ],
        },
        // Snapshot 5: "foo" scheduled, round 2
        FlowTestCheck {
            snapshot: &state.snapshots[5].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
                (
                    &bar_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Failed),
                ),
                (
                    &baz_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Cancelled),
                ),
            ],
        },
    ]);
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_update_success_triggers_update_of_derived_datasets() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness
        .create_derived_dataset("bar", vec![foo_id.clone()])
        .await;
    let baz_id = harness
        .create_derived_dataset("baz", vec![foo_id.clone()])
        .await;

    harness
        .set_dataset_flow_schedule(
            Utc::now(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(30).into(),
        )
        .await;

    for dataset_id in [&bar_id, &baz_id] {
        harness
            .set_dataset_flow_start_condition(
                Utc::now(),
                dataset_id.clone(),
                DatasetFlowType::Ingest,
                StartConditionConfiguration {
                    throttling_period: None,
                    minimal_data_batch: None,
                },
            )
            .await;
    }

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Remember start time
    let start_time = Utc::now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();

    // Obtain access to event bus
    let event_bus = harness.catalog.get_one::<EventBus>().unwrap();

    // Run scheduler concurrently with manual triggers script
    let _ = tokio::select! {
        res = harness.flow_service.run(start_time) => res.int_err(),
        _ = async {
            // "foo" is definitely schedule now
            let next_time = start_time + Duration::milliseconds(50);
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

            // Extract dataset tasks
            let scheduled_tasks = harness.take_scheduled_tasks().await;
            assert_eq!(1, scheduled_tasks.len());
            let task_dataset_id = match &scheduled_tasks[0].logical_plan {
                LogicalPlan::UpdateDataset(lp) => lp.dataset_id.clone(),
                _ => unreachable!()
            };
            assert_eq!(task_dataset_id, foo_id);

            // Send running & finished for this task
            event_bus.dispatch_event(TaskEventRunning {
                event_time: next_time,
                task_id: scheduled_tasks[0].task_id,
            }).await.unwrap();
            event_bus.dispatch_event(TaskEventFinished {
                event_time: next_time,
                task_id: scheduled_tasks[0].task_id,
                outcome: TaskOutcome::Success,
            }).await.unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        } => Ok(())
    };

    let state = test_flow_listener.state.lock().unwrap();
    assert_eq!(5, state.snapshots.len());

    let start_moment = state.snapshots[0].0;
    let schedule_foo_moment = state.snapshots[1].0;
    let finish_foo_moment = state.snapshots[2].0;
    let schedule_bar_baz_moment = state.snapshots[3].0;
    let reschedule_foo_moment = state.snapshots[4].0;

    assert_eq!(start_moment, start_time);
    assert_eq!(
        schedule_foo_moment - start_moment,
        Duration::milliseconds(30)
    );
    assert_eq!(finish_foo_moment - start_moment, Duration::milliseconds(50));
    assert_eq!(
        schedule_bar_baz_moment - start_moment,
        Duration::milliseconds(50)
    );
    assert_eq!(
        reschedule_foo_moment - start_moment,
        Duration::milliseconds(80)
    );

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key: FlowKey = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();
    let baz_flow_key: FlowKey = FlowKeyDataset::new(baz_id.clone(), DatasetFlowType::Ingest).into();

    assert_flow_test_checks(&[
        // Snapshot 0: after initial queueing
        FlowTestCheck {
            snapshot: &state.snapshots[0].1,
            patterns: vec![(&foo_flow_key, FlowStatus::Queued, None)],
        },
        // Snapshot 1: "foo" scheduled
        FlowTestCheck {
            snapshot: &state.snapshots[1].1,
            patterns: vec![(&foo_flow_key, FlowStatus::Scheduled, None)],
        },
        // Snapshot 2: "foo" finished
        FlowTestCheck {
            snapshot: &state.snapshots[2].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
                (&bar_flow_key, FlowStatus::Queued, None),
                (&baz_flow_key, FlowStatus::Queued, None),
            ],
        },
        // Snapshot 3: "bar" & "baz" scheduled
        FlowTestCheck {
            snapshot: &state.snapshots[3].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Queued, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
                (&bar_flow_key, FlowStatus::Scheduled, None),
                (&baz_flow_key, FlowStatus::Scheduled, None),
            ],
        },
        // Snapshot 4: "foo" rescheduled
        FlowTestCheck {
            snapshot: &state.snapshots[4].1,
            patterns: vec![
                (&foo_flow_key, FlowStatus::Scheduled, None),
                (
                    &foo_flow_key,
                    FlowStatus::Finished,
                    Some(FlowOutcome::Success),
                ),
                (&bar_flow_key, FlowStatus::Scheduled, None),
                (&baz_flow_key, FlowStatus::Scheduled, None),
            ],
        },
    ]);
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO next:
//  - derived more than 1 level
//  - throttling derived
//  - cancelling queued/scheduled flow (at flow level, not at task level)

/////////////////////////////////////////////////////////////////////////////////////////

struct FlowTestCheck<'a> {
    snapshot: &'a HashMap<FlowKey, Vec<FlowState>>,
    patterns: Vec<(&'a FlowKey, FlowStatus, Option<FlowOutcome>)>,
}

fn assert_flow_test_checks(flow_test_checks: &[FlowTestCheck<'_>]) {
    for test_check in flow_test_checks {
        let mut pattern_idx_per_key = HashMap::new();

        let snapshot_total_flows: usize = test_check.snapshot.values().map(|v| v.len()).sum();
        assert_eq!(snapshot_total_flows, test_check.patterns.len());

        for pattern in test_check.patterns.iter() {
            let flow_states = test_check.snapshot.get(pattern.0).unwrap();

            let index = if let Some(index) = pattern_idx_per_key.get_mut(pattern.0) {
                *index += 1;
                *index
            } else {
                pattern_idx_per_key.insert(pattern.0, 0);
                0
            };

            let flow_state = flow_states.get(index).unwrap();

            assert_eq!(flow_state.status(), pattern.1);
            assert_eq!(flow_state.outcome, pattern.2);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TestFlowSystemListener {
    flow_service: Arc<dyn FlowService>,
    state: Arc<Mutex<TestFlowSystemListenerState>>,
}

#[derive(Default)]
struct TestFlowSystemListenerState {
    snapshots: Vec<(DateTime<Utc>, HashMap<FlowKey, Vec<FlowState>>)>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn AsyncEventHandler<FlowServiceEvent>)]
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

        let mut flow_states_map: HashMap<FlowKey, Vec<FlowState>> = HashMap::new();
        for flow in flows {
            flow_states_map
                .entry(flow.flow_key.clone())
                .and_modify(|flows| flows.push(flow.clone()))
                .or_insert(vec![flow]);
        }

        let mut state = self.state.lock().unwrap();
        state.snapshots.push((event_time, flow_states_map));
    }
}

#[async_trait::async_trait]
impl AsyncEventHandler<FlowServiceEvent> for TestFlowSystemListener {
    async fn handle(&self, event: &FlowServiceEvent) -> Result<(), InternalError> {
        self.snapshot_flows(event.event_time()).await;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

const SCHEDULING_ALIGNMENT_MS: i64 = 10;

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
                MetadataFactory::dataset_snapshot()
                    .name(dataset_name)
                    .kind(DatasetKind::Root)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap();

        result.dataset_handle.id
    }

    async fn create_derived_dataset(
        &self,
        dataset_name: &str,
        input_ids: Vec<DatasetID>,
    ) -> DatasetID {
        let create_result = self
            .dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_name)
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(input_ids)
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap();
        create_result.dataset_handle.id
    }

    async fn eager_dependencies_graph_init(&self) {
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
    }

    async fn delete_dataset(&self, dataset_id: &DatasetID) {
        // Eagerly push dependency graph initialization before deletes.
        // It's ignored, if requested 2nd time
        self.eager_dependencies_graph_init().await;

        // Do the actual deletion
        self.dataset_repo
            .delete_dataset(&(dataset_id.as_local_ref()))
            .await
            .unwrap();
    }

    async fn set_dataset_flow_schedule(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        schedule: Schedule,
    ) {
        self.flow_configuration_service
            .set_configuration(
                request_time,
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::Schedule(schedule),
            )
            .await
            .unwrap();
    }

    async fn set_dataset_flow_start_condition(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
        start_condition: StartConditionConfiguration,
    ) {
        self.flow_configuration_service
            .set_configuration(
                request_time,
                FlowKeyDataset::new(dataset_id, dataset_flow_type).into(),
                false,
                FlowConfigurationRule::StartCondition(start_condition),
            )
            .await
            .unwrap();
    }

    async fn pause_dataset_flow(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_config = self
            .flow_configuration_service
            .find_configuration(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_configuration_service
            .set_configuration(request_time, flow_key, true, current_config.rule)
            .await
            .unwrap();
    }

    async fn resume_dataset_flow(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) {
        let flow_key: FlowKey = FlowKeyDataset::new(dataset_id, dataset_flow_type).into();
        let current_config = self
            .flow_configuration_service
            .find_configuration(flow_key.clone())
            .await
            .unwrap()
            .unwrap();

        self.flow_configuration_service
            .set_configuration(request_time, flow_key, false, current_config.rule)
            .await
            .unwrap();
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

    async fn take_scheduled_tasks(&self) -> Vec<TaskState> {
        let mut task_states: Vec<_> = Vec::new();
        while let Some(task_id) = self.task_scheduler.try_take().await.unwrap() {
            let task_state = self.task_scheduler.get_task(task_id).await.unwrap();
            task_states.push(task_state);
        }
        task_states
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
