// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
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
use tokio::task::yield_now;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_read_initial_config_and_queue_without_waiting() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 60ms
    let foo_id = harness.create_root_dataset("foo").await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(60).into(),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: start running at 10ms, finish at 20ms
                let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Task 1: start running at 90ms, finish at 100ms
                let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(90),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
                });
                let foo_task1_handle = foo_task1_driver.run();

                // Main simulation boundary - 120ms total
                //  - "foo" should immediately schedule "task 0", since "foo" has never run yet
                //  - "task 0" will take action and complete, this will enqueue the next flow
                //    run for "foo" after full scheduling period
                //  - when that period is over, "task 1" should be scheduled
                //  - "task 1" will take action and complete, enqueing another flow
                let sim_handle = harness.advance_time(Duration::milliseconds(120));
                tokio::join!(foo_task0_handle, foo_task1_handle, sim_handle)
            } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" Ingest:
                Flow ID = 0 Queued

            #1: +0ms:
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +10ms:
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +20ms:
              "foo" Ingest:
                Flow ID = 1 Queued
                Flow ID = 0 Finished Success

            #4: +80ms:
              "foo" Ingest:
                Flow ID = 1 Scheduled
                Flow ID = 0 Finished Success

            #5: +90ms:
              "foo" Ingest:
                Flow ID = 1 Running
                Flow ID = 0 Finished Success

            #6: +100ms:
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cron_config() {
    // Note: this test runs with 1s step, CRON does not apply to milliseconds
    let harness =
        FlowHarness::new_custom_time_properties(Duration::seconds(1), Duration::seconds(1));

    // Create a "foo" root dataset, and configure ingestion cron schedule of every
    // 5s
    let foo_id = harness.create_root_dataset("foo").await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Schedule::Cron(ScheduleCron {
                source_5component_cron_expression: String::from("<irrelevant>"),
                cron_schedule: cron::Schedule::from_str("*/5 * * * * *").unwrap(),
            }),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::seconds(1))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: start running at 6s, finish at 7s
                let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::seconds(6),
                    finish_in_with: Some((Duration::seconds(1), TaskOutcome::Success)),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Main simulation boundary - 12s total: at 10s 2nd scheduling happens
                let sim_handle = harness.advance_time_custom_alignment(Duration::seconds(1), Duration::seconds(12));
                tokio::join!(foo_task0_handle, sim_handle)
            } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" Ingest:
                Flow ID = 0 Queued

            #1: +5000ms:
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +6000ms:
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +7000ms:
              "foo" Ingest:
                Flow ID = 1 Queued
                Flow ID = 0 Finished Success

            #4: +10000ms:
              "foo" Ingest:
                Flow ID = 1 Scheduled
                Flow ID = 0 Finished Success

            "#
        )
    );
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
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(90).into(),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();
    let bar_flow_key: FlowKey = FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::Ingest).into();

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: "foo" start running at 10ms, finish at 20ms
                let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
                });
                let task0_handle = task0_driver.run();

                // Task 1: "for" start running at 60ms, finish at 70ms
                let task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(60),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
                });
                let task1_handle = task1_driver.run();

                // Task 2: "bar" start running at 100ms, finish at 110ms
                let task2_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(2),
                    dataset_id: Some(bar_id.clone()),
                    run_since_start: Duration::milliseconds(100),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
                });
                let task2_handle = task2_driver.run();

                // Manual trigger for "foo" at 40ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_key: foo_flow_key,
                    run_since_start: Duration::milliseconds(40),
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 80ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_key: bar_flow_key,
                    run_since_start: Duration::milliseconds(80),
                });
                let trigger1_handle = trigger1_driver.run();

                // Main simulation script
                let main_handle = async {
                    // "foo":
                    //  - flow 0 => task 0 gets scheduled immediately at 0ms
                    //  - flow 0 => task 0 starts at 10ms and finishes running at 20ms
                    //  - next flow => enqueued at 20ms to trigger in 1 period of 90ms - at 110ms
                    // "bar": silent

                    // Moment 40ms - manual foo trigger happens here:
                    //  - flow 1 gets a 2nd trigger and is rescheduled to 40ms (20ms finish + 20ms throttling <= 40ms now)
                    //  - task 1 starts at 60ms, finishes at 70ms (leave some gap to fight with random order)
                    //  - flow 3 queued for 70 + 90 = 160ms
                    //  - "bar": still silent

                    // Moment 80ms - manual bar trigger happens here:
                    //  - flow 2 immediately scheduled
                    //  - task 2 gets scheduled at 80ms
                    //  - task 2 starts at 100ms and finishes at 110ms (ensure gap to fight against task execution order)
                    //  - no next flow enqueued

                    // Stop at 180ms: "foo" flow 3 gets scheduled at 160ms
                    harness.advance_time(Duration::milliseconds(180)).await;
                };

                tokio::join!(task0_handle, task1_handle, task2_handle, trigger0_handle, trigger1_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" Ingest:
                Flow ID = 0 Queued

            #1: +0ms:
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +10ms:
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +20ms:
              "foo" Ingest:
                Flow ID = 1 Queued
                Flow ID = 0 Finished Success

            #4: +40ms:
              "foo" Ingest:
                Flow ID = 1 Scheduled
                Flow ID = 0 Finished Success

            #5: +60ms:
              "foo" Ingest:
                Flow ID = 1 Running
                Flow ID = 0 Finished Success

            #6: +70ms:
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #7: +80ms:
              "bar" Ingest:
                Flow ID = 3 Scheduled
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #8: +100ms:
              "bar" Ingest:
                Flow ID = 3 Running
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #9: +110ms:
              "bar" Ingest:
                Flow ID = 3 Finished Success
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #10: +160ms:
              "bar" Ingest:
                Flow ID = 3 Finished Success
              "foo" Ingest:
                Flow ID = 2 Scheduled
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_configuration_paused_resumed_modified() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id: DatasetID = harness.create_root_dataset("bar").await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(50).into(),
        )
        .await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(80).into(),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task1_handle = task1_driver.run();

            // Main simulation script
            let main_handle = async {
                // Initially both "foo" and "bar" are scheduled without waiting.
                // "bar":
                //  - flow 0: task 1 starts at 20ms, finishes at 30sms
                //  - next flow 2 queued for 110ms (30+80)
                // "foo":
                //  - flow 1: task 0 starts at 10ms, finishes at 20ms
                //  - next flow 3 queued for 70ms (20+50)

                // 50ms: Pause both flow configs in between completion 2 first tasks and queing
                harness.advance_time(Duration::milliseconds(50)).await;
                harness.pause_dataset_flow(start_time + Duration::milliseconds(50), foo_id.clone(), DatasetFlowType::Ingest).await;
                harness.pause_dataset_flow(start_time + Duration::milliseconds(50), bar_id.clone(), DatasetFlowType::Ingest).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::milliseconds(50))
                    .await;

                // 80ms: Wake up after initially planned "foo" scheduling but before planned "bar" scheduling:
                //  - "foo":
                //    - gets resumed with previous period of 50ms
                //    - gets scheduled immediately at 80ms (waited >= 1 period)
                //  - "bar":
                //    - gets a config update for period of 70ms
                //    - get queued for 100ms (last success at 30ms + period of 70ms)
                harness.advance_time(Duration::milliseconds(30)).await;
                harness.resume_dataset_flow(start_time + Duration::milliseconds(80), foo_id.clone(), DatasetFlowType::Ingest).await;
                harness.set_dataset_flow_schedule(start_time + Duration::milliseconds(80), bar_id.clone(), DatasetFlowType::Ingest, Duration::milliseconds(70).into()).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::milliseconds(80))
                    .await;

                // 120ms: finish
                harness.advance_time(Duration::milliseconds(40)).await;
            };

            tokio::join!(task0_handle, task1_handle, main_handle)

         } => Ok(()),
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" Ingest:
                Flow ID = 1 Queued
              "foo" Ingest:
                Flow ID = 0 Queued

            #1: +0ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +10ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +20ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" Ingest:
                Flow ID = 3 Queued
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #6: +50ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #7: +80ms:
              "bar" Ingest:
                Flow ID = 5 Queued
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Queued
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +80ms:
              "bar" Ingest:
                Flow ID = 5 Queued
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Scheduled
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #9: +100ms:
              "bar" Ingest:
                Flow ID = 5 Scheduled
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Scheduled
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(50).into(),
        )
        .await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(70).into(),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task1_handle = task1_driver.run();

            let main_handle = async {
                // 0ms: Both "foo" and "bar" are initially scheduled without waiting
                //  "foo":
                //   - flow 0 scheduled at 0ms
                //   - task 0 starts at 10ms, finishes at 20ms
                //   - flow 2 enqueued for 20ms + period = 70ms
                //  "bar":
                //   - flow 1 scheduled at 0ms
                //   - task 1 starts at 20ms, finishes at 30ms
                //   - flow 3 enqueued for 30ms + period = 100ms

                // 50ms: deleting "foo" in QUEUED state
                harness.advance_time(Duration::milliseconds(50)).await;
                harness.delete_dataset(&foo_id).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::milliseconds(50))
                    .await;

                // 120ms: deleting "bar" in SCHEDULED state
                harness.advance_time(Duration::milliseconds(70)).await;
                harness.delete_dataset(&bar_id).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::milliseconds(120))
                    .await;

                // 140ms: finish
                harness.advance_time(Duration::milliseconds(20)).await;
            };

            tokio::join!(task0_handle, task1_handle, main_handle)
         } => Ok(()),
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" Ingest:
                Flow ID = 1 Queued
              "foo" Ingest:
                Flow ID = 0 Queued

            #1: +0ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +10ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +20ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" Ingest:
                Flow ID = 3 Queued
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #6: +50ms:
              "bar" Ingest:
                Flow ID = 3 Queued
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #7: +100ms:
              "bar" Ingest:
                Flow ID = 3 Scheduled
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +120ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_completions_trigger_next_loop_on_success() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;
    let baz_id = harness.create_root_dataset("baz").await;

    for dataset_id in [&foo_id, &bar_id, &baz_id] {
        harness
            .set_dataset_flow_schedule(
                harness.now_datetime(),
                dataset_id.clone(),
                DatasetFlowType::Ingest,
                Duration::milliseconds(40).into(),
            )
            .await;
    }

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());
    test_flow_listener.define_dataset_display_name(baz_id.clone(), "baz".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms with failure
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Failed)),
            });
            let task1_handle = task1_driver.run();

            // Task 1: "baz" start running at 30ms, finish at 40ms with cancellation
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Cancelled)),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                // 0ms: all 3 datasets are scheduled immediately without waiting:
                //  "foo":
                //   - flow 0 scheduled at 0ms
                //   - task 0 starts at 10ms, finishes at 20ms
                //   - next flow 3 enqueued for 20ms + period = 60ms
                //  "bar":
                //   - flow 1 scheduled at 0ms
                //   - task 1 starts at 20ms, finishes at 30ms with failure
                //   - next flow not enqueued
                //  "baz":
                //   - flow 2 scheduled at 0ms
                //   - task 2 starts at 30ms, finishes at 40ms with cancellation
                //   - next flow not enqueued

                // 80ms: the succeeded dataset schedule another update
                harness.advance_time(Duration::milliseconds(80)).await;
            };

            tokio::join!(task0_handle, task1_handle, task2_handle, main_handle)

         } => Ok(()),
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" Ingest:
                Flow ID = 1 Queued
              "baz" Ingest:
                Flow ID = 2 Queued
              "foo" Ingest:
                Flow ID = 0 Queued

            #1: +0ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "baz" Ingest:
                Flow ID = 2 Scheduled
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +10ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "baz" Ingest:
                Flow ID = 2 Scheduled
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +20ms:
              "bar" Ingest:
                Flow ID = 1 Scheduled
              "baz" Ingest:
                Flow ID = 2 Scheduled
              "foo" Ingest:
                Flow ID = 3 Queued
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running
              "baz" Ingest:
                Flow ID = 2 Scheduled
              "foo" Ingest:
                Flow ID = 3 Queued
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Scheduled
              "foo" Ingest:
                Flow ID = 3 Queued
                Flow ID = 0 Finished Success

            #6: +30ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Running
              "foo" Ingest:
                Flow ID = 3 Queued
                Flow ID = 0 Finished Success

            #7: +40ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Finished Cancelled
              "foo" Ingest:
                Flow ID = 3 Queued
                Flow ID = 0 Finished Success

            #8: +60ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Finished Cancelled
              "foo" Ingest:
                Flow ID = 3 Scheduled
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_derived_dataset_triggered_initially_and_after_input_change() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness
        .create_derived_dataset("bar", vec![foo_id.clone()])
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(80).into(),
        )
        .await;

    harness
        .set_dataset_flow_start_condition(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::ExecuteTransform,
            StartConditionConfiguration {
                throttling_period: None,
                minimal_data_batch: None,
            },
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<TestFlowSystemListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 110ms, finish at 120ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 130ms, finish at 140ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(130),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success)),
            });
            let task3_handle = task3_driver.run();


            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(220)).await;
            };

            tokio::join!(task0_handle, task1_handle, task2_handle, task3_handle, main_handle)

        } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Queued
              "foo" Ingest:
                Flow ID = 0 Queued

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Scheduled
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Queued
                Flow ID = 0 Finished Success

            #6: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Scheduled
                Flow ID = 0 Finished Success

            #7: +110ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Running
                Flow ID = 0 Finished Success

            #8: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Queued
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Queued
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #9: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Scheduled
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Queued
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #10: +130ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Running
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Queued
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #11: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Queued
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #12: +200ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Scheduled
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO next:
//  - throttling
//  - succesfull run, pause, resume, scheduling window adjustment (short, long)
//  - derived more than 1 level
//  - cancelling queued/scheduled flow (at flow level, not at task level)

/////////////////////////////////////////////////////////////////////////////////////////

struct TestFlowSystemListener {
    flow_service: Arc<dyn FlowService>,
    fake_time_source: Arc<FakeSystemTimeSource>,
    state: Arc<Mutex<TestFlowSystemListenerState>>,
}

type FlowSnapshot = (DateTime<Utc>, HashMap<FlowKey, Vec<FlowState>>);

#[derive(Default)]
struct TestFlowSystemListenerState {
    snapshots: Vec<FlowSnapshot>,
    dataset_display_names: HashMap<DatasetID, String>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn AsyncEventHandler<FlowServiceEvent>)]
impl TestFlowSystemListener {
    fn new(
        flow_service: Arc<dyn FlowService>,
        fake_time_source: Arc<FakeSystemTimeSource>,
    ) -> Self {
        Self {
            flow_service,
            fake_time_source,
            state: Arc::new(Mutex::new(TestFlowSystemListenerState::default())),
        }
    }

    async fn make_a_snapshot(&self, event_time: DateTime<Utc>) {
        use futures::TryStreamExt;
        let flows: Vec<_> = self
            .flow_service
            .list_all_flows(FlowPaginationOpts {
                limit: 100,
                offset: 0,
            })
            .await
            .unwrap()
            .matched_stream
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

    fn define_dataset_display_name(&self, id: DatasetID, display_name: String) {
        let mut state = self.state.lock().unwrap();
        state.dataset_display_names.insert(id, display_name);
    }
}

impl std::fmt::Display for TestFlowSystemListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let initial_time = self.fake_time_source.initial_time;

        let state = self.state.lock().unwrap();
        for i in 0..state.snapshots.len() {
            let (snapshot_time, snapshots) = state.snapshots.get(i).unwrap();
            writeln!(
                f,
                "#{i}: +{}ms:",
                (*snapshot_time - initial_time).num_milliseconds(),
            )?;

            let mut flow_headings = snapshots
                .keys()
                .map(|flow_key| {
                    (
                        flow_key,
                        match flow_key {
                            FlowKey::Dataset(fk_dataset) => format!(
                                "\"{}\" {:?}",
                                state
                                    .dataset_display_names
                                    .get(&fk_dataset.dataset_id)
                                    .cloned()
                                    .unwrap_or_else(|| fk_dataset.dataset_id.to_string()),
                                fk_dataset.flow_type
                            ),
                            FlowKey::System(fk_system) => {
                                format!("System {:?}", fk_system.flow_type)
                            }
                        },
                    )
                })
                .collect::<Vec<_>>();
            flow_headings.sort_by_key(|(_, title)| title.clone());

            for (flow_key, heading) in flow_headings {
                writeln!(f, "  {heading}:")?;
                for state in snapshots.get(flow_key).unwrap() {
                    write!(f, "    Flow ID = {} {:?}", state.flow_id, state.status(),)?;
                    if let Some(outcome) = state.outcome {
                        writeln!(f, " {outcome:?}",)?;
                    } else {
                        writeln!(f)?;
                    }
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncEventHandler<FlowServiceEvent> for TestFlowSystemListener {
    async fn handle(&self, event: &FlowServiceEvent) -> Result<(), InternalError> {
        self.make_a_snapshot(event.event_time()).await;
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

const SCHEDULING_ALIGNMENT_MS: i64 = 10;
const SCHEDULING_MANDATORY_THROTTLING_PERIOD_MS: i64 = SCHEDULING_ALIGNMENT_MS * 2;

/////////////////////////////////////////////////////////////////////////////////////////

struct FlowHarness {
    _tmp_dir: tempfile::TempDir,
    catalog: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_service: Arc<dyn FlowService>,
    fake_system_time_source: FakeSystemTimeSource,
}

impl FlowHarness {
    fn new() -> Self {
        Self::new_custom_time_properties(
            Duration::milliseconds(SCHEDULING_ALIGNMENT_MS),
            Duration::milliseconds(SCHEDULING_MANDATORY_THROTTLING_PERIOD_MS),
        )
    }

    fn new_custom_time_properties(
        awaiting_step: Duration,
        mandatory_throttling_period: Duration,
    ) -> Self {
        let tmp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = tmp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let t = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        let fake_system_time_source = FakeSystemTimeSource::new(t);

        let catalog = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add_value(FlowServiceRunConfig::new(
                awaiting_step,
                mandatory_throttling_period,
            ))
            .add::<FlowServiceInMemory>()
            .add::<FlowEventStoreInMem>()
            .add::<FlowConfigurationServiceInMemory>()
            .add::<FlowConfigurationEventStoreInMem>()
            .add_value(fake_system_time_source.clone())
            .bind::<dyn SystemTimeSource, FakeSystemTimeSource>()
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

        Self {
            _tmp_dir: tmp_dir,
            catalog,
            flow_service,
            flow_configuration_service,
            dataset_repo,
            fake_system_time_source,
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

    fn task_driver(&self, args: TaskDriverArgs) -> TaskDriver {
        TaskDriver::new(
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            args,
        )
    }

    fn manual_flow_trigger_driver(&self, args: ManualFlowTriggerArgs) -> ManualFlowTriggerDriver {
        ManualFlowTriggerDriver::new(
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            args,
        )
    }

    fn now_datetime(&self) -> DateTime<Utc> {
        self.fake_system_time_source.now()
    }

    async fn advance_time(&self, time_quantum: Duration) {
        self.advance_time_custom_alignment(
            Duration::milliseconds(SCHEDULING_ALIGNMENT_MS),
            time_quantum,
        )
        .await;
    }

    async fn advance_time_custom_alignment(&self, alignment: Duration, time_quantum: Duration) {
        // Examples:
        // 12 ÷ 4 = 3
        // 15 ÷ 4 = 4
        // 16 ÷ 4 = 4
        fn div_up(a: i64, b: i64) -> i64 {
            (a + (b - 1)) / b
        }

        let time_increments_count = div_up(
            time_quantum.num_milliseconds(),
            alignment.num_milliseconds(),
        );

        for _ in 0..time_increments_count {
            yield_now().await;
            self.fake_system_time_source.advance(alignment);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct TaskDriver {
    time_source: Arc<dyn SystemTimeSource>,
    event_bus: Arc<EventBus>,
    task_event_store: Arc<dyn TaskSystemEventStore>,
    args: TaskDriverArgs,
}

struct TaskDriverArgs {
    task_id: TaskID,
    dataset_id: Option<DatasetID>,
    run_since_start: Duration,
    finish_in_with: Option<(Duration, TaskOutcome)>,
}

impl TaskDriver {
    fn new(
        time_source: Arc<dyn SystemTimeSource>,
        event_bus: Arc<EventBus>,
        task_event_store: Arc<dyn TaskSystemEventStore>,
        args: TaskDriverArgs,
    ) -> Self {
        Self {
            time_source,
            event_bus,
            task_event_store,
            args,
        }
    }

    async fn run(self) {
        let start_time = self.time_source.now();

        self.time_source.sleep(self.args.run_since_start).await;
        while !(self.task_exists().await) {
            yield_now().await;
        }

        self.ensure_task_matches_dataset().await;

        self.event_bus
            .dispatch_event(TaskEventRunning {
                event_time: start_time + self.args.run_since_start,
                task_id: self.args.task_id,
            })
            .await
            .unwrap();

        if let Some((finish_in, with_outcome)) = self.args.finish_in_with {
            self.time_source.sleep(finish_in).await;

            self.event_bus
                .dispatch_event(TaskEventFinished {
                    event_time: start_time + self.args.run_since_start + finish_in,
                    task_id: self.args.task_id,
                    outcome: with_outcome,
                })
                .await
                .unwrap();
        }
    }

    async fn task_exists(&self) -> bool {
        Task::try_load(self.args.task_id, self.task_event_store.as_ref())
            .await
            .unwrap()
            .is_some()
    }

    async fn ensure_task_matches_dataset(&self) {
        let task = Task::load(self.args.task_id, self.task_event_store.as_ref())
            .await
            .expect("Task does not exist yet");

        match &task.logical_plan {
            LogicalPlan::UpdateDataset(ud) => {
                assert!(self.args.dataset_id.is_some());
                assert_eq!(&ud.dataset_id, self.args.dataset_id.as_ref().unwrap());
            }
            LogicalPlan::Probe(_) => assert!(self.args.dataset_id.is_none()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct ManualFlowTriggerDriver {
    time_source: Arc<dyn SystemTimeSource>,
    flow_service: Arc<dyn FlowService>,
    args: ManualFlowTriggerArgs,
}

struct ManualFlowTriggerArgs {
    flow_key: FlowKey,
    run_since_start: Duration,
}

impl ManualFlowTriggerDriver {
    fn new(
        time_source: Arc<dyn SystemTimeSource>,
        flow_service: Arc<dyn FlowService>,
        args: ManualFlowTriggerArgs,
    ) -> Self {
        Self {
            time_source,
            flow_service,
            args,
        }
    }

    async fn run(self) {
        let start_time = self.time_source.now();

        self.time_source.sleep(self.args.run_since_start).await;

        self.flow_service
            .trigger_manual_flow(
                start_time + self.args.run_since_start,
                self.args.flow_key,
                FAKE_ACCOUNT_ID.to_string(),
                AccountName::new_unchecked(auth::DEFAULT_ACCOUNT_NAME),
            )
            .await
            .unwrap();
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
