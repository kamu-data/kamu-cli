// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Duration, DurationRound, TimeZone, Utc};
use dill::*;
use event_bus::EventBus;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_task_system::*;
use kamu_task_system_inmem::{TaskSchedulerInMemory, TaskSystemEventStoreInMemory};
use opendatafabric::*;
use tokio::task::yield_now;

use super::{
    FlowSystemTestListener,
    ManualFlowTriggerArgs,
    ManualFlowTriggerDriver,
    TaskDriver,
    TaskDriverArgs,
};

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
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Task 1: start running at 90ms, finish at 100ms
                let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(90),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
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

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
                    finish_in_with: Some((Duration::seconds(1), TaskOutcome::Success(TaskResult::Empty))),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Main simulation boundary - 12s total: at 10s 2nd scheduling happens
                let sim_handle = harness.advance_time_custom_alignment(Duration::seconds(1), Duration::seconds(12));
                tokio::join!(foo_task0_handle, sim_handle)
            } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
                });
                let task0_handle = task0_driver.run();

                // Task 1: "for" start running at 60ms, finish at 70ms
                let task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(60),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
                });
                let task1_handle = task1_driver.run();

                // Task 2: "bar" start running at 100ms, finish at 110ms
                let task2_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(2),
                    dataset_id: Some(bar_id.clone()),
                    run_since_start: Duration::milliseconds(100),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
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

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
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
async fn test_respect_last_success_time_when_schedule_resumes() {
    let harness = FlowHarness::new();

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(100).into(),
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::milliseconds(60).into(),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
              finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
          });
          let task0_handle = task0_driver.run();

          // Task 1: "bar" start running at 20ms, finish at 30ms
          let task1_driver = harness.task_driver(TaskDriverArgs {
              task_id: TaskID::new(1),
              dataset_id: Some(bar_id.clone()),
              run_since_start: Duration::milliseconds(20),
              finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
          });
          let task1_handle = task1_driver.run();

          // Main simulation script
          let main_handle = async {
              // Initially both "foo" isscheduled without waiting.
              // "foo":
              //  - flow 0: task 0 starts at 10ms, finishes at 20ms
              //  - next flow 2 queued for 120ms (20ms initiated + 100ms period)
              // "bar":
              //  - flow 1: task 1 starts at 20ms, finishes at 30ms
              //  - next flow 3 queued for 90ms (30ms initiated + 60ms period)

              // 50ms: Pause flow config before next flow runs
              harness.advance_time(Duration::milliseconds(50)).await;
              harness.pause_dataset_flow(start_time + Duration::milliseconds(50), foo_id.clone(), DatasetFlowType::Ingest).await;
              harness.pause_dataset_flow(start_time + Duration::milliseconds(50), bar_id.clone(), DatasetFlowType::Ingest).await;
              test_flow_listener
                  .make_a_snapshot(start_time + Duration::milliseconds(50))
                  .await;

              // 100ms: Wake up after initially planned "bar" scheduling but before planned "foo" scheduling:
              //  - "foo":
              //    - resumed with period 100ms
              //    - last success at 20ms
              //    - enqueued for 120ms (still wait a little bit since last success)
              //  - "bar":
              //    - resumed with period 60ms
              //    - last success at 30ms
              //    - gets scheduled immediately (waited longer than 30ms last success + 60ms period)
              harness.advance_time(Duration::milliseconds(50)).await;
              harness.resume_dataset_flow(start_time + Duration::milliseconds(100), foo_id.clone(), DatasetFlowType::Ingest).await;
              harness.resume_dataset_flow(start_time + Duration::milliseconds(100), bar_id.clone(), DatasetFlowType::Ingest).await;
              test_flow_listener
                  .make_a_snapshot(start_time + Duration::milliseconds(100))
                  .await;

              // 150ms: finish
              harness.advance_time(Duration::milliseconds(50)).await;
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

            #7: +100ms:
              "bar" Ingest:
                Flow ID = 5 Queued
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Queued
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +100ms:
              "bar" Ingest:
                Flow ID = 5 Scheduled
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Queued
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #9: +120ms:
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

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
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
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
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
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 110ms, finish at 120ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
            });
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 130ms, finish at 140ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(130),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
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

#[test_log::test(tokio::test)]
async fn test_throttling_manual_triggers() {
    let harness = FlowHarness::new_custom_time_properties(
        Duration::milliseconds(SCHEDULING_ALIGNMENT_MS), // 10ms
        Duration::milliseconds(SCHEDULING_ALIGNMENT_MS * 10), // 100ms
    );

    // Foo Flow
    let foo_id = harness.create_root_dataset("foo").await;
    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

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
        // Manual trigger for "foo" at 20ms
        let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
            flow_key: foo_flow_key.clone(),
            run_since_start: Duration::milliseconds(20),
        });
        let trigger0_handle = trigger0_driver.run();

        // Manual trigger for "foo" at 30ms
        let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
            flow_key: foo_flow_key.clone(),
            run_since_start: Duration::milliseconds(30),
        });
        let trigger1_handle = trigger1_driver.run();

        // Manual trigger for "foo" at 70ms
        let trigger2_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
          flow_key: foo_flow_key,
          run_since_start: Duration::milliseconds(70),
        });
        let trigger2_handle = trigger2_driver.run();

        // Task 0: "foo" start running at 40ms, finish at 50ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(0),
          dataset_id: Some(foo_id.clone()),
          run_since_start: Duration::milliseconds(40),
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
        });
        let task0_handle = task0_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::milliseconds(100)).await;
          test_flow_listener
              .make_a_snapshot(start_time + Duration::milliseconds(100))
              .await;
          harness.advance_time(Duration::milliseconds(70)).await;
        };

        tokio::join!(trigger0_handle, trigger1_handle, trigger2_handle, task0_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Ingest:
                Flow ID = 0 Scheduled

            #2: +40ms:
              "foo" Ingest:
                Flow ID = 0 Running

            #3: +50ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #4: +100ms:
              "foo" Ingest:
                Flow ID = 1 Queued
                Flow ID = 0 Finished Success

            #5: +150ms:
              "foo" Ingest:
                Flow ID = 1 Scheduled
                Flow ID = 0 Finished Success

          "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_throttling_derived_dataset_with_2_parents() {
    let harness = FlowHarness::new_custom_time_properties(
        Duration::milliseconds(SCHEDULING_ALIGNMENT_MS), // 10ms
        Duration::milliseconds(SCHEDULING_ALIGNMENT_MS * 10), // 100ms
    );

    let foo_id = harness.create_root_dataset("foo").await;
    let bar_id = harness.create_root_dataset("bar").await;
    let baz_id = harness
        .create_derived_dataset("baz", vec![foo_id.clone(), bar_id.clone()])
        .await;

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
            Duration::milliseconds(150).into(),
        )
        .await;

    harness
        .set_dataset_flow_start_condition(
            harness.now_datetime(),
            baz_id.clone(),
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
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
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
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(1),
          dataset_id: Some(bar_id.clone()),
          run_since_start: Duration::milliseconds(20),
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "baz" start running at 30ms, finish at 50ms (simulate longer run)
        let task2_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(2),
          dataset_id: Some(baz_id.clone()),
          run_since_start: Duration::milliseconds(30),
          finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Success(TaskResult::Empty))),
        });
        let task2_handle = task2_driver.run();

        // Task 3: "foo" start running at 130ms, finish at 140ms
        let task3_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(3),
          dataset_id: Some(foo_id.clone()),
          run_since_start: Duration::milliseconds(130),
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
        });
        let task3_handle = task3_driver.run();

        // Task 4: "baz" start running at 160ms, finish at 170ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(4),
          dataset_id: Some(baz_id.clone()),
          run_since_start: Duration::milliseconds(160),
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
        });
        let task4_handle = task4_driver.run();

        // Task 5: "bar" start running at 190ms, finish at 200ms
        let task5_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(5),
          dataset_id: Some(bar_id.clone()),
          run_since_start: Duration::milliseconds(190),
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::Empty))),
        });
        let task5_handle = task5_driver.run();

        // Main simulation script
        let main_handle = async {
          // Stage 0: initial auto-polling
          //  - all 3 datasets auto-polled at 0ms, flows 0,1,2 correspondongly
          //  - foo:
          //     - task 0 starts at 10ms, finishes at 20ms, flow 0 completes at 20ms
          //     - flow 3 queued for 120ns: 20ms initiated + max(period 50ms, throttling 100ms)
          //     - baz not queued as pending already, trigger recorded
          //  - bar:
          //     - task 1 starts at 20ms, finishes at 30ms, flow 1 completes at 30ms
          //     - flow 4 queued for 180ms: 30ms initiated + max(period 150ms, throttling 100ms)
          //     - baz not queued as pending already, trigger recorded
          //  - baz:
          //     - task 2 starts at 30ms, finishes at 50ms, flow 2 completes at 50ms
          //     - no continuation enqueued

          // Stage 1: foo runs next flow
          //  - foo:
          //     - flow 3 scheduled at 120ms
          //     - task 3 starts at 130ms, finishes at 140ms, flow 3 completes at 140ms
          //     - baz flow 5 enqueued as derived for 150ms: max(140ms initiated, last attempt 50ms + throttling 100ms)
          //     - foo flow 6 enqueued for 240ms: 140ms initiated + max(period 50ms, throttling 100ms)

          // Stage 2: baz executes triggered by foo
          //  - baz:
          //     - flow 5 scheduled at 150ms
          //     - task 4 starts at 160ms, finishes at 170ms, flow 5 completes at 170ms
          //     - no continuation enqueued

          // Stage 3: bar runs next flow
          // - bar
          //   - flow 4 schedules at 180ms
          //   - task 5 starts at 190ms, finishes at 200ms, flow 4 completes at 200ms
          //   - baz flow 7 enqueued as derived for 270ms: max(200ms initiated, last attempt 170ms + hrottling 100ms)
          //   - bar flow 8 enqueued for 350ms: 200ms initiated + max (period 150ms, throttling 100ms)

          harness.advance_time(Duration::milliseconds(400)).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task3_handle, task4_handle, task5_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
          #0: +0ms:
            "bar" Ingest:
              Flow ID = 1 Queued
            "baz" ExecuteTransform:
              Flow ID = 2 Queued
            "foo" Ingest:
              Flow ID = 0 Queued

          #1: +0ms:
            "bar" Ingest:
              Flow ID = 1 Scheduled
            "baz" ExecuteTransform:
              Flow ID = 2 Scheduled
            "foo" Ingest:
              Flow ID = 0 Scheduled

          #2: +10ms:
            "bar" Ingest:
              Flow ID = 1 Scheduled
            "baz" ExecuteTransform:
              Flow ID = 2 Scheduled
            "foo" Ingest:
              Flow ID = 0 Running

          #3: +20ms:
            "bar" Ingest:
              Flow ID = 1 Scheduled
            "baz" ExecuteTransform:
              Flow ID = 2 Scheduled
            "foo" Ingest:
              Flow ID = 3 Queued
              Flow ID = 0 Finished Success

          #4: +20ms:
            "bar" Ingest:
              Flow ID = 1 Running
            "baz" ExecuteTransform:
              Flow ID = 2 Scheduled
            "foo" Ingest:
              Flow ID = 3 Queued
              Flow ID = 0 Finished Success

          #5: +30ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Scheduled
            "foo" Ingest:
              Flow ID = 3 Queued
              Flow ID = 0 Finished Success

          #6: +30ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Running
            "foo" Ingest:
              Flow ID = 3 Queued
              Flow ID = 0 Finished Success

          #7: +50ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 3 Queued
              Flow ID = 0 Finished Success

          #8: +120ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 3 Scheduled
              Flow ID = 0 Finished Success

          #9: +130ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 3 Running
              Flow ID = 0 Finished Success

          #10: +140ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Queued
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Queued
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #11: +150ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Scheduled
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Queued
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #12: +160ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Running
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Queued
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #13: +170ms:
            "bar" Ingest:
              Flow ID = 4 Queued
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Queued
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #14: +180ms:
            "bar" Ingest:
              Flow ID = 4 Scheduled
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Queued
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #15: +190ms:
            "bar" Ingest:
              Flow ID = 4 Running
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Queued
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #16: +200ms:
            "bar" Ingest:
              Flow ID = 8 Queued
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Queued
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Queued
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #17: +240ms:
            "bar" Ingest:
              Flow ID = 8 Queued
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Queued
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Scheduled
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #18: +270ms:
            "bar" Ingest:
              Flow ID = 8 Queued
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Scheduled
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Scheduled
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #19: +350ms:
            "bar" Ingest:
              Flow ID = 8 Scheduled
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Scheduled
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Scheduled
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

        "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO next:
//  - derived more than 1 level
//  - cancelling queued/scheduled flow (at flow level, not at task level)

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
            .add::<FlowSystemTestListener>()
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
