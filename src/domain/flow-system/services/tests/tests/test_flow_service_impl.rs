// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use chrono::{Duration, DurationRound, Utc};
use kamu::testing::MockDatasetChangesService;
use kamu_core::*;
use kamu_flow_system::*;
use kamu_task_system::*;
use opendatafabric::*;

use super::{
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    ManualFlowTriggerArgs,
    TaskDriverArgs,
    SCHEDULING_ALIGNMENT_MS,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_read_initial_config_and_queue_without_waiting() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 60ms
    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(60).unwrap().into(),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                    run_since_start: Duration::try_milliseconds(10).unwrap(),
                    finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                      dataset_id: foo_id.clone()
                    }),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Task 1: start running at 90ms, finish at 100ms
                let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::try_milliseconds(90).unwrap(),
                    finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                      dataset_id: foo_id.clone()
                    }),
                });
                let foo_task1_handle = foo_task1_driver.run();

                // Main simulation boundary - 120ms total
                //  - "foo" should immediately schedule "task 0", since "foo" has never run yet
                //  - "task 0" will take action and complete, this will enqueue the next flow
                //    run for "foo" after full scheduling period
                //  - when that period is over, "task 1" should be scheduled
                //  - "task 1" will take action and complete, enqueuing another flow
                let sim_handle = harness.advance_time(Duration::try_milliseconds(120).unwrap());
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
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #4: +80ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=80ms)
                Flow ID = 0 Finished Success

            #5: +90ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #6: +100ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
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
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::try_seconds(1).unwrap()),
        mandatory_throttling_period: Some(Duration::try_seconds(1).unwrap()),
        ..Default::default()
    });

    // Create a "foo" root dataset, and configure ingestion cron schedule of every
    // 5s
    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
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
        .duration_round(Duration::try_seconds(1).unwrap())
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
                    run_since_start: Duration::try_seconds(6).unwrap(),
                    finish_in_with: Some((Duration::try_seconds(1).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                      dataset_id: foo_id.clone()
                    }),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Main simulation boundary - 12s total: at 10s 2nd scheduling happens
                let sim_handle = harness.advance_time_custom_alignment(Duration::try_seconds(1).unwrap(), Duration::try_seconds(12).unwrap());
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
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=5000ms)

            #1: +5000ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=5000ms)

            #2: +6000ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +7000ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=10000ms)
                Flow ID = 0 Finished Success

            #4: +10000ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=10000ms)
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    // Note: only "foo" has auto-schedule, "bar" hasn't
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(90).unwrap().into(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                    run_since_start: Duration::try_milliseconds(10).unwrap(),
                    finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                      dataset_id: foo_id.clone()
                    }),
                });
                let task0_handle = task0_driver.run();

                // Task 1: "for" start running at 60ms, finish at 70ms
                let task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::try_milliseconds(60).unwrap(),
                    finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                      dataset_id: foo_id.clone()
                    }),
                });
                let task1_handle = task1_driver.run();

                // Task 2: "bar" start running at 100ms, finish at 110ms
                let task2_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(2),
                    dataset_id: Some(bar_id.clone()),
                    run_since_start: Duration::try_milliseconds(100).unwrap(),
                    finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                      dataset_id: bar_id.clone()
                    }),
                });
                let task2_handle = task2_driver.run();

                // Manual trigger for "foo" at 40ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_key: foo_flow_key,
                    run_since_start: Duration::try_milliseconds(40).unwrap(),
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 80ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_key: bar_flow_key,
                    run_since_start: Duration::try_milliseconds(80).unwrap(),
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
                    harness.advance_time(Duration::try_milliseconds(180).unwrap()).await;
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
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=110ms)
                Flow ID = 0 Finished Success

            #4: +40ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=40ms)
                Flow ID = 0 Finished Success

            #5: +60ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #6: +70ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #7: +80ms:
              "bar" Ingest:
                Flow ID = 3 Waiting Manual Executor(task=2, since=80ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #8: +100ms:
              "bar" Ingest:
                Flow ID = 3 Running(task=2)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #9: +110ms:
              "bar" Ingest:
                Flow ID = 3 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #10: +160ms:
              "bar" Ingest:
                Flow ID = 3 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=3, since=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_compacting() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    harness.eager_dependencies_graph_init().await;

    let foo_flow_key: FlowKey =
        FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::HardCompacting).into();
    let bar_flow_key: FlowKey =
        FlowKeyDataset::new(bar_id.clone(), DatasetFlowType::HardCompacting).into();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                    run_since_start: Duration::try_milliseconds(10).unwrap(),
                    finish_in_with: Some((Duration::try_milliseconds(20).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::HardCompactingDataset(HardCompactingDataset {
                      dataset_id: foo_id.clone(),
                      max_slice_size: None,
                      max_slice_records: None,
                      keep_metadata_only: false,
                    }),
                });
                let task0_handle = task0_driver.run();

                let task1_driver = harness.task_driver(TaskDriverArgs {
                  task_id: TaskID::new(1),
                  dataset_id: Some(bar_id.clone()),
                  run_since_start: Duration::try_milliseconds(60).unwrap(),
                  finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                  expected_logical_plan: LogicalPlan::HardCompactingDataset(HardCompactingDataset {
                    dataset_id: bar_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                    keep_metadata_only: false,
                  }),
                });
                let task1_handle = task1_driver.run();

                // Manual trigger for "foo" at 10ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_key: foo_flow_key,
                    run_since_start: Duration::try_milliseconds(10).unwrap(),
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 50ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_key: bar_flow_key,
                    run_since_start: Duration::try_milliseconds(50).unwrap(),
                });
                let trigger1_handle = trigger1_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Moment 10ms - manual foo trigger happens here:
                    //  - flow 0 gets trigger and finishes at 30ms

                    // Moment 50ms - manual foo trigger happens here:
                    //  - flow 1 trigger and finishes
                    //  - task 1 starts at 60ms, finishes at 70ms (leave some gap to fight with random order)

                    harness.advance_time(Duration::try_milliseconds(100).unwrap()).await;
                };

                tokio::join!(task0_handle, task1_handle, trigger0_handle, trigger1_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" HardCompacting:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #2: +10ms:
              "foo" HardCompacting:
                Flow ID = 0 Running(task=0)

            #3: +30ms:
              "bar" HardCompacting:
                Flow ID = 1 Waiting Manual
              "foo" HardCompacting:
                Flow ID = 0 Finished Success

            #4: +50ms:
              "bar" HardCompacting:
                Flow ID = 1 Waiting Manual Executor(task=1, since=50ms)
              "foo" HardCompacting:
                Flow ID = 0 Finished Success

            #5: +60ms:
              "bar" HardCompacting:
                Flow ID = 1 Running(task=1)
              "foo" HardCompacting:
                Flow ID = 0 Finished Success

            #6: +70ms:
              "bar" HardCompacting:
                Flow ID = 1 Finished Success
              "foo" HardCompacting:
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_compacting_with_config() {
    let max_slice_size = 1_000_000u64;
    let max_slice_records = 1000u64;
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness.eager_dependencies_graph_init().await;
    harness
        .set_dataset_flow_compacting_rule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::HardCompacting,
            CompactingRule::new_checked(max_slice_size, max_slice_records, false).unwrap(),
        )
        .await;

    let foo_flow_key: FlowKey =
        FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::HardCompacting).into();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: "foo" start running at 30ms, finish at 40ms
                let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::try_milliseconds(30).unwrap(),
                    finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                    expected_logical_plan: LogicalPlan::HardCompactingDataset(HardCompactingDataset {
                      dataset_id: foo_id.clone(),
                      max_slice_size: Some(max_slice_size),
                      max_slice_records: Some(max_slice_records),
                      keep_metadata_only: false,
                    }),
                });
                let task0_handle = task0_driver.run();

                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_key: foo_flow_key,
                    run_since_start: Duration::try_milliseconds(20).unwrap(),
                });
                let trigger0_handle = trigger0_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Moment 30ms - manual foo trigger happens here:
                    //  - flow 0 trigger and finishes at 40ms
                    harness.advance_time(Duration::try_milliseconds(80).unwrap()).await;
                };

                tokio::join!(task0_handle, trigger0_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" HardCompacting:
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #2: +30ms:
              "foo" HardCompacting:
                Flow ID = 0 Running(task=0)

            #3: +40ms:
              "foo" HardCompacting:
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_keep_metada_only_compacting() {
    let max_slice_size = 1_000_000u64;
    let max_slice_records = 1000u64;
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_bar_id = harness
        .create_derived_dataset(
            DatasetAlias {
                dataset_name: DatasetName::new_unchecked("foo.bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_compacting_rule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::HardCompacting,
            CompactingRule::new_checked(max_slice_size, max_slice_records, true).unwrap(),
        )
        .await;
    harness
        .set_dataset_flow_batching_rule(
            harness.now_datetime(),
            foo_bar_id.clone(),
            DatasetFlowType::ExecuteTransform,
            BatchingRule::new_checked(1, Duration::try_seconds(1).unwrap()).unwrap(),
        )
        .await;

    harness.eager_dependencies_graph_init().await;

    let foo_flow_key: FlowKey =
        FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::HardCompacting).into();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_Bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
        .unwrap();

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_service.run(start_time) => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo_bar" execute_transform start running at 10ms, finish at 30ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
              task_id: TaskID::new(0),
              dataset_id: Some(foo_bar_id.clone()),
              run_since_start: Duration::try_milliseconds(10).unwrap(),
              finish_in_with: Some((Duration::try_milliseconds(20).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
              expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                dataset_id: foo_bar_id.clone(),
                }),
            });
            let task0_handle = task0_driver.run();
            // Task 1: "foo" start running at 40ms, finish at 130ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::try_milliseconds(40).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(70).unwrap(), TaskOutcome::Success(TaskResult::CompactingDatasetResult(TaskCompactingDatasetResult {
                  compacting_result: CompactingResult::Success {
                    old_head: Multihash::from_digest_sha3_256(b"old-slice"),
                    new_head: Multihash::from_digest_sha3_256(b"new-slice"),
                    old_num_blocks: 5,
                    new_num_blocks: 4,
                }})))),
                expected_logical_plan: LogicalPlan::HardCompactingDataset(HardCompactingDataset {
                  dataset_id: foo_id.clone(),
                  max_slice_size: Some(max_slice_size),
                  max_slice_records: Some(max_slice_records),
                  keep_metadata_only: true,
                }),
            });
            let task1_handle = task1_driver.run();

            let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                flow_key: foo_flow_key,
                run_since_start: Duration::try_milliseconds(30).unwrap(),
            });
            let trigger1_handle = trigger1_driver.run();

              // Task 2: "foo_bar" execute_transform start running at 170ms, finish at 240ms
              let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                dataset_id: Some(foo_bar_id.clone()),
                run_since_start: Duration::try_milliseconds(170).unwrap(),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((Duration::try_milliseconds(70).unwrap(), TaskOutcome::Success(TaskResult::CompactingDatasetResult(TaskCompactingDatasetResult {
                  compacting_result: CompactingResult::Success {
                    old_head: Multihash::from_digest_sha3_256(b"old-slice"),
                    new_head: Multihash::from_digest_sha3_256(b"new-slice"),
                    old_num_blocks: 5,
                    new_num_blocks: 4,
                }})))),
                // Make sure we will take config from root dataset
                expected_logical_plan: LogicalPlan::HardCompactingDataset(HardCompactingDataset {
                  dataset_id: foo_bar_id.clone(),
                  max_slice_size: Some(max_slice_size),
                  max_slice_records: Some(max_slice_records),
                  keep_metadata_only: true,
                }),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::try_milliseconds(500).unwrap()).await;
            };

            tokio::join!(task0_handle, task1_handle, trigger1_handle, task2_handle, main_handle)
        } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Running(task=0)

            #3: +30ms:
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Finished Success

            #4: +30ms:
              "foo" HardCompacting:
                Flow ID = 1 Waiting Manual Executor(task=1, since=30ms)
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Finished Success

            #5: +40ms:
              "foo" HardCompacting:
                Flow ID = 1 Running(task=1)
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Finished Success

            #6: +110ms:
              "foo" HardCompacting:
                Flow ID = 1 Finished Success
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo_Bar" HardCompacting:
                Flow ID = 2 Waiting Input(foo)

            #7: +110ms:
              "foo" HardCompacting:
                Flow ID = 1 Finished Success
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo_Bar" HardCompacting:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=110ms)

            #8: +170ms:
              "foo" HardCompacting:
                Flow ID = 1 Finished Success
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo_Bar" HardCompacting:
                Flow ID = 2 Running(task=2)

            #9: +240ms:
              "foo" HardCompacting:
                Flow ID = 1 Finished Success
              "foo_Bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo_Bar" HardCompacting:
                Flow ID = 2 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_configuration_paused_resumed_modified() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id: DatasetID = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(50).unwrap().into(),
        )
        .await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(80).unwrap().into(),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                run_since_start: Duration::try_milliseconds(10).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: foo_id.clone()
                }),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::try_milliseconds(20).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: bar_id.clone()
                }),
            });
            let task1_handle = task1_driver.run();

            // Main simulation script
            let main_handle = async {
                // Initially, both "foo" and "bar" are scheduled without waiting.
                // "bar":
                //  - flow 0: task 1 starts at 20ms, finishes at 30sms
                //  - next flow 2 queued for 110ms (30+80)
                // "foo":
                //  - flow 1: task 0 starts at 10ms, finishes at 20ms
                //  - next flow 3 queued for 70ms (20+50)

                // 50ms: Pause both flow configs in between completion 2 first tasks and queuing
                harness.advance_time(Duration::try_milliseconds(50).unwrap()).await;
                harness.pause_dataset_flow(start_time + Duration::try_milliseconds(50).unwrap(), foo_id.clone(), DatasetFlowType::Ingest).await;
                harness.pause_dataset_flow(start_time + Duration::try_milliseconds(50).unwrap(), bar_id.clone(), DatasetFlowType::Ingest).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::try_milliseconds(50).unwrap())
                    .await;

                // 80ms: Wake up after initially planned "foo" scheduling but before planned "bar" scheduling:
                //  - "foo":
                //    - gets resumed with previous period of 50ms
                //    - gets scheduled immediately at 80ms (waited >= 1 period)
                //  - "bar":
                //    - gets a config update for period of 70ms
                //    - get queued for 100ms (last success at 30ms + period of 70ms)
                harness.advance_time(Duration::try_milliseconds(30).unwrap()).await;
                harness.resume_dataset_flow(start_time + Duration::try_milliseconds(80).unwrap(), foo_id.clone(), DatasetFlowType::Ingest).await;
                harness.set_dataset_flow_schedule(start_time + Duration::try_milliseconds(80).unwrap(), bar_id.clone(), DatasetFlowType::Ingest, Duration::try_milliseconds(70).unwrap().into()).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::try_milliseconds(80).unwrap())
                    .await;

                // 120ms: finish
                harness.advance_time(Duration::try_milliseconds(40).unwrap()).await;
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
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=110ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
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
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +80ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=2, since=80ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #9: +100ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=3, since=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=2, since=80ms)
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

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(100).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(60).unwrap().into(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                run_since_start: Duration::try_milliseconds(10).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: foo_id.clone()
                }),
          });
          let task0_handle = task0_driver.run();

          // Task 1: "bar" start running at 20ms, finish at 30ms
          let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::try_milliseconds(20).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: bar_id.clone()
                }),
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
              harness.advance_time(Duration::try_milliseconds(50).unwrap()).await;
              harness.pause_dataset_flow(start_time + Duration::try_milliseconds(50).unwrap(), foo_id.clone(), DatasetFlowType::Ingest).await;
              harness.pause_dataset_flow(start_time + Duration::try_milliseconds(50).unwrap(), bar_id.clone(), DatasetFlowType::Ingest).await;
              test_flow_listener
                  .make_a_snapshot(start_time + Duration::try_milliseconds(50).unwrap())
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
              harness.advance_time(Duration::try_milliseconds(50).unwrap()).await;
              harness.resume_dataset_flow(start_time + Duration::try_milliseconds(100).unwrap(), foo_id.clone(), DatasetFlowType::Ingest).await;
              harness.resume_dataset_flow(start_time + Duration::try_milliseconds(100).unwrap(), bar_id.clone(), DatasetFlowType::Ingest).await;
              test_flow_listener
                  .make_a_snapshot(start_time + Duration::try_milliseconds(100).unwrap())
                  .await;

              // 150ms: finish
              harness.advance_time(Duration::try_milliseconds(50).unwrap()).await;
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
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=90ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
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
                Flow ID = 5 Waiting AutoPolling
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +100ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #9: +120ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=3, since=120ms)
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

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(50).unwrap().into(),
        )
        .await;
    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(70).unwrap().into(),
        )
        .await;
    harness.eager_dependencies_graph_init().await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                run_since_start: Duration::try_milliseconds(10).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: foo_id.clone()
                }),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::try_milliseconds(20).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: bar_id.clone()
                }),
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
                harness.advance_time(Duration::try_milliseconds(50).unwrap()).await;
                harness.delete_dataset(&foo_id).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::try_milliseconds(50).unwrap())
                    .await;

                // 120ms: deleting "bar" in SCHEDULED state
                harness.advance_time(Duration::try_milliseconds(70).unwrap()).await;
                harness.delete_dataset(&bar_id).await;
                test_flow_listener
                    .make_a_snapshot(start_time + Duration::try_milliseconds(120).unwrap())
                    .await;

                // 140ms: finish
                harness.advance_time(Duration::try_milliseconds(20).unwrap()).await;
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
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #6: +50ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #7: +100ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=2, since=100ms)
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

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;
    let baz_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("baz"),
            account_name: None,
        })
        .await;

    for dataset_id in [&foo_id, &bar_id, &baz_id] {
        harness
            .set_dataset_flow_schedule(
                harness.now_datetime(),
                dataset_id.clone(),
                DatasetFlowType::Ingest,
                Duration::try_milliseconds(40).unwrap().into(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                run_since_start: Duration::try_milliseconds(10).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: foo_id.clone()
                }),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms with failure
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::try_milliseconds(20).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Failed(TaskError::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: bar_id.clone()
                }),
            });
            let task1_handle = task1_driver.run();

            // Task 1: "baz" start running at 30ms, finish at 40ms with cancellation
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::try_milliseconds(30).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Cancelled)),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: baz_id.clone()
                }),
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
                harness.advance_time(Duration::try_milliseconds(80).unwrap()).await;
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
                Flow ID = 1 Waiting AutoPolling
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=60ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=60ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=60ms)
                Flow ID = 0 Finished Success

            #6: +30ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=60ms)
                Flow ID = 0 Finished Success

            #7: +40ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=60ms)
                Flow ID = 0 Finished Success

            #8: +60ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=60ms)
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_derived_dataset_triggered_initially_and_after_input_change() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(MockDatasetChangesService::with_increment_since(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 3,
                updated_watermark: None,
            },
        )),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_derived_dataset(
            DatasetAlias {
                dataset_name: DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(80).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_batching_rule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::ExecuteTransform,
            BatchingRule::new_checked(1, Duration::try_seconds(1).unwrap()).unwrap(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
                run_since_start: Duration::try_milliseconds(10).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: foo_id.clone()
                }),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::try_milliseconds(20).unwrap(),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                  pull_result: PullResult::Updated {
                    old_head: Some(Multihash::from_digest_sha3_256(b"old-slice")),
                    new_head: Multihash::from_digest_sha3_256(b"new-slice"),
                  },
                })))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: bar_id.clone()
                }),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 110ms, finish at 120ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::try_milliseconds(110).unwrap(),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                  pull_result: PullResult::Updated {
                    old_head: Some(Multihash::from_digest_sha3_256(b"new-slice")),
                    new_head: Multihash::from_digest_sha3_256(b"newest-slice"),
                  },
                })))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: foo_id.clone()
                }),
            });
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 130ms, finish at 140ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::try_milliseconds(130).unwrap(),
                finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
                expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
                  dataset_id: bar_id.clone()
                }),
            });
            let task3_handle = task3_driver.run();


            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::try_milliseconds(220).unwrap()).await;
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
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #6: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 0 Finished Success

            #7: +110ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 0 Finished Success

            #8: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(1, until=1120ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #9: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Executor(task=3, since=120ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #10: +130ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Running(task=3)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #11: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #12: +200ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=4, since=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_throttling_manual_triggers() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap()),
        mandatory_throttling_period: Some(
            Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS * 10).unwrap(),
        ),
        ..Default::default()
    });

    // Foo Flow
    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_flow_key: FlowKey = FlowKeyDataset::new(foo_id.clone(), DatasetFlowType::Ingest).into();

    // Enforce dependency graph initialization
    harness.eager_dependencies_graph_init().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
            run_since_start: Duration::try_milliseconds(20).unwrap(),
        });
        let trigger0_handle = trigger0_driver.run();

        // Manual trigger for "foo" at 30ms
        let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
            flow_key: foo_flow_key.clone(),
            run_since_start: Duration::try_milliseconds(30).unwrap(),
        });
        let trigger1_handle = trigger1_driver.run();

        // Manual trigger for "foo" at 70ms
        let trigger2_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
          flow_key: foo_flow_key,
          run_since_start: Duration::try_milliseconds(70).unwrap(),
        });
        let trigger2_handle = trigger2_driver.run();

        // Task 0: "foo" start running at 40ms, finish at 50ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(0),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(40).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task0_handle = task0_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::try_milliseconds(100).unwrap()).await;
          test_flow_listener
              .make_a_snapshot(start_time + Duration::try_milliseconds(100).unwrap())
              .await;
          harness.advance_time(Duration::try_milliseconds(70).unwrap()).await;
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
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #2: +40ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +50ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #4: +100ms:
              "foo" Ingest:
                Flow ID = 1 Waiting Manual Throttling(for=100ms, wakeup=150ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #5: +150ms:
              "foo" Ingest:
                Flow ID = 1 Waiting Manual Executor(task=1, since=150ms)
                Flow ID = 0 Finished Success

          "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_throttling_derived_dataset_with_2_parents() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap()), // 10ms,
        mandatory_throttling_period: Some(
            Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS * 10).unwrap(),
        ), /* 100ms */
        mock_dataset_changes: Some(MockDatasetChangesService::with_increment_since(
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 7,
                updated_watermark: None,
            },
        )),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;
    let baz_id = harness
        .create_derived_dataset(
            DatasetAlias {
                dataset_name: DatasetName::new_unchecked("baz"),
                account_name: None,
            },
            vec![foo_id.clone(), bar_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(50).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(150).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_batching_rule(
            harness.now_datetime(),
            baz_id.clone(),
            DatasetFlowType::ExecuteTransform,
            BatchingRule::new_checked(1, Duration::try_hours(24).unwrap()).unwrap(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
            run_since_start: Duration::try_milliseconds(10).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(20).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"fbar-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "baz" start running at 30ms, finish at 50ms (simulate longer run)
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::try_milliseconds(30).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(20).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: baz_id.clone()
            }),
        });
        let task2_handle = task2_driver.run();

        // Task 3: "foo" start running at 130ms, finish at 140ms
        let task3_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(3),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(130).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                    old_head: Some(Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    new_head: Multihash::from_digest_sha3_256(b"foo-newest-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task3_handle = task3_driver.run();

        // Task 4: "baz" start running at 160ms, finish at 170ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::try_milliseconds(160).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::Empty))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: baz_id.clone()
            }),
        });
        let task4_handle = task4_driver.run();

        // Task 5: "bar" start running at 190ms, finish at 200ms
        let task5_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(5),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(190).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-newest-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
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

          harness.advance_time(Duration::try_milliseconds(400).unwrap()).await;
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
              Flow ID = 1 Waiting AutoPolling
            "baz" ExecuteTransform:
              Flow ID = 2 Waiting AutoPolling
            "foo" Ingest:
              Flow ID = 0 Waiting AutoPolling

          #1: +0ms:
            "bar" Ingest:
              Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
            "baz" ExecuteTransform:
              Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
            "foo" Ingest:
              Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

          #2: +10ms:
            "bar" Ingest:
              Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
            "baz" ExecuteTransform:
              Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
            "foo" Ingest:
              Flow ID = 0 Running(task=0)

          #3: +20ms:
            "bar" Ingest:
              Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
            "baz" ExecuteTransform:
              Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
            "foo" Ingest:
              Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
              Flow ID = 0 Finished Success

          #4: +20ms:
            "bar" Ingest:
              Flow ID = 1 Running(task=1)
            "baz" ExecuteTransform:
              Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
            "foo" Ingest:
              Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
              Flow ID = 0 Finished Success

          #5: +30ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
            "foo" Ingest:
              Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
              Flow ID = 0 Finished Success

          #6: +30ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Running(task=2)
            "foo" Ingest:
              Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
              Flow ID = 0 Finished Success

          #7: +50ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
              Flow ID = 0 Finished Success

          #8: +120ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 3 Waiting AutoPolling Executor(task=3, since=120ms)
              Flow ID = 0 Finished Success

          #9: +130ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 3 Running(task=3)
              Flow ID = 0 Finished Success

          #10: +140ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Waiting Input(foo) Throttling(for=100ms, wakeup=150ms, shifted=140ms)
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #11: +150ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Waiting Input(foo) Executor(task=4, since=150ms)
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #12: +160ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Running(task=4)
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #13: +170ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Schedule(wakeup=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #14: +180ms:
            "bar" Ingest:
              Flow ID = 4 Waiting AutoPolling Executor(task=5, since=180ms)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #15: +190ms:
            "bar" Ingest:
              Flow ID = 4 Running(task=5)
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #16: +200ms:
            "bar" Ingest:
              Flow ID = 8 Waiting AutoPolling Schedule(wakeup=350ms)
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Waiting Input(bar) Throttling(for=100ms, wakeup=270ms, shifted=200ms)
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #17: +240ms:
            "bar" Ingest:
              Flow ID = 8 Waiting AutoPolling Schedule(wakeup=350ms)
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Waiting Input(bar) Throttling(for=100ms, wakeup=270ms, shifted=200ms)
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #18: +270ms:
            "bar" Ingest:
              Flow ID = 8 Waiting AutoPolling Schedule(wakeup=350ms)
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Waiting Input(bar) Executor(task=7, since=270ms)
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

          #19: +350ms:
            "bar" Ingest:
              Flow ID = 8 Waiting AutoPolling Executor(task=8, since=350ms)
              Flow ID = 4 Finished Success
              Flow ID = 1 Finished Success
            "baz" ExecuteTransform:
              Flow ID = 7 Waiting Input(bar) Executor(task=7, since=270ms)
              Flow ID = 5 Finished Success
              Flow ID = 2 Finished Success
            "foo" Ingest:
              Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
              Flow ID = 3 Finished Success
              Flow ID = 0 Finished Success

        "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_records_reached() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetChangesService::new();
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 10,
                updated_watermark: None,
            })
        });

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_derived_dataset(
            DatasetAlias {
                dataset_name: DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(50).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_batching_rule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::ExecuteTransform,
            BatchingRule::new_checked(10, Duration::try_milliseconds(120).unwrap()).unwrap(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
            run_since_start: Duration::try_milliseconds(10).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(20).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "foo" start running at 80ms, finish at 90ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(80).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task2_handle = task2_driver.run();

        // Task 3: "foo" start running at 150ms, finish at 160ms
        let task3_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(3),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(150).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-new-slice-2")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice-3"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task3_handle = task3_driver.run();

        // Task 4: "bar" start running at 170ms, finish at 180ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(170).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task4_handle = task4_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::try_milliseconds(400).unwrap()).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task3_handle, task4_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #6: +70ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=70ms)
                Flow ID = 0 Finished Success

            #7: +80ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 0 Finished Success

            #8: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(10, until=210ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #9: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(10, until=210ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=3, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #10: +150ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(10, until=210ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Running(task=3)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #11: +160ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(10, until=210ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=210ms)
                Flow ID = 4 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #12: +160ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Executor(task=4, since=160ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=210ms)
                Flow ID = 4 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #13: +170ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Running(task=4)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=210ms)
                Flow ID = 4 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #14: +180ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=210ms)
                Flow ID = 4 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #15: +210ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=5, since=210ms)
                Flow ID = 4 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

      "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_timeout() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetChangesService::new();
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_derived_dataset(
            DatasetAlias {
                dataset_name: DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(50).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_batching_rule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::ExecuteTransform,
            BatchingRule::new_checked(10, Duration::try_milliseconds(150).unwrap()).unwrap(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
            run_since_start: Duration::try_milliseconds(10).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(20).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "foo" start running at 80ms, finish at 90ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(80).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task2_handle = task2_driver.run();

        // Task 3 is scheduled, but never runs

        // Task 4: "bar" start running at 250ms, finish at 2560ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(250).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task4_handle = task4_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::try_milliseconds(400).unwrap()).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task4_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #6: +70ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=70ms)
                Flow ID = 0 Finished Success

            #7: +80ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 0 Finished Success

            #8: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(10, until=240ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #9: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(10, until=240ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=3, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #10: +240ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Executor(task=4, since=240ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=3, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #11: +250ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Running(task=4)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=3, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #12: +260ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=3, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

      "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_watermark() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetChangesService::new();
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 0, // no records, just watermark
                updated_watermark: Some(Utc::now()),
            })
        });

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_derived_dataset(
            DatasetAlias {
                dataset_name: DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(40).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_batching_rule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::ExecuteTransform,
            BatchingRule::new_checked(10, Duration::try_milliseconds(200).unwrap()).unwrap(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
            run_since_start: Duration::try_milliseconds(10).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(20).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "foo" start running at 70ms, finish at 80ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(70).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task2_handle = task2_driver.run();

        // Task 3 is scheduled, but never runs

        // Task 4: "bar" start running at 290ms, finish at 300ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(290).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task4_handle = task4_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::try_milliseconds(400).unwrap()).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task4_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
        #0: +0ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting AutoPolling
          "foo" Ingest:
            Flow ID = 0 Waiting AutoPolling

        #1: +0ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
          "foo" Ingest:
            Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

        #2: +10ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
          "foo" Ingest:
            Flow ID = 0 Running(task=0)

        #3: +20ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Schedule(wakeup=60ms)
            Flow ID = 0 Finished Success

        #4: +20ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Running(task=1)
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Schedule(wakeup=60ms)
            Flow ID = 0 Finished Success

        #5: +30ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Schedule(wakeup=60ms)
            Flow ID = 0 Finished Success

        #6: +60ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=60ms)
            Flow ID = 0 Finished Success

        #7: +70ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 2 Running(task=2)
            Flow ID = 0 Finished Success

        #8: +80ms:
          "bar" ExecuteTransform:
            Flow ID = 3 Waiting Input(foo) Batching(10, until=280ms)
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #9: +120ms:
          "bar" ExecuteTransform:
            Flow ID = 3 Waiting Input(foo) Batching(10, until=280ms)
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 4 Waiting AutoPolling Executor(task=3, since=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #10: +280ms:
          "bar" ExecuteTransform:
            Flow ID = 3 Waiting Input(foo) Executor(task=4, since=280ms)
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 4 Waiting AutoPolling Executor(task=3, since=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #11: +290ms:
          "bar" ExecuteTransform:
            Flow ID = 3 Running(task=4)
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 4 Waiting AutoPolling Executor(task=3, since=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #12: +300ms:
          "bar" ExecuteTransform:
            Flow ID = 3 Finished Success
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 4 Waiting AutoPolling Executor(task=3, since=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        "#
        )
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_with_2_inputs() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetChangesService::new();
    // 'foo': first reading of task 3 after 'foo' task 3
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    // 'foo': Second reading of task 3 after 'bar' task 4
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    // 'bar' : First reading of task 4 after task 4
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 7,
                updated_watermark: None,
            })
        });
    // 'foo': third reading of tasks 3, 5 after foo task 5
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 8,
                updated_watermark: None,
            })
        });
    // 'bar' : Second reading of task 4 after foo task 5
    mock_dataset_changes
        .expect_get_increment_since()
        .times(1)
        .in_sequence(&mut seq)
        .returning(|_, _| {
            Ok(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 7,
                updated_watermark: None,
            })
        });

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(DatasetAlias {
            dataset_name: DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;
    let baz_id = harness
        .create_derived_dataset(
            DatasetAlias {
                dataset_name: DatasetName::new_unchecked("baz"),
                account_name: None,
            },
            vec![foo_id.clone(), bar_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            foo_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(80).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_schedule(
            harness.now_datetime(),
            bar_id.clone(),
            DatasetFlowType::Ingest,
            Duration::try_milliseconds(120).unwrap().into(),
        )
        .await;

    harness
        .set_dataset_flow_batching_rule(
            harness.now_datetime(),
            baz_id.clone(),
            DatasetFlowType::ExecuteTransform,
            BatchingRule::new_checked(15, Duration::try_milliseconds(200).unwrap()).unwrap(),
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
        .duration_round(Duration::try_milliseconds(SCHEDULING_ALIGNMENT_MS).unwrap())
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
            run_since_start: Duration::try_milliseconds(10).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(20).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "baz" start running at 30ms, finish at 40ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::try_milliseconds(30).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"baz-old-slice")),
                new_head: Multihash::from_digest_sha3_256(b"baz-new-slice"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: baz_id.clone()
            }),
        });
        let task2_handle = task2_driver.run();

        // Task 3: "foo" start running at 110ms, finish at 120ms
        let task3_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(3),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(110).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task3_handle = task3_driver.run();

        // Task 4: "bar" start running at 160ms, finish at 170ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::try_milliseconds(160).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"bar-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: bar_id.clone()
            }),
        });
        let task4_handle = task4_driver.run();

        // Task 5: "foo" start running at 210ms, finish at 220ms
        let task5_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(5),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::try_milliseconds(210).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"foo-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: foo_id.clone()
            }),
        });
        let task5_handle = task5_driver.run();

        // Task 6: "baz" start running at 230ms, finish at 240ms
        let task6_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(6),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::try_milliseconds(230).unwrap(),
            finish_in_with: Some((Duration::try_milliseconds(10).unwrap(), TaskOutcome::Success(TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult{
                pull_result: PullResult::Updated {
                old_head: Some(Multihash::from_digest_sha3_256(b"baz-new-slice")),
                new_head: Multihash::from_digest_sha3_256(b"baz-new-slice-2"),
                },
            })))),
            expected_logical_plan: LogicalPlan::UpdateDataset(UpdateDataset {
              dataset_id: baz_id.clone()
            }),
        });
        let task6_handle = task6_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::try_milliseconds(400).unwrap()).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task3_handle, task4_handle, task5_handle, task6_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        format!("{}", test_flow_listener.as_ref()),
        indoc::indoc!(
            r#"
        #0: +0ms:
          "bar" Ingest:
            Flow ID = 1 Waiting AutoPolling
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting AutoPolling
          "foo" Ingest:
            Flow ID = 0 Waiting AutoPolling

        #1: +0ms:
          "bar" Ingest:
            Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
          "foo" Ingest:
            Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

        #2: +10ms:
          "bar" Ingest:
            Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
          "foo" Ingest:
            Flow ID = 0 Running(task=0)

        #3: +20ms:
          "bar" Ingest:
            Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #4: +20ms:
          "bar" Ingest:
            Flow ID = 1 Running(task=1)
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #5: +30ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #6: +30ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Running(task=2)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #7: +40ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #8: +100ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Executor(task=3, since=100ms)
            Flow ID = 0 Finished Success

        #9: +110ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 3 Running(task=3)
            Flow ID = 0 Finished Success

        #10: +120ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Batching(15, until=320ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #11: +150ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Executor(task=4, since=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Batching(15, until=320ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #12: +160ms:
          "bar" Ingest:
            Flow ID = 4 Running(task=4)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Batching(15, until=320ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #13: +170ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Batching(15, until=320ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #14: +200ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Batching(15, until=320ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 6 Waiting AutoPolling Executor(task=5, since=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #15: +210ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Batching(15, until=320ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 6 Running(task=5)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #16: +220ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Batching(15, until=320ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 8 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 6 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #17: +220ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Waiting Input(foo) Executor(task=6, since=220ms)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 8 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 6 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #18: +230ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Running(task=6)
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 8 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 6 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #19: +240ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Finished Success
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 8 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 6 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #20: +290ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Executor(task=7, since=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Finished Success
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 8 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 6 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #21: +300ms:
          "bar" Ingest:
            Flow ID = 7 Waiting AutoPolling Executor(task=7, since=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 5 Finished Success
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 8 Waiting AutoPolling Executor(task=8, since=300ms)
            Flow ID = 6 Finished Success
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
