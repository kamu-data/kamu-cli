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
use futures::TryStreamExt;
use kamu_accounts::{AccountConfig, CurrentAccountSubject};
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowConfigRuleCompact,
    FlowConfigRuleCompactFull,
    FlowConfigRuleIngest,
    FlowConfigRuleReset,
};
use kamu_adapter_task_dataset::{
    LogicalPlanDatasetHardCompact,
    LogicalPlanDatasetReset,
    LogicalPlanDatasetUpdate,
    TaskResultDatasetHardCompact,
    TaskResultDatasetReset,
    TaskResultDatasetUpdate,
};
use kamu_core::{CompactionResult, PullResult, ResetResult};
use kamu_datasets::DatasetIntervalIncrement;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::*;

use super::{
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    ManualFlowAbortArgs,
    ManualFlowTriggerArgs,
    SCHEDULING_ALIGNMENT_MS,
    TaskDriverArgs,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_read_initial_config_and_queue_without_waiting() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 60ms
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness
        .set_dataset_flow_ingest(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
            },
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
        )
        .await;
    harness.eager_initialization().await;

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: start running at 10ms, finish at 20ms
                let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Task 1: start running at 90ms, finish at 100ms
                let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(90),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let foo_task1_handle = foo_task1_driver.run();

                // Main simulation boundary - 120ms total
                //  - "foo" should immediately schedule "task 0", since "foo" has never run yet
                //  - "task 0" will take action and complete, this will schedule the next flow
                //    run for "foo" after full period
                //  - when that period is over, "task 1" should be scheduled
                //  - "task 1" will take action and complete, enqueuing another flow
                let sim_handle = harness.advance_time(Duration::milliseconds(120));
                tokio::join!(foo_task0_handle, foo_task1_handle, sim_handle)
            } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_read_initial_config_should_not_queue_in_recovery_case() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_ingest_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST);

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Configure ingestion schedule every 60ms, but use event store directly
    harness
        .flow_trigger_event_store
        .save_events(
            &foo_ingest_binding,
            None,
            vec![
                FlowTriggerEventCreated {
                    event_time: start_time,
                    flow_binding: foo_ingest_binding.clone(),
                    paused: false,
                    rule: FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Mimic we are recovering from server restart, where a waiting flow for "foo"
    // existed already
    let flow_id = harness.flow_event_store.new_flow_id().await.unwrap();
    harness
        .flow_event_store
        .save_events(
            &flow_id,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_time,
                    flow_id,
                    flow_binding: foo_ingest_binding.clone(),
                    trigger: FlowTriggerInstance::AutoPolling(FlowTriggerAutoPolling {
                        trigger_time: start_time,
                    }),
                    config_snapshot: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    event_time: Utc::now(),
                    flow_id,
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: start_time + Duration::milliseconds(100),
                    }),
                    last_trigger_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    event_time: Utc::now(),
                    flow_id,
                    scheduled_for_activation_at: start_time + Duration::milliseconds(100),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    harness.eager_initialization().await;

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: start running at 110ms, finish at 120ms
                let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(110),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Main simulation boundary - 130ms total
                let sim_handle = harness.advance_time(Duration::milliseconds(130));
                tokio::join!(foo_task0_handle, sim_handle)
            } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=100ms)

            #1: +100ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=100ms)

            #2: +110ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #3: +120ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=180ms)
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cron_config() {
    // Note: this test runs with 1s step, CRON does not apply to milliseconds
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::seconds(1)),
        mandatory_throttling_period: Some(Duration::seconds(1)),
        ..Default::default()
    });

    // Create a "foo" root dataset, configure ingestion cron schedule of every 5s
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness.eager_initialization().await;

    // Remember start time
    let start_time = harness
        .now_datetime()
        .duration_round(Duration::seconds(1))
        .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: start running at 6s, finish at 7s
                let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::seconds(6),
                    finish_in_with: Some((Duration::seconds(1), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false,
                    }.into_logical_plan(),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Wait 2 s
                    harness.advance_time_custom_alignment(Duration::seconds(1), Duration::seconds(2)).await;

                    // Enable CRON config (we are skipping moment 0s)
                    harness
                      .set_flow_trigger(
                          harness.now_datetime(),
                          FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
                          FlowTriggerRule::Schedule(Schedule::Cron(ScheduleCron {
                            source_5component_cron_expression: String::from("<irrelevant>"),
                            cron_schedule: cron::Schedule::from_str("*/5 * * * * *").unwrap(),
                          })),
                        )
                        .await;
                    test_flow_listener
                        .make_a_snapshot(start_time + Duration::seconds(1))
                        .await;

                    // Main simulation boundary - 12s total: at 10s 2nd scheduling happens;
                    harness.advance_time_custom_alignment(Duration::seconds(1), Duration::seconds(11)).await;
                };

                tokio::join!(foo_task0_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +1000ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=5000ms)

            #2: +5000ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=5000ms)

            #3: +6000ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +7000ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=10000ms)
                Flow ID = 0 Finished Success

            #5: +10000ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=10000ms)
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST);
    let bar_flow_binding = FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST);

    // Note: only "foo" has auto-schedule, "bar" hasn't
    harness
        .set_flow_trigger(
            harness.now_datetime(),
            foo_flow_binding.clone(),
            FlowTriggerRule::Schedule(Duration::milliseconds(90).into()),
        )
        .await;
    harness.eager_initialization().await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: "foo" start running at 10ms, finish at 20ms
                let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let task0_handle = task0_driver.run();

                // Task 1: "for" start running at 60ms, finish at 70ms
                let task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(60),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let task1_handle = task1_driver.run();

                // Task 2: "bar" start running at 100ms, finish at 110ms
                let task2_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(2),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                    dataset_id: Some(bar_id.clone()),
                    run_since_start: Duration::milliseconds(100),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: bar_id.clone(),
                      fetch_uncacheable: true
                    }.into_logical_plan(),
                });
                let task2_handle = task2_driver.run();

                // Manual trigger for "foo" at 40ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: foo_flow_binding,
                    run_since_start: Duration::milliseconds(40),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 80ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                  flow_binding: bar_flow_binding,
                    run_since_start: Duration::milliseconds(80),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: Some(FlowConfigRuleIngest {
                      fetch_uncacheable: true
                    }.into_flow_config()),
                });
                let trigger1_handle = trigger1_driver.run();

                // Main simulation script
                let main_handle = async {
                    // "foo":
                    //  - flow 0 => task 0 gets scheduled immediately at 0ms
                    //  - flow 0 => task 0 starts at 10ms and finishes running at 20ms
                    //  - next flow => scheduled at 20ms to trigger in 1 period of 90ms - at 110ms
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
                    //  - no next flow scheduled

                    // Stop at 180ms: "foo" flow 3 gets scheduled at 160ms
                    harness.advance_time(Duration::milliseconds(180)).await;
                };

                tokio::join!(task0_handle, task1_handle, task2_handle, trigger0_handle, trigger1_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_trigger_with_ingest_config() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST);
    let bar_flow_binding = FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST);

    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            FlowConfigRuleIngest {
                fetch_uncacheable: true,
            },
        )
        .await;
    harness
        .set_flow_trigger(
            harness.now_datetime(),
            foo_flow_binding.clone(),
            FlowTriggerRule::Schedule(Duration::milliseconds(90).into()),
        )
        .await;
    harness.eager_initialization().await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: "foo" start running at 10ms, finish at 20ms
                let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: true
                    }.into_logical_plan(),
                });
                let task0_handle = task0_driver.run();

                // Task 1: "for" start running at 60ms, finish at 70ms
                let task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(60),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: true
                    }.into_logical_plan(),
                });
                let task1_handle = task1_driver.run();

                // Task 2: "bar" start running at 100ms, finish at 110ms
                let task2_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(2),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                    dataset_id: Some(bar_id.clone()),
                    run_since_start: Duration::milliseconds(100),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: bar_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let task2_handle = task2_driver.run();

                // Manual trigger for "foo" at 40ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: foo_flow_binding,
                    run_since_start: Duration::milliseconds(40),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 80ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: bar_flow_binding,
                    run_since_start: Duration::milliseconds(80),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger1_handle = trigger1_driver.run();

                // Main simulation script
                let main_handle = async {
                    // "foo":
                    //  - flow 0 => task 0 gets scheduled immediately at 0ms
                    //  - flow 0 => task 0 starts at 10ms and finishes running at 20ms
                    //  - next flow => scheduled at 20ms to trigger in 1 period of 90ms - at 110ms
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
                    //  - no next flow scheduled

                    // Stop at 180ms: "foo" flow 3 gets scheduled at 110ms
                    harness.advance_time(Duration::milliseconds(180)).await;
                };

                tokio::join!(task0_handle, task1_handle, task2_handle, trigger0_handle, trigger1_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_compaction() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);
    let bar_flow_binding = FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                  // Task 0: "foo" start running at 10ms, finish at 20ms
                  let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetHardCompact {
                      dataset_id: foo_id.clone(),
                      max_slice_size: None,
                      max_slice_records: None,
                      keep_metadata_only: false,
                    }.into_logical_plan(),
                });
                let task0_handle = task0_driver.run();

                let task1_driver = harness.task_driver(TaskDriverArgs {
                  task_id: TaskID::new(1),
                  task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                  dataset_id: Some(bar_id.clone()),
                  run_since_start: Duration::milliseconds(60),
                  finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                  expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: bar_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                    keep_metadata_only: false,
                  }.into_logical_plan(),
                });
                let task1_handle = task1_driver.run();

                // Manual trigger for "foo" at 10ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: foo_flow_binding,
                    run_since_start: Duration::milliseconds(10),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 50ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: bar_flow_binding,
                    run_since_start: Duration::milliseconds(50),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger1_handle = trigger1_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Moment 10ms - manual foo trigger happens here:
                    //  - flow 0 gets trigger and finishes at 30ms

                    // Moment 50ms - manual foo trigger happens here:
                    //  - flow 1 trigger and finishes
                    //  - task 1 starts at 60ms, finishes at 70ms (leave some gap to fight with random order)

                    harness.advance_time(Duration::milliseconds(100)).await;
                };

                tokio::join!(task0_handle, task1_handle, trigger0_handle, trigger1_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #2: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)

            #3: +30ms:
              "bar" HardCompaction:
                Flow ID = 1 Waiting Manual
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            #4: +50ms:
              "bar" HardCompaction:
                Flow ID = 1 Waiting Manual Executor(task=1, since=50ms)
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            #5: +60ms:
              "bar" HardCompaction:
                Flow ID = 1 Running(task=1)
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            #6: +70ms:
              "bar" HardCompaction:
                Flow ID = 1 Finished Success
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_reset() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness.eager_initialization().await;
    harness
        .set_dataset_flow_reset_rule(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_RESET),
            FlowConfigRuleReset {
                new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                recursive: false,
            },
        )
        .await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_RESET);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                  // Task 0: "foo" start running at 20ms, finish at 110ms
                  let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(20),
                    finish_in_with: Some((Duration::milliseconds(90), TaskOutcome::Success(
                      TaskResultDatasetReset {
                        reset_result: ResetResult { new_head: odf::Multihash::from_digest_sha3_256(b"new-slice") },
                      }.into_task_result())
                    )),
                    expected_logical_plan: LogicalPlanDatasetReset {
                      dataset_id: foo_id.clone(),
                      // By default, should reset to seed block
                      new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                      old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                      recursive: false,
                    }.into_logical_plan(),
                });
                let task0_handle = task0_driver.run();

                // Manual trigger for "foo" at 10ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: foo_flow_binding,
                    run_since_start: Duration::milliseconds(10),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger0_handle = trigger0_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Moment 20ms - manual foo trigger happens here:
                    //  - flow 0 gets trigger and finishes at 110ms
                    harness.advance_time(Duration::milliseconds(250)).await;
                };

                tokio::join!(task0_handle, trigger0_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" Reset:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #2: +20ms:
              "foo" Reset:
                Flow ID = 0 Running(task=0)

            #3: +110ms:
              "foo" Reset:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_trigger_keep_metadata_compaction_for_derivatives() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;
    let foo_baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.baz"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_reset_rule(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_RESET),
            FlowConfigRuleReset {
                new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                recursive: true,
            },
        )
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_RESET);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener.define_dataset_display_name(foo_baz_id.clone(), "foo_baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
      // Run API service
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
          let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
              flow_binding: foo_flow_binding,
              run_since_start: Duration::milliseconds(10),
              initiator_id: None,
              flow_configuration_snapshot_maybe: None,
          });
          let trigger0_handle = trigger0_driver.run();

          // Task 0: "foo" start running at 20ms, finish at 90ms
          let task0_driver = harness.task_driver(TaskDriverArgs {
              task_id: TaskID::new(0),
              task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
              dataset_id: Some(foo_id.clone()),
              run_since_start: Duration::milliseconds(20),
              finish_in_with: Some((Duration::milliseconds(70), TaskOutcome::Success(
                TaskResultDatasetReset {
                  reset_result: ResetResult { new_head: odf::Multihash::from_digest_sha3_256(b"new-slice") }
                }.into_task_result()
              ))),
              expected_logical_plan: LogicalPlanDatasetReset {
                dataset_id: foo_id.clone(),
                new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                recursive: true,
              }.into_logical_plan(),
          });
          let task0_handle = task0_driver.run();

          // Task 1: "foo_bar" start running at 110ms, finish at 180sms
          let task1_driver = harness.task_driver(TaskDriverArgs {
              task_id: TaskID::new(1),
              task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
              dataset_id: Some(foo_baz_id.clone()),
              run_since_start: Duration::milliseconds(110),
              finish_in_with: Some(
                (
                  Duration::milliseconds(70),
                  TaskOutcome::Success(TaskResultDatasetHardCompact {
                    compaction_result: CompactionResult::Success {
                      old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-2"),
                      new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-2"),
                      old_num_blocks: 5,
                      new_num_blocks: 4,
                    }
                  }.into_task_result()
                )
              )),
              expected_logical_plan: LogicalPlanDatasetHardCompact {
                dataset_id: foo_baz_id.clone(),
                max_slice_size: None,
                max_slice_records: None,
                keep_metadata_only: true,
              }.into_logical_plan(),
          });
          let task1_handle = task1_driver.run();

          // Task 2: "foo_bar_baz" start running at 200ms, finish at 240ms
          let task2_driver = harness.task_driver(TaskDriverArgs {
              task_id: TaskID::new(2),
              task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
              dataset_id: Some(foo_bar_id.clone()),
              run_since_start: Duration::milliseconds(200),
              finish_in_with: Some(
                (
                  Duration::milliseconds(40),
                  TaskOutcome::Success(TaskResultDatasetHardCompact {
                    compaction_result: CompactionResult::Success {
                      old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-3"),
                      new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-3"),
                      old_num_blocks: 8,
                      new_num_blocks: 3,
                    }
                  }.into_task_result()
                )
              )),
              expected_logical_plan: LogicalPlanDatasetHardCompact {
                dataset_id: foo_bar_id.clone(),
                max_slice_size: None,
                max_slice_records: None,
                keep_metadata_only: true,
              }.into_logical_plan(),
          });
          let task2_handle = task2_driver.run();

          // Main simulation script
          let main_handle = async {
              harness.advance_time(Duration::milliseconds(400)).await;
          };

          tokio::join!(trigger0_handle, task0_handle, task1_handle, task2_handle, main_handle)
      } => Ok(())
  }
  .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
          #0: +0ms:

          #1: +10ms:
            "foo" Reset:
              Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

          #2: +20ms:
            "foo" Reset:
              Flow ID = 0 Running(task=0)

          #3: +90ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo)
            "foo_baz" HardCompaction:
              Flow ID = 1 Waiting Input(foo)

          #4: +90ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo) Executor(task=2, since=90ms)
            "foo_baz" HardCompaction:
              Flow ID = 1 Waiting Input(foo) Executor(task=1, since=90ms)

          #5: +110ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo) Executor(task=2, since=90ms)
            "foo_baz" HardCompaction:
              Flow ID = 1 Running(task=1)

          #6: +180ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo) Executor(task=2, since=90ms)
            "foo_baz" HardCompaction:
              Flow ID = 1 Finished Success

          #7: +200ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Running(task=2)
            "foo_baz" HardCompaction:
              Flow ID = 1 Finished Success

          #8: +240ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Finished Success
            "foo_baz" HardCompaction:
              Flow ID = 1 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_compaction_with_config() {
    let max_slice_size = 1_000_000u64;
    let max_slice_records = 1000u64;
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness.eager_initialization().await;
    harness
        .set_dataset_flow_compaction_rule(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT),
            FlowConfigRuleCompact::Full(
                FlowConfigRuleCompactFull::new_checked(max_slice_size, max_slice_records, false)
                    .unwrap(),
            ),
        )
        .await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: "foo" start running at 30ms, finish at 40ms
                let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(30),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetHardCompact {
                      dataset_id: foo_id.clone(),
                      max_slice_size: Some(max_slice_size),
                      max_slice_records: Some(max_slice_records),
                      keep_metadata_only: false,
                    }.into_logical_plan(),
                });
                let task0_handle = task0_driver.run();

                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: foo_flow_binding,
                    run_since_start: Duration::milliseconds(20),
                    initiator_id: None,
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger0_handle = trigger0_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Moment 30ms - manual foo trigger happens here:
                    //  - flow 0 trigger and finishes at 40ms
                    harness.advance_time(Duration::milliseconds(80)).await;
                };

                tokio::join!(task0_handle, trigger0_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #2: +30ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)

            #3: +40ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_full_hard_compaction_trigger_keep_metadata_compaction_for_derivatives() {
    let max_slice_size = 1_000_000u64;
    let max_slice_records = 1000u64;
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;
    let foo_baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.baz"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_compaction_rule(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT),
            FlowConfigRuleCompact::Full(
                FlowConfigRuleCompactFull::new_checked(max_slice_size, max_slice_records, true)
                    .unwrap(),
            ),
        )
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener.define_dataset_display_name(foo_baz_id.clone(), "foo_baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                flow_configuration_snapshot_maybe: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 20ms, finish at 90ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some(
                  (
                    Duration::milliseconds(70),
                    TaskOutcome::Success(TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                      }
                    }.into_task_result())
                  )
                ),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_id.clone(),
                  max_slice_size: Some(max_slice_size),
                  max_slice_records: Some(max_slice_records),
                  keep_metadata_only: false,
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo_baz" start running at 110ms, finish at 180sms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_baz_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some(
                  (
                    Duration::milliseconds(70),
                    TaskOutcome::Success(TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-2"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-2"),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                      }
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_baz_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo_bar" start running at 200ms, finish at 240ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_bar_id.clone()),
                run_since_start: Duration::milliseconds(200),
                finish_in_with: Some(
                  (
                    Duration::milliseconds(40),
                    TaskOutcome::Success(TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-3"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-3"),
                        old_num_blocks: 8,
                        new_num_blocks: 3,
                      }
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_bar_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(300)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, task1_handle, task2_handle, main_handle)
        } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
          #0: +0ms:

          #1: +10ms:
            "foo" HardCompaction:
              Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

          #2: +20ms:
            "foo" HardCompaction:
              Flow ID = 0 Running(task=0)

          #3: +90ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo)
            "foo_baz" HardCompaction:
              Flow ID = 1 Waiting Input(foo)

          #4: +90ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo) Executor(task=2, since=90ms)
            "foo_baz" HardCompaction:
              Flow ID = 1 Waiting Input(foo) Executor(task=1, since=90ms)

          #5: +110ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo) Executor(task=2, since=90ms)
            "foo_baz" HardCompaction:
              Flow ID = 1 Running(task=1)

          #6: +180ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Waiting Input(foo) Executor(task=2, since=90ms)
            "foo_baz" HardCompaction:
              Flow ID = 1 Finished Success

          #7: +200ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Running(task=2)
            "foo_baz" HardCompaction:
              Flow ID = 1 Finished Success

          #8: +240ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" HardCompaction:
              Flow ID = 2 Finished Success
            "foo_baz" HardCompaction:
              Flow ID = 1 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref()),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_keep_metadata_only_with_recursive_compaction() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;
    let foo_bar_baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar.baz"),
                account_name: None,
            },
            vec![foo_bar_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_compaction_rule(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT),
            FlowConfigRuleCompact::MetadataOnly { recursive: true },
        )
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener
        .define_dataset_display_name(foo_bar_baz_id.clone(), "foo_bar_baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                flow_configuration_snapshot_maybe: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 20ms, finish at 90ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some(
                  (
                    Duration::milliseconds(70),
                    TaskOutcome::Success(TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                      }
                    }.into_task_result())
                  )
                ),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo_bar" start running at 110ms, finish at 180sms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_bar_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some(
                  (
                    Duration::milliseconds(70),
                    TaskOutcome::Success(TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-2"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-2"),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                      }
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_bar_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo_bar_baz" start running at 200ms, finish at 240ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_bar_baz_id.clone()),
                run_since_start: Duration::milliseconds(200),
                finish_in_with: Some(
                  (
                    Duration::milliseconds(40),
                    TaskOutcome::Success(TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-3"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-3"),
                        old_num_blocks: 8,
                        new_num_blocks: 3,
                      }
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_bar_baz_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(300)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, task1_handle, task2_handle, main_handle)
        } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #2: +20ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)

            #3: +90ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Waiting Input(foo)

            #4: +90ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Waiting Input(foo) Executor(task=1, since=90ms)

            #5: +110ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Running(task=1)

            #6: +180ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Finished Success
              "foo_bar_baz" HardCompaction:
                Flow ID = 2 Waiting Input(foo_bar)

            #7: +180ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Finished Success
              "foo_bar_baz" HardCompaction:
                Flow ID = 2 Waiting Input(foo_bar) Executor(task=2, since=180ms)

            #8: +200ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Finished Success
              "foo_bar_baz" HardCompaction:
                Flow ID = 2 Running(task=2)

            #9: +240ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Finished Success
              "foo_bar_baz" HardCompaction:
                Flow ID = 2 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_keep_metadata_only_without_recursive_compaction() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;
    let foo_bar_baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar.baz"),
                account_name: None,
            },
            vec![foo_bar_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_compaction_rule(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT),
            FlowConfigRuleCompact::MetadataOnly { recursive: false },
        )
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener
        .define_dataset_display_name(foo_bar_baz_id.clone(), "foo_bar_baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                flow_configuration_snapshot_maybe: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 20ms, finish at 90ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some(
                  (
                    Duration::milliseconds(70),
                    TaskOutcome::Success(TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                      }
                    }.into_task_result())
                  )
                ),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(150)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, main_handle)
        } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #2: +20ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)

            #3: +90ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_keep_metadata_only_compaction_multiple_accounts() {
    let wasya = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("wasya"));
    let petya = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("petya"));

    let subject_wasya = CurrentAccountSubject::logged(wasya.get_id(), wasya.account_name.clone());
    let subject_petya = CurrentAccountSubject::logged(petya.get_id(), petya.account_name.clone());

    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: Some(subject_wasya.account_name().clone()),
        })
        .await;

    let foo_bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar"),
                account_name: Some(subject_wasya.account_name().clone()),
            },
            vec![foo_id.clone()],
        )
        .await;
    let foo_baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.baz"),
                account_name: Some(subject_petya.account_name().clone()),
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_compaction_rule(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT),
            FlowConfigRuleCompact::MetadataOnly { recursive: true },
        )
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener.define_dataset_display_name(foo_baz_id.clone(), "foo_baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 80ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                  Duration::milliseconds(70),
                  TaskOutcome::Success(TaskResultDatasetHardCompact {
                    compaction_result: CompactionResult::Success {
                      old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                      new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                      old_num_blocks: 5,
                      new_num_blocks: 4,
                    }
                  }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                flow_configuration_snapshot_maybe: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 1: "foo_bar" hard_compaction start running at 110ms, finish at 180ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_bar_id.clone()),
                run_since_start: Duration::milliseconds(110),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((
                  Duration::milliseconds(70),
                  TaskOutcome::Success(
                    TaskResultDatasetHardCompact {
                      compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                      }
                    }.into_task_result()
                ))),
                // Make sure we will take config from root dataset
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                  dataset_id: foo_bar_id.clone(),
                  max_slice_size: None,
                  max_slice_records: None,
                  keep_metadata_only: true,
                }.into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(400)).await;
            };

            tokio::join!(task0_handle, trigger0_handle, task1_handle, main_handle)
        } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #2: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)

            #3: +80ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Waiting Input(foo)

            #4: +80ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Waiting Input(foo) Executor(task=1, since=80ms)

            #5: +110ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Running(task=1)

            #6: +180ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo_bar" HardCompaction:
                Flow ID = 1 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_flow_configuration_paused_resumed_modified() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;
    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
        )
        .await;
    harness.eager_initialization().await;

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
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
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

                // 50ms: Pause both flow triggers in between completion 2 first tasks and queuing
                harness.advance_time(Duration::milliseconds(50)).await;
                harness.pause_flow(start_time + Duration::milliseconds(50), FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;
                harness.pause_flow(start_time + Duration::milliseconds(50), FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;

                // 80ms: Wake up after initially planned "foo" scheduling but before planned "bar" scheduling:
                //  - "foo":
                //    - gets resumed with previous period of 50ms
                //    - gets scheduled immediately at 80ms (waited >= 1 period)
                //  - "bar":
                //    - gets a trigger update for period of 70ms
                //    - get queued for 100ms (last success at 30ms + period of 70ms)
                harness.advance_time(Duration::milliseconds(30)).await;
                harness.resume_flow(start_time + Duration::milliseconds(80), FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;
                harness.set_flow_trigger(
                  start_time + Duration::milliseconds(80),
                  FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST),
                  FlowTriggerRule::Schedule(Duration::milliseconds(70).into()),
                ).await;
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
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=110ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #7: +50ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +80ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #9: +80ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=2, since=80ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #10: +100ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=3, since=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=2, since=80ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_respect_last_success_time_when_schedule_resumes() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

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
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
          // Task 0: "foo" start running at 10ms, finish at 20ms
          let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
          });
          let task0_handle = task0_driver.run();

          // Task 1: "bar" start running at 20ms, finish at 30ms
          let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
          });
          let task1_handle = task1_driver.run();

          // Main simulation script
          let main_handle = async {
              // Initially both "foo" is scheduled without waiting.
              // "foo":
              //  - flow 0: task 0 starts at 10ms, finishes at 20ms
              //  - next flow 2 queued for 120ms (20ms initiated + 100ms period)
              // "bar":
              //  - flow 1: task 1 starts at 20ms, finishes at 30ms
              //  - next flow 3 queued for 90ms (30ms initiated + 60ms period)

              // 50ms: Pause flow config before next flow runs
              harness.advance_time(Duration::milliseconds(50)).await;
              harness.pause_flow(start_time + Duration::milliseconds(50), FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;
              harness.pause_flow(start_time + Duration::milliseconds(50), FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;

              // 100ms: Wake up after initially planned "bar" scheduling but before planned "foo" scheduling:
              //  - "foo":
              //    - resumed with period 100ms
              //    - last success at 20ms
              //    - scheduled for 120ms (still wait a little bit since last success)
              //  - "bar":
              //    - resumed with period 60ms
              //    - last success at 30ms
              //    - gets scheduled immediately (waited longer than 30ms last success + 60ms period)
              harness.advance_time(Duration::milliseconds(50)).await;
              harness.resume_flow(start_time + Duration::milliseconds(100), FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;
              harness.resume_flow(start_time + Duration::milliseconds(100), FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;
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
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=90ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #7: +50ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +100ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #9: +100ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #10: +120ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=3, since=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

      "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;
    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(70).into()),
        )
        .await;
    harness.eager_initialization().await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false,
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            let main_handle = async {
                // 0ms: Both "foo" and "bar" are initially scheduled without waiting
                //  "foo":
                //   - flow 0 scheduled at 0ms
                //   - task 0 starts at 10ms, finishes at 20ms
                //   - flow 2 scheduled for 20ms + period = 70ms
                //  "bar":
                //   - flow 1 scheduled at 0ms
                //   - task 1 starts at 20ms, finishes at 30ms
                //   - flow 3 scheduled for 30ms + period = 100ms

                // 50ms: deleting "foo" in QUEUED state
                harness.advance_time(Duration::milliseconds(50)).await;
                harness.issue_dataset_deleted(&foo_id).await;

                // 120ms: deleting "bar" in SCHEDULED state
                harness.advance_time(Duration::milliseconds(70)).await;
                harness.issue_dataset_deleted(&bar_id).await;

                // 140ms: finish
                harness.advance_time(Duration::milliseconds(20)).await;
            };

            tokio::join!(task0_handle, task1_handle, main_handle)
         } => Ok(()),
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_task_completions_trigger_next_loop_on_success() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    let baz_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("baz"),
            account_name: None,
        })
        .await;

    for dataset_id in [&foo_id, &bar_id, &baz_id] {
        harness
            .set_flow_trigger(
                harness.now_datetime(),
                FlowBinding::new_dataset(dataset_id.clone(), FLOW_TYPE_DATASET_INGEST),
                FlowTriggerRule::Schedule(Duration::milliseconds(40).into()),
            )
            .await;
    }

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());
    test_flow_listener.define_dataset_display_name(baz_id.clone(), "baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms with failure
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Failed(TaskError::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 1: "baz" start running at 30ms, finish at 40ms with cancellation
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Cancelled)),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: baz_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                // 0ms: all 3 datasets are scheduled immediately without waiting:
                //  "foo":
                //   - flow 0 scheduled at 0ms
                //   - task 0 starts at 10ms, finishes at 20ms
                //   - next flow 3 scheduled for 20ms + period = 60ms
                //  "bar":
                //   - flow 1 scheduled at 0ms
                //   - task 1 starts at 20ms, finishes at 30ms with failure
                //   - next flow not scheduled
                //  "baz":
                //   - flow 2 scheduled at 0ms
                //   - task 2 starts at 30ms, finishes at 40ms with cancellation
                //   - next flow not scheduled

                // 80ms: the succeeded dataset schedule another update
                harness.advance_time(Duration::milliseconds(80)).await;
            };

            tokio::join!(task0_handle, task1_handle, task2_handle, main_handle)

         } => Ok(()),
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_derived_dataset_triggered_initially_and_after_input_change() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(MockDatasetIncrementQueryService::with_increment_since(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 3,
                updated_watermark: None,
            },
        )),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(BatchingRule::new_checked(1, Duration::seconds(1)).unwrap()),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((
                  Duration::milliseconds(10),
                  TaskOutcome::Success(
                    TaskResultDatasetUpdate {
                      pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                      },
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 110ms, finish at 120ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((
                  Duration::milliseconds(10),
                  TaskOutcome::Success(
                    TaskResultDatasetUpdate {
                      pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"newest-slice"),
                      },
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 130ms, finish at 140ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(130),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_throttling_manual_triggers() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS)),
        mandatory_throttling_period: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS * 10)),
        ..Default::default()
    });

    // Foo Flow
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST);

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

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
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
        // Manual trigger for "foo" at 20ms
        let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
            flow_binding: foo_flow_binding.clone(),
            run_since_start: Duration::milliseconds(20),
            initiator_id: None,
            flow_configuration_snapshot_maybe: None,
        });
        let trigger0_handle = trigger0_driver.run();

        // Manual trigger for "foo" at 30ms
        let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
            flow_binding: foo_flow_binding.clone(),
            run_since_start: Duration::milliseconds(30),
            initiator_id: None,
            flow_configuration_snapshot_maybe: None,
        });
        let trigger1_handle = trigger1_driver.run();

        // Manual trigger for "foo" at 70ms
        let trigger2_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
          flow_binding: foo_flow_binding,
          run_since_start: Duration::milliseconds(70),
          initiator_id: None,
          flow_configuration_snapshot_maybe: None,
        });
        let trigger2_handle = trigger2_driver.run();

        // Task 0: "foo" start running at 40ms, finish at 50ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(0),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(40),
            finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_throttling_derived_dataset_with_2_parents() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS)), // 10ms,
        mandatory_throttling_period: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS * 10)), /* 100ms */
        mock_dataset_changes: Some(MockDatasetIncrementQueryService::with_increment_since(
            DatasetIntervalIncrement {
                num_blocks: 2,
                num_records: 7,
                updated_watermark: None,
            },
        )),
    });

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    let baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("baz"),
                account_name: None,
            },
            vec![foo_id.clone(), bar_id.clone()],
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(150).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(baz_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(BatchingRule::new_checked(1, Duration::hours(24)).unwrap()),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());
    test_flow_listener.define_dataset_display_name(baz_id.clone(), "baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
      // Run API service
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
        // Task 0: "foo" start running at 10ms, finish at 20ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(0),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(10),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(20),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"fbar-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "baz" start running at 30ms, finish at 50ms (simulate longer run)
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::milliseconds(30),
            finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Success(TaskResult::empty()))),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: baz_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task2_handle = task2_driver.run();

        // Task 3: "foo" start running at 130ms, finish at 140ms
        let task3_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(3),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(130),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                      old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                      new_head: odf::Multihash::from_digest_sha3_256(b"foo-newest-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task3_handle = task3_driver.run();

        // Task 4: "baz" start running at 160ms, finish at 170ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "5")]),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::milliseconds(160),
            finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: baz_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task4_handle = task4_driver.run();

        // Task 5: "bar" start running at 190ms, finish at 200ms
        let task5_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(5),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(190),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-newest-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task5_handle = task5_driver.run();

        // Main simulation script
        let main_handle = async {
          // Stage 0: initial auto-polling
          //  - all 3 datasets auto-polled at 0ms, flows 0,1,2 correspondingly
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
          //     - no continuation scheduled

          // Stage 1: foo runs next flow
          //  - foo:
          //     - flow 3 scheduled at 120ms
          //     - task 3 starts at 130ms, finishes at 140ms, flow 3 completes at 140ms
          //     - baz flow 5 scheduled as derived for 150ms: max(140ms initiated, last attempt 50ms + throttling 100ms)
          //     - foo flow 6 scheduled for 240ms: 140ms initiated + max(period 50ms, throttling 100ms)

          // Stage 2: baz executes triggered by foo
          //  - baz:
          //     - flow 5 scheduled at 150ms
          //     - task 4 starts at 160ms, finishes at 170ms, flow 5 completes at 170ms
          //     - no continuation scheduled

          // Stage 3: bar runs next flow
          // - bar
          //   - flow 4 schedules at 180ms
          //   - task 5 starts at 190ms, finishes at 200ms, flow 4 completes at 200ms
          //   - baz flow 7 scheduled as derived for 270ms: max(200ms initiated, last attempt 170ms + hrottling 100ms)
          //   - bar flow 8 scheduled for 350ms: 200ms initiated + max (period 150ms, throttling 100ms)

          harness.advance_time(Duration::milliseconds(400)).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task3_handle, task4_handle, task5_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_records_reached() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
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
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(
                BatchingRule::new_checked(10, Duration::milliseconds(120)).unwrap(),
            ),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
      // Run API service
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
        // Task 0: "foo" start running at 10ms, finish at 20ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(0),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(10),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(20),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "foo" start running at 80ms, finish at 90ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(80),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task2_handle = task2_driver.run();

        // Task 3: "foo" start running at 150ms, finish at 160ms
        let task3_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(3),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(150),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-3"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task3_handle = task3_driver.run();

        // Task 4: "bar" start running at 170ms, finish at 180ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(170),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task4_handle = task4_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::milliseconds(400)).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task3_handle, task4_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_timeout() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
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
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(
                BatchingRule::new_checked(10, Duration::milliseconds(150)).unwrap(),
            ),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
      // Run API service
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
        // Task 0: "foo" start running at 10ms, finish at 20ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(0),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(10),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(20),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "foo" start running at 80ms, finish at 90ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(80),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task2_handle = task2_driver.run();

        // Task 3 is scheduled, but never runs

        // Task 4: "bar" start running at 250ms, finish at 2560ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(250),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task4_handle = task4_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::milliseconds(400)).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task4_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_watermark() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
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
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(40).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(
                BatchingRule::new_checked(10, Duration::milliseconds(200)).unwrap(),
            ),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
      // Run API service
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
        // Task 0: "foo" start running at 10ms, finish at 20ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(0),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(10),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(20),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "foo" start running at 70ms, finish at 80ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(70),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task2_handle = task2_driver.run();

        // Task 3 is scheduled, but never runs

        // Task 4: "bar" start running at 290ms, finish at 300ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(290),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task4_handle = task4_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::milliseconds(400)).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task4_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_condition_with_2_inputs() {
    let mut seq = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
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
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: None,
        })
        .await;

    let baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("baz"),
                account_name: None,
            },
            vec![foo_id.clone(), bar_id.clone()],
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(120).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(baz_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(
                BatchingRule::new_checked(15, Duration::milliseconds(200)).unwrap(),
            ),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());
    test_flow_listener.define_dataset_display_name(baz_id.clone(), "baz".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
      // Run API service
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
        // Task 0: "foo" start running at 10ms, finish at 20ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(0),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(10),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task0_handle = task0_driver.run();

        // Task 1: "bar" start running at 20ms, finish at 30ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(20),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task1_handle = task1_driver.run();

        // Task 2: "baz" start running at 30ms, finish at 40ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(2),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::milliseconds(30),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"baz-old-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"baz-new-slice"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: baz_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task2_handle = task2_driver.run();

        // Task 3: "foo" start running at 110ms, finish at 120ms
        let task3_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(3),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(110),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                  }
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task3_handle = task3_driver.run();

        // Task 4: "bar" start running at 160ms, finish at 170ms
        let task4_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(4),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
            dataset_id: Some(bar_id.clone()),
            run_since_start: Duration::milliseconds(160),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"bar-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: bar_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task4_handle = task4_driver.run();

        // Task 5: "foo" start running at 210ms, finish at 220ms
        let task5_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(5),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "6")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(210),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task5_handle = task5_driver.run();

        // Task 6: "baz" start running at 230ms, finish at 240ms
        let task6_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(6),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "5")]),
            dataset_id: Some(baz_id.clone()),
            run_since_start: Duration::milliseconds(230),
            finish_in_with: Some((
              Duration::milliseconds(10),
              TaskOutcome::Success(
                TaskResultDatasetUpdate {
                  pull_result: PullResult::Updated {
                    old_head: Some(odf::Multihash::from_digest_sha3_256(b"baz-new-slice")),
                    new_head: odf::Multihash::from_digest_sha3_256(b"baz-new-slice-2"),
                  },
                }.into_task_result()
              )
            )),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: baz_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });
        let task6_handle = task6_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::milliseconds(400)).await;
        };

        tokio::join!(task0_handle, task1_handle, task2_handle, task3_handle, task4_handle, task5_handle, task6_handle, main_handle)
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_all_flow_initiators() {
    let foo = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("foo"));
    let bar = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("bar"));

    let subject_foo = CurrentAccountSubject::logged(foo.get_id(), foo.account_name.clone());
    let subject_bar = CurrentAccountSubject::logged(bar.get_id(), bar.account_name.clone());

    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: Some(subject_foo.account_name().clone()),
        })
        .await;

    let foo_account_id = odf::AccountID::new_seeded_ed25519(subject_foo.account_name().as_bytes());
    let bar_account_id = odf::AccountID::new_seeded_ed25519(subject_bar.account_name().as_bytes());

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: Some(subject_bar.account_name().clone()),
        })
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);
    let bar_flow_binding = FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                  // Task 0: "foo" start running at 10ms, finish at 20ms
                  let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetHardCompact {
                      dataset_id: foo_id.clone(),
                      max_slice_size: None,
                      max_slice_records: None,
                      keep_metadata_only: false,
                    }.into_logical_plan(),
                });
                let task0_handle = task0_driver.run();

                let task1_driver = harness.task_driver(TaskDriverArgs {
                  task_id: TaskID::new(1),
                  task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                  dataset_id: Some(bar_id.clone()),
                  run_since_start: Duration::milliseconds(60),
                  finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                  expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: bar_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                    keep_metadata_only: false,
                  }.into_logical_plan(),
                });
                let task1_handle = task1_driver.run();

                // Manual trigger for "foo" at 10ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: foo_flow_binding,
                    run_since_start: Duration::milliseconds(10),
                    initiator_id: Some(foo_account_id.clone()),
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 50ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: bar_flow_binding,
                    run_since_start: Duration::milliseconds(50),
                    initiator_id: Some(bar_account_id.clone()),
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger1_handle = trigger1_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Moment 10ms - manual foo trigger happens here:
                    //  - flow 0 gets trigger and finishes at 30ms

                    // Moment 50ms - manual foo trigger happens here:
                    //  - flow 1 trigger and finishes
                    //  - task 1 starts at 60ms, finishes at 70ms (leave some gap to fight with random order)

                    harness.advance_time(Duration::milliseconds(100)).await;
                };

                tokio::join!(task0_handle, task1_handle, trigger0_handle, trigger1_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    let foo_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_all_flow_initiators_by_dataset(&foo_id)
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!([foo_account_id.clone()], *foo_dataset_initiators_list);

    let bar_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_all_flow_initiators_by_dataset(&bar_id)
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!([bar_account_id.clone()], *bar_dataset_initiators_list);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_all_datasets_with_flow() {
    let foo = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("foo"));
    let bar = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("bar"));

    let subject_foo = CurrentAccountSubject::logged(foo.get_id(), foo.account_name.clone());
    let subject_bar = CurrentAccountSubject::logged(bar.get_id(), bar.account_name.clone());

    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: Some(subject_foo.account_name().clone()),
        })
        .await;

    let _foo_bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.bar"),
                account_name: Some(subject_foo.account_name().clone()),
            },
            vec![foo_id.clone()],
        )
        .await;

    let foo_account_id = odf::AccountID::new_seeded_ed25519(subject_foo.account_name().as_bytes());
    let bar_account_id = odf::AccountID::new_seeded_ed25519(subject_bar.account_name().as_bytes());

    let bar_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("bar"),
            account_name: Some(subject_bar.account_name().clone()),
        })
        .await;

    harness.eager_initialization().await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_COMPACT);
    let bar_flow_binding = FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_COMPACT);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                  // Task 0: "foo" start running at 10ms, finish at 20ms
                  let task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetHardCompact {
                      dataset_id: foo_id.clone(),
                      max_slice_size: None,
                      max_slice_records: None,
                      keep_metadata_only: false,
                    }.into_logical_plan(),
                });
                let task0_handle = task0_driver.run();

                let task1_driver = harness.task_driver(TaskDriverArgs {
                  task_id: TaskID::new(1),
                  task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                  dataset_id: Some(bar_id.clone()),
                  run_since_start: Duration::milliseconds(60),
                  finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                  expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: bar_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                    keep_metadata_only: false,
                  }.into_logical_plan(),
                });
                let task1_handle = task1_driver.run();

                // Manual trigger for "foo" at 10ms
                let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: foo_flow_binding,
                    run_since_start: Duration::milliseconds(10),
                    initiator_id: Some(foo_account_id.clone()),
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger0_handle = trigger0_driver.run();

                // Manual trigger for "bar" at 50ms
                let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
                    flow_binding: bar_flow_binding,
                    run_since_start: Duration::milliseconds(50),
                    initiator_id: Some(bar_account_id.clone()),
                    flow_configuration_snapshot_maybe: None,
                });
                let trigger1_handle = trigger1_driver.run();

                // Main simulation script
                let main_handle = async {
                    // Moment 10ms - manual foo trigger happens here:
                    //  - flow 0 gets trigger and finishes at 30ms

                    // Moment 50ms - manual foo trigger happens here:
                    //  - flow 1 trigger and finishes
                    //  - task 1 starts at 60ms, finishes at 70ms (leave some gap to fight with random order)

                    harness.advance_time(Duration::milliseconds(100)).await;
                };

                tokio::join!(task0_handle, task1_handle, trigger0_handle, trigger1_handle, main_handle)
            } => Ok(())
    }
    .unwrap();

    let foo_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_all_flow_initiators_by_dataset(&foo_id)
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!([foo_account_id.clone()], *foo_dataset_initiators_list);

    let bar_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_all_flow_initiators_by_dataset(&bar_id)
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!([bar_account_id.clone()], *bar_dataset_initiators_list);

    let all_datasets_with_flow: Vec<_> = harness
        .flow_query_service
        .list_all_datasets_with_flow_by_account(&foo_account_id)
        .await
        .unwrap();

    pretty_assertions::assert_eq!([foo_id], *all_datasets_with_flow);

    let all_datasets_with_flow: Vec<_> = harness
        .flow_query_service
        .list_all_datasets_with_flow_by_account(&bar_account_id)
        .await
        .unwrap();

    pretty_assertions::assert_eq!([bar_id], *all_datasets_with_flow);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_abort_flow_before_scheduling_tasks() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 100ns
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
        )
        .await;
    harness.eager_initialization().await;

    // Run scheduler concurrently with manual aborts script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: start running at 10ms, finish at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Manual abort for "foo" at 80ms
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(1),
                abort_since_start: Duration::milliseconds(80),
            });
            let abort0_handle = abort0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, abort0_handle, sim_handle)
        } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #4: +80ms:
              "foo" Ingest:
                Flow ID = 1 Finished Aborted
                Flow ID = 0 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_abort_flow_after_scheduling_still_waiting_for_executor() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 50ms
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;
    harness.eager_initialization().await;

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: start running at 10ms, finish at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Manual abort for "foo" at 90ms
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(1),
                abort_since_start: Duration::milliseconds(90),
            });
            let abort0_handle = abort0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, abort0_handle, sim_handle)
        } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #4: +70ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=70ms)
                Flow ID = 0 Finished Success

            #5: +90ms:
              "foo" Ingest:
                Flow ID = 1 Finished Aborted
                Flow ID = 0 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_abort_flow_after_task_running_has_started() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 50ms
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;
    harness.eager_initialization().await;

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: start running at 10ms, finish at 110ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(100), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Manual abort for "foo" at 50ms, which is within task run period
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(0),
                abort_since_start: Duration::milliseconds(50),
            });
            let abort0_handle = abort0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, abort0_handle, sim_handle)
        } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
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

            #3: +50ms:
              "foo" Ingest:
                Flow ID = 0 Finished Aborted

          "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_abort_flow_after_task_finishes() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 50ms
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
        )
        .await;
    harness.eager_initialization().await;

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: start running at 10ms, finish at 30ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Task 1: start running at 90ms, finish at 110ms
            let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(90),
                finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let foo_task1_handle = foo_task1_driver.run();

            // Manual abort for "foo" at 50ms, which is after flow 0 has finished, and before flow 1 has started
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(0),
                abort_since_start: Duration::milliseconds(50),
            });
            let abort0_handle = abort0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, foo_task1_handle, abort0_handle, sim_handle)
        } => Ok(())
    }
    .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
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

            #3: +30ms:
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

            #6: +110ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_respect_last_success_time_when_activate_configuration() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;
    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(BatchingRule::new_checked(1, Duration::seconds(10)).unwrap()),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

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
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
          // Task 0: "foo" start running at 10ms, finish at 20ms
          let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
          });
          let task0_handle = task0_driver.run();

          // Task 1: "bar" start running at 20ms, finish at 30ms
          let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
          });
          let task1_handle = task1_driver.run();

          // Main simulation script
          let main_handle = async {
              // Initially both "foo" and "bar are scheduled without waiting.
              // "foo":
              //  - flow 0: task 0 starts at 10ms, finishes at 20ms
              //  - next flow 2 queued for 120ms (20ms initiated + 100ms period)
              // "bar":
              //  - flow 1: task 1 starts at 20ms, finishes at 30ms
              //  - next flow 3 queued for 90ms (30ms initiated + 60ms period)

              // 50ms: Pause flow config before next flow runs
              harness.advance_time(Duration::milliseconds(50)).await;
              harness.pause_flow(start_time + Duration::milliseconds(50), FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;
              harness.pause_flow(start_time + Duration::milliseconds(50), FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM)).await;

              // 100ms: Wake up after initially planned "bar" scheduling but before planned "foo" scheduling:
              //  - "foo":
              //    - resumed with period 100ms
              //    - last success at 20ms
              //    - scheduled for 120ms (still wait a little bit since last success)
              //  - "bar":
              //    - resumed with period 60ms
              //    - last success at 30ms
              //    - gets scheduled immediately (waited longer than 30ms last success + 60ms period)
              harness.advance_time(Duration::milliseconds(50)).await;
              harness.resume_flow(start_time + Duration::milliseconds(100), FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST)).await;
              harness.resume_flow(start_time + Duration::milliseconds(100), FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM)).await;
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
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #4: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #5: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #6: +50ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #7: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting AutoPolling Batching(1, until=10100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #8: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting AutoPolling Batching(1, until=10100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=2, since=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

      "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_disable_trigger_on_flow_fail() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset, and configure ingestion schedule every 60ms
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness
        .set_dataset_flow_ingest(
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
            },
        )
        .await;

    let trigger_rule = FlowTriggerRule::Schedule(Duration::milliseconds(60).into());

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            trigger_rule.clone(),
        )
        .await;
    harness.eager_initialization().await;

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
                // Task 0: start running at 10ms, finish at 20ms
                let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(0),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(10),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let foo_task0_handle = foo_task0_driver.run();

                // Task 1: start running at 90ms, finish at 100ms
                let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                    task_id: TaskID::new(1),
                    task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                    dataset_id: Some(foo_id.clone()),
                    run_since_start: Duration::milliseconds(90),
                    finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Failed(TaskError::empty()))),
                    expected_logical_plan: LogicalPlanDatasetUpdate {
                      dataset_id: foo_id.clone(),
                      fetch_uncacheable: false
                    }.into_logical_plan(),
                });
                let foo_task1_handle = foo_task1_driver.run();

                // Main simulation boundary - 120ms total
                //  - "foo" should immediately schedule "task 0", since "foo" has never run yet
                //  - "task 0" will take action and complete, this will schedule the next flow
                //    run for "foo" after full period
                //  - when that period is over, "task 1" should be scheduled
                //  - "task 1" will take action and complete
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
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            "#
        )
    );

    let foo_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST);

    let current_trigger = harness
        .flow_trigger_service
        .find_trigger(&foo_binding)
        .await
        .unwrap();
    assert_eq!(
        current_trigger,
        Some(FlowTriggerState {
            flow_binding: foo_binding,
            status: FlowTriggerStatus::PausedTemporarily,
            rule: trigger_rule,
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_trigger_enable_during_flow_throttling() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS)),
        mandatory_throttling_period: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS * 20)),
        ..Default::default()
    });

    // Foo Flow
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST);

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

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
      res = harness.flow_agent.run() => res.int_err(),

      // Run simulation script and task drivers
      _ = async {
        // Manual trigger for "foo" at 20ms
        let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
            flow_binding: foo_flow_binding.clone(),
            run_since_start: Duration::milliseconds(20),
            initiator_id: None,
            flow_configuration_snapshot_maybe: None,
        });
        let trigger0_handle = trigger0_driver.run();

        // Manual trigger for "foo" at 30ms
        let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
            flow_binding: foo_flow_binding.clone(),
            run_since_start: Duration::milliseconds(30),
            initiator_id: None,
            flow_configuration_snapshot_maybe: None,
        });
        let trigger1_handle = trigger1_driver.run();

        // Manual trigger for "foo" at 70ms
        let trigger2_driver = harness.manual_flow_trigger_driver(ManualFlowTriggerArgs {
          flow_binding: foo_flow_binding,
          run_since_start: Duration::milliseconds(70),
          initiator_id: None,
          flow_configuration_snapshot_maybe: None,
        });
        let trigger2_handle = trigger2_driver.run();

        // Task 0: "foo" start running at 40ms, finish at 50ms
        let task0_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(0),
          task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
          dataset_id: Some(foo_id.clone()),
          run_since_start: Duration::milliseconds(40),
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
          expected_logical_plan: LogicalPlanDatasetUpdate {
            dataset_id: foo_id.clone(),
            fetch_uncacheable: false
          }.into_logical_plan(),
        });

        // Task 1: "foo" start running at 250ms, finish at 260ms
        let task1_driver = harness.task_driver(TaskDriverArgs {
            task_id: TaskID::new(1),
            task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
            dataset_id: Some(foo_id.clone()),
            run_since_start: Duration::milliseconds(250),
            finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
            expected_logical_plan: LogicalPlanDatasetUpdate {
              dataset_id: foo_id.clone(),
              fetch_uncacheable: false
            }.into_logical_plan(),
        });

        // Task 2: "foo" start running at 260ms, finish at 160ms
        let task2_driver = harness.task_driver(TaskDriverArgs {
          task_id: TaskID::new(2),
          task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
          dataset_id: Some(foo_id.clone()),
          run_since_start: Duration::milliseconds(520),
          finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
          expected_logical_plan: LogicalPlanDatasetUpdate {
            dataset_id: foo_id.clone(),
            fetch_uncacheable: false
          }.into_logical_plan(),
      });

        let task0_handle = task0_driver.run();
        let task1_handle = task1_driver.run();
        let task2_handle = task2_driver.run();

        // Main simulation script
        let main_handle = async {
          harness.advance_time(Duration::milliseconds(100)).await;
          test_flow_listener
              .make_a_snapshot(start_time + Duration::milliseconds(100))
              .await;
          harness
            .set_flow_trigger(
                harness.now_datetime(),
                FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
                FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
            )
            .await;

          harness.advance_time(Duration::milliseconds(1000)).await;
        };

        tokio::join!(
          trigger0_handle, trigger1_handle, trigger2_handle, task0_handle, task1_handle, task2_handle, main_handle
        )
      } => Ok(())
    }
    .unwrap();

    pretty_assertions::assert_eq!(
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
            Flow ID = 1 Waiting Manual Throttling(for=200ms, wakeup=250ms, shifted=70ms)
            Flow ID = 0 Finished Success

        #5: +250ms:
          "foo" Ingest:
            Flow ID = 1 Waiting Manual Executor(task=1, since=250ms)
            Flow ID = 0 Finished Success

        #6: +250ms:
          "foo" Ingest:
            Flow ID = 1 Running(task=1)
            Flow ID = 0 Finished Success

        #7: +260ms:
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Throttling(for=200ms, wakeup=460ms, shifted=320ms)
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #8: +460ms:
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=460ms)
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #9: +520ms:
          "foo" Ingest:
            Flow ID = 2 Running(task=2)
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #10: +530ms:
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Throttling(for=200ms, wakeup=730ms, shifted=590ms)
            Flow ID = 2 Finished Success
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #11: +730ms:
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Executor(task=3, since=730ms)
            Flow ID = 2 Finished Success
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

      "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dependencies_flow_trigger_instantly_with_zero_batching_rule() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(MockDatasetIncrementQueryService::with_increment_since(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 0,
                updated_watermark: None,
            },
        )),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(foo_id.clone(), FLOW_TYPE_DATASET_INGEST),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now_datetime(),
            FlowBinding::new_dataset(bar_id.clone(), FLOW_TYPE_DATASET_TRANSFORM),
            FlowTriggerRule::Batching(BatchingRule::new_checked(0, Duration::seconds(0)).unwrap()),
        )
        .await;

    // Enforce dependency graph initialization
    harness.eager_initialization().await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with manual triggers script
    tokio::select! {
        // Run API service
        res = harness.flow_agent.run() => res.int_err(),

        // Run simulation script and task drivers
        _ = async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((
                  Duration::milliseconds(10),
                  TaskOutcome::Success(
                    TaskResultDatasetUpdate {
                      pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                      },
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 110ms, finish at 120ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                // Send some PullResult with records to bypass batching condition
                finish_in_with: Some((
                  Duration::milliseconds(10),
                  TaskOutcome::Success(
                    TaskResultDatasetUpdate {
                      pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"newest-slice"),
                      },
                    }.into_task_result()
                  )
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: foo_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 130ms, finish at 140ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(130),
                finish_in_with: Some((Duration::milliseconds(10), TaskOutcome::Success(TaskResult::empty()))),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                  dataset_id: bar_id.clone(),
                  fetch_uncacheable: false
                }.into_logical_plan(),
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
                Flow ID = 3 Waiting Input(foo) Batching(0, until=120ms)
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
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO next:
//  - derived more than 1 level
