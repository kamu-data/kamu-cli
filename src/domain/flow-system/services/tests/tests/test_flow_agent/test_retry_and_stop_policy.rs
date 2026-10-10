// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;
use kamu_adapter_flow_dataset::*;
use kamu_adapter_task_dataset::*;
use kamu_flow_system::*;
use kamu_task_system::*;

use crate::tests::{FlowHarness, FlowSystemTestListener, ManualFlowActivationArgs, TaskDriverArgs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_disable_trigger_on_flow_fail_default() {
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
            ingest_dataset_binding(&foo_id),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            None, // No retry policy
        )
        .await;

    let trigger_rule = FlowTriggerRule::Schedule(Duration::milliseconds(60).into());

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            trigger_rule.clone(),
            FlowTriggerStopPolicy::default(), // Default policy is to pause on first failure
        )
        .await;

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 10ms, finish at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Task 1: start running at 90ms, finish at 100ms
            let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(90),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task1_handle = foo_task1_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(200));
            tokio::join!(foo_task0_handle, foo_task1_handle, sim_handle);
        })
        .await
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
                Flow ID = 0 Finished Success

            #4: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #5: +80ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=80ms)
                Flow ID = 0 Finished Success

            #6: +90ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #7: +100ms:
              "foo" Ingest:
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            "#
        )
    );

    // The trigger should be paused after first failure
    let foo_binding = ingest_dataset_binding(&foo_id);
    let trigger_status = harness.get_flow_trigger_status(&foo_binding).await;
    assert_eq!(
        trigger_status,
        Some(FlowTriggerStatus::StoppedAutomatically)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_disable_trigger_on_flow_fail_consecutive3() {
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
            ingest_dataset_binding(&foo_id),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            None, // No retry policy
        )
        .await;

    let trigger_rule = FlowTriggerRule::Schedule(Duration::milliseconds(60).into());

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            trigger_rule.clone(),
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        )
        .await;

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 10ms, finish at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Task 1: start running at 90ms, finish at 100ms
            let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(90),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task1_handle = foo_task1_driver.run();

            // Task 2: start running at 160ms, finish at 170ms
            let foo_task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(160),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task2_handle = foo_task2_driver.run();

            // Task 3: start running at 240ms, finish at 250ms
            let foo_task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(240),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task3_handle = foo_task3_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(300));
            tokio::join!(
                foo_task0_handle,
                foo_task1_handle,
                foo_task2_handle,
                foo_task3_handle,
                sim_handle
            );
        })
        .await
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
                Flow ID = 0 Finished Success

            #4: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #5: +80ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=80ms)
                Flow ID = 0 Finished Success

            #6: +90ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #7: +100ms:
              "foo" Ingest:
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #8: +100ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #9: +160ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=160ms)
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #10: +160ms:
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #11: +170ms:
              "foo" Ingest:
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #12: +170ms:
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=230ms)
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #13: +230ms:
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=230ms)
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #14: +240ms:
              "foo" Ingest:
                Flow ID = 3 Running(task=3)
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #15: +250ms:
              "foo" Ingest:
                Flow ID = 3 Finished Failed
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            "#
        )
    );

    // The trigger should be paused after 3 consecutive failures
    let foo_binding = ingest_dataset_binding(&foo_id);
    let trigger_status = harness.get_flow_trigger_status(&foo_binding).await;
    assert_eq!(
        trigger_status,
        Some(FlowTriggerStatus::StoppedAutomatically)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_disable_trigger_on_flow_fail_skipped() {
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
            ingest_dataset_binding(&foo_id),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            None, // No retry policy
        )
        .await;

    let trigger_rule = FlowTriggerRule::Schedule(Duration::milliseconds(60).into());

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            trigger_rule.clone(),
            FlowTriggerStopPolicy::Never,
        )
        .await;

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 10ms, finish at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Task 1: start running at 90ms, finish at 100ms
            let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(90),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task1_handle = foo_task1_driver.run();

            // Task 2: start running at 170, finish at 180ms
            let foo_task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(170),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task2_handle = foo_task2_driver.run();

            // Task 3: start running at 250ms, finish at 260ms
            let foo_task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(250),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task3_handle = foo_task3_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(300));
            tokio::join!(
                foo_task0_handle,
                foo_task1_handle,
                foo_task2_handle,
                foo_task3_handle,
                sim_handle
            );
        })
        .await
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
                Flow ID = 0 Finished Success

            #4: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #5: +80ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=80ms)
                Flow ID = 0 Finished Success

            #6: +90ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #7: +100ms:
              "foo" Ingest:
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #8: +100ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #9: +160ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=160ms)
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #10: +170ms:
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #11: +180ms:
              "foo" Ingest:
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #12: +180ms:
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=240ms)
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #13: +240ms:
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=240ms)
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #14: +250ms:
              "foo" Ingest:
                Flow ID = 3 Running(task=3)
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #15: +260ms:
              "foo" Ingest:
                Flow ID = 3 Finished Failed
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            #16: +260ms:
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=320ms)
                Flow ID = 3 Finished Failed
                Flow ID = 2 Finished Failed
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            "#
        )
    );

    // The trigger should remain active despite failures
    let foo_binding = ingest_dataset_binding(&foo_id);
    let trigger_status = harness.get_flow_trigger_status(&foo_binding).await;
    assert_eq!(trigger_status, Some(FlowTriggerStatus::Active));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_disable_trigger_on_flow_fail_ignores_stop_policy_for_unrecoverable_error() {
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
            ingest_dataset_binding(&foo_id),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            None, // No retry policy
        )
        .await;

    let trigger_rule = FlowTriggerRule::Schedule(Duration::milliseconds(60).into());

    // Consecutive 3 failures as a policy
    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            trigger_rule.clone(),
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        )
        .await;

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 10ms, finish at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Task 1: start running at 90ms, finish at 100ms
            let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(90),
                // Important: unrecoverable error in the result
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_unrecoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task1_handle = foo_task1_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(300));
            tokio::join!(foo_task0_handle, foo_task1_handle, sim_handle);
        })
        .await
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
                Flow ID = 0 Finished Success

            #4: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #5: +80ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=80ms)
                Flow ID = 0 Finished Success

            #6: +90ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #7: +100ms:
              "foo" Ingest:
                Flow ID = 1 Finished Failed
                Flow ID = 0 Finished Success

            "#
        )
    );

    // The trigger should be paused after 1st failure (as error is unrecoverable)
    let foo_binding = ingest_dataset_binding(&foo_id);
    let trigger_status = harness.get_flow_trigger_status(&foo_binding).await;
    assert_eq!(
        trigger_status,
        Some(FlowTriggerStatus::StoppedAutomatically)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_ingest_with_retry_policy_success_at_last_attempt() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            Some(RetryPolicy {
                max_attempts: 2,
                min_delay_seconds: 1,
                backoff_type: RetryBackoffType::Fixed,
            }),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Manual trigger for "foo" at 20ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(20),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 30ms, fail at 40ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo" start running at 1040ms, fail at 1050ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(1040),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 2050ms, success at 2070ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(2050),
                finish_in_with: Some((
                    Duration::milliseconds(20),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(2100)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                trigger0_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual

            #2: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #3: +30ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +40ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=1040ms)

            #5: +1040ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=1040ms) Executor(task=1, since=1040ms)

            #6: +1040ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0,1)

            #7: +1050ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=2050ms)

            #8: +2050ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=2050ms) Executor(task=2, since=2050ms)

            #9: +2050ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0,1,2)

            #10: +2070ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    // Planned at 20ms, completed at 2070ms, after 2 retries
    let ingest_flow_type = ingest_dataset_binding(&foo_id).flow_type;
    assert_eq!(harness.completed_flows(&ingest_flow_type, "success"), 1);
    assert_eq!(harness.completed_flows(&ingest_flow_type, "failed"), 0);
    harness.assert_completed_flows_duration_seconds(&ingest_flow_type, "success", 2.05);
    assert_eq!(
        harness.completed_flows_retried_at_most(&ingest_flow_type, "success", 1),
        0
    );
    assert_eq!(
        harness.completed_flows_retried_at_most(&ingest_flow_type, "success", 2),
        1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_ingest_with_retry_policy_failure_after_all_attempts() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            Some(RetryPolicy {
                max_attempts: 2,
                min_delay_seconds: 1,
                backoff_type: RetryBackoffType::Fixed,
            }),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Manual trigger for "foo" at 20ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(20),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 30ms, fail at 40ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo" start running at 1040ms, fail at 1050ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(1040),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 2050ms, fail at 2060ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(2050),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(2100)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                trigger0_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual

            #2: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #3: +30ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +40ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=1040ms)

            #5: +1040ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=1040ms) Executor(task=1, since=1040ms)

            #6: +1040ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0,1)

            #7: +1050ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=2050ms)

            #8: +2050ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=2050ms) Executor(task=2, since=2050ms)

            #9: +2050ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0,1,2)

            #10: +2060ms:
              "foo" Ingest:
                Flow ID = 0 Finished Failed

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    // Retries do not complete the flow: only the final failure counts
    let ingest_flow_type = ingest_dataset_binding(&foo_id).flow_type;
    assert_eq!(harness.completed_flows(&ingest_flow_type, "failed"), 1);
    assert_eq!(harness.completed_flows(&ingest_flow_type, "success"), 0);
    assert_eq!(
        harness.completed_flows_retried_at_most(&ingest_flow_type, "failed", 1),
        0
    );
    assert_eq!(
        harness.completed_flows_retried_at_most(&ingest_flow_type, "failed", 2),
        1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_ingest_with_retry_policy_ignored_on_unrecoverable_error() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            Some(RetryPolicy {
                max_attempts: 2,
                min_delay_seconds: 1,
                backoff_type: RetryBackoffType::Fixed,
            }),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Manual trigger for "foo" at 20ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(20),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 30ms, fail at 40ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(30),
                // IMPORTANT: unrecoverable error in the results
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_unrecoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(200)).await;
            };

            tokio::join!(task0_handle, trigger0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual

            #2: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #3: +30ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +40ms:
              "foo" Ingest:
                Flow ID = 0 Finished Failed

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_retry_planned_before_restart_happens_after_restart() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            Some(RetryPolicy {
                max_attempts: 2,
                min_delay_seconds: 1,
                backoff_type: RetryBackoffType::Fixed,
            }),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // First run: the task fails, a retry is planned at 1040ms
    harness
        .simulate_flow_scenario(|| async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(20),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(100)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, main_handle);
        })
        .await
        .unwrap();

    // After a restart, only the stored retry time brings the flow back
    harness
        .simulate_flow_scenario(|| async {
            // Task 1: the retry at 1040ms, runs at 1050ms, succeeds at 1060ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(950),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            let main_handle = async {
                // Restarted at 100ms: the retry is not due before 1040ms
                harness.advance_time(Duration::milliseconds(930)).await;
                assert!(!harness.task_exists(TaskID::new(1)).await);

                // It's due right at 1040ms
                harness.advance_time(Duration::milliseconds(10)).await;
                assert!(harness.task_exists(TaskID::new(1)).await);

                harness.advance_time(Duration::milliseconds(50)).await;
            };

            tokio::join!(task1_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual

            #2: +20ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #3: +30ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +40ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=1040ms)

            #5: +100ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=1040ms)

            #6: +1040ms:
              "foo" Ingest:
                Flow ID = 0 Retrying(scheduled_at=1040ms) Executor(task=1, since=1040ms)

            #7: +1050ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0,1)

            #8: +1060ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_stricter_stop_policy_stops_trigger_at_once() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_flow_binding = ingest_dataset_binding(&foo_id);

    let trigger_rule = FlowTriggerRule::Schedule(Duration::milliseconds(60).into());
    harness
        .set_flow_trigger(
            harness.now(),
            foo_flow_binding.clone(),
            trigger_rule.clone(),
            FlowTriggerStopPolicy::AfterConsecutiveFailures {
                failures_count: ConsecutiveFailuresCount::try_new(3).unwrap(),
            },
        )
        .await;

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 10ms, fail at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Task 1: start running at 90ms, fail at 100ms
            let foo_task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(90),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task1_handle = foo_task1_driver.run();

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(120)).await;
                pretty_assertions::assert_eq!(
                    Some(FlowTriggerStatus::Active),
                    harness.get_flow_trigger_status(&foo_flow_binding).await
                );

                // 2 failures are already enough under the new policy
                harness
                    .set_flow_trigger(
                        harness.now(),
                        foo_flow_binding.clone(),
                        trigger_rule.clone(),
                        FlowTriggerStopPolicy::AfterConsecutiveFailures {
                            failures_count: ConsecutiveFailuresCount::try_new(2).unwrap(),
                        },
                    )
                    .await;

                harness.advance_time(Duration::milliseconds(10)).await;
                pretty_assertions::assert_eq!(
                    Some(FlowTriggerStatus::StoppedAutomatically),
                    harness.get_flow_trigger_status(&foo_flow_binding).await
                );

                // The flow waiting for 160ms is aborted, and never forms a task
                harness.advance_time(Duration::milliseconds(70)).await;
                assert!(!harness.task_exists(TaskID::new(2)).await);
            };

            tokio::join!(foo_task0_handle, foo_task1_handle, main_handle);
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
