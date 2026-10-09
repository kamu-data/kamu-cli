// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use chrono::{Duration, DurationRound};
use kamu_adapter_flow_dataset::*;
use kamu_adapter_task_dataset::*;
use kamu_core::{PullResult, TransformStatus};
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::*;
use odf::dataset::MetadataChainIncrementInterval;

use crate::tests::{
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    ManualFlowAbortArgs,
    ManualFlowActivationArgs,
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
            ingest_dataset_binding(&foo_id),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: false,
            },
            None, // No retry policy
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
            FlowTriggerStopPolicy::default(),
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
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task1_handle = foo_task1_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, foo_task1_handle, sim_handle);
        })
        .await
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
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #8: +100ms:
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

    let foo_ingest_binding = ingest_dataset_binding(&foo_id);

    // Remember start time
    let start_time = harness.aligned_now();

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
                    stop_policy: FlowTriggerStopPolicy::default(),
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
                    activation_cause: FlowActivationCause::AutoPolling(
                        FlowActivationCauseAutoPolling {
                            activation_time: start_time,
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    event_time: start_time,
                    flow_id,
                    flow_binding: foo_ingest_binding.clone(),
                    start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                        wake_up_at: start_time + Duration::milliseconds(100),
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    event_time: start_time,
                    flow_id,
                    flow_binding: foo_ingest_binding.clone(),
                    scheduled_for_activation_at: start_time + Duration::milliseconds(100),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 110ms, finish at 120ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
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

            // Main simulation boundary - 130ms total
            let sim_handle = harness.advance_time(Duration::milliseconds(130));
            tokio::join!(foo_task0_handle, sim_handle);
        })
        .await
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
                Flow ID = 0 Finished Success

            #4: +120ms:
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

    // Remember start time
    let _start_time = harness.now().duration_round(Duration::seconds(1)).unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 6s, finish at 7s
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::seconds(6),
                finish_in_with: Some((
                    Duration::seconds(1),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Main simulation script
            let main_handle = async {
                // Wait 2 s
                harness
                    .advance_time_custom_alignment(Duration::seconds(1), Duration::seconds(2))
                    .await;

                // Enable CRON config (we are skipping moment 0s)
                harness
                    .set_flow_trigger(
                        harness.now(),
                        ingest_dataset_binding(&foo_id),
                        FlowTriggerRule::Schedule(Schedule::Cron(ScheduleCron {
                            source_5component_cron_expression: String::from("<irrelevant>"),
                            cron_schedule: cron::Schedule::from_str("*/5 * * * * *").unwrap(),
                        })),
                        FlowTriggerStopPolicy::default(),
                    )
                    .await;

                // Main simulation boundary - 12s total: at 10s 2nd scheduling happens;
                harness
                    .advance_time_custom_alignment(Duration::seconds(1), Duration::seconds(11))
                    .await;
            };

            tokio::join!(foo_task0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +2000ms:
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
                Flow ID = 0 Finished Success

            #5: +7000ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=10000ms)
                Flow ID = 0 Finished Success

            #6: +10000ms:
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

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    let bar_flow_binding = ingest_dataset_binding(&bar_id);

    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            FlowConfigRuleIngest {
                fetch_uncacheable: true,
                fetch_next_iteration: false,
            },
            None, // No retry policy
        )
        .await;
    harness
        .set_flow_trigger(
            harness.now(),
            foo_flow_binding.clone(),
            FlowTriggerRule::Schedule(Duration::milliseconds(90).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
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
                    fetch_uncacheable: true,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "for" start running at 60ms, finish at 70ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(60),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: true,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "bar" start running at 100ms, finish at 110ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(100),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Manual trigger for "foo" at 40ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(40),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Manual trigger for "bar" at 80ms
            let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: bar_flow_binding,
                run_since_start: Duration::milliseconds(80),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger1_handle = trigger1_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(180)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                trigger0_handle,
                trigger1_handle,
                main_handle
            );
        })
        .await
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
                Flow ID = 0 Finished Success

            #4: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=110ms)
                Flow ID = 0 Finished Success

            #5: +40ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=110ms) Activating(at=40ms)
                Flow ID = 0 Finished Success

            #6: +40ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=40ms)
                Flow ID = 0 Finished Success

            #7: +60ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #8: +70ms:
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #9: +70ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #10: +80ms:
              "bar" Ingest:
                Flow ID = 3 Waiting Manual
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #11: +80ms:
              "bar" Ingest:
                Flow ID = 3 Waiting Manual Executor(task=2, since=80ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #12: +100ms:
              "bar" Ingest:
                Flow ID = 3 Running(task=2)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #13: +110ms:
              "bar" Ingest:
                Flow ID = 3 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=160ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #14: +160ms:
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
async fn test_ingest_flow_with_multiple_iterations() {
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
        mock_dataset_changes: Some(MockDatasetIncrementQueryService::with_increment_between(
            MetadataChainIncrementInterval {
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

    let foo_flow_binding = ingest_dataset_binding(&foo_id);

    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            FlowConfigRuleIngest {
                fetch_uncacheable: false,
                fetch_next_iteration: true,
            },
            None,
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(1, Duration::seconds(1)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 30ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                // Send some PullResult with records and has_more flag to trigger another iteration
                finish_in_with: Some((
                    Duration::milliseconds(20),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                                new_head: odf::Multihash::from_digest_sha3_256(b"newest-slice"),
                                has_more: true,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo" start running at 40ms, finish at 60ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(40),
                // Send some PullResult with records and has_more flag to trigger another iteration
                finish_in_with: Some((
                    Duration::milliseconds(20),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"newest-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"newest-slice-1"),
                                has_more: true,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 60ms, finish at 80ms
            // And returns empty result
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(60),
                finish_in_with: Some((
                    Duration::milliseconds(20),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 100ms, finish at 120ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(100),
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
            let task3_handle = task3_driver.run();

            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(700)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                task3_handle,
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

            #1: +10ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual

            #2: +10ms:
              "foo" Ingest:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #3: +10ms:
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +30ms:
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +30ms:
              "foo" Ingest:
                Flow ID = 1 Waiting Iteration Finish
                Flow ID = 0 Finished Success

            #6: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(3/1, until=1030ms)
              "foo" Ingest:
                Flow ID = 1 Waiting Iteration Finish
                Flow ID = 0 Finished Success

            #7: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(3/1, until=1030ms) Activating(at=30ms)
              "foo" Ingest:
                Flow ID = 1 Waiting Iteration Finish
                Flow ID = 0 Finished Success

            #8: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(3/1, until=1030ms) Activating(at=30ms)
              "foo" Ingest:
                Flow ID = 1 Waiting Iteration Finish Executor(task=1, since=30ms)
                Flow ID = 0 Finished Success

            #9: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=30ms)
              "foo" Ingest:
                Flow ID = 1 Waiting Iteration Finish Executor(task=1, since=30ms)
                Flow ID = 0 Finished Success

            #10: +40ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=30ms)
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #11: +60ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=30ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #12: +60ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=30ms)
              "foo" Ingest:
                Flow ID = 3 Waiting Iteration Finish
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #13: +60ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting Iteration Finish
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #14: +60ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting Iteration Finish Executor(task=3, since=60ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #15: +80ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting Iteration Finish Executor(task=3, since=60ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #16: +80ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Throttling(for=20ms, wakeup=100ms, shifted=60ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting Iteration Finish Executor(task=3, since=60ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #17: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Executor(task=4, since=100ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting Iteration Finish Executor(task=3, since=60ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #18: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Executor(task=4, since=100ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Running(task=3)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #19: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Executor(task=4, since=100ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "bar" ExecuteTransform Flow ID = 2
            "foo" Ingest Flow ID = 1 => "bar" ExecuteTransform Flow ID = 4
            "#
        ),
        harness.activation_links_report().await
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
            harness.now(),
            ingest_dataset_binding(&bar_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;
    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness.aligned_now();

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
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
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 20ms, finish at 30ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Main simulation script
            let main_handle = async {
                // 50ms: Pause both flow triggers in between completion 2 first tasks and
                // queuing
                harness.advance_time(Duration::milliseconds(50)).await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(50),
                        &ingest_dataset_binding(&foo_id),
                    )
                    .await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(50),
                        &ingest_dataset_binding(&bar_id),
                    )
                    .await;

                // 80ms: Wake up after initially planned "foo" scheduling but before planned
                // "bar" scheduling
                harness.advance_time(Duration::milliseconds(30)).await;
                harness
                    .resume_flow(
                        start_time + Duration::milliseconds(80),
                        &ingest_dataset_binding(&foo_id),
                    )
                    .await;
                harness
                    .set_flow_trigger(
                        start_time + Duration::milliseconds(80),
                        ingest_dataset_binding(&bar_id),
                        FlowTriggerRule::Schedule(Duration::milliseconds(70).into()),
                        FlowTriggerStopPolicy::default(),
                    )
                    .await;

                // 120ms: finish
                harness.advance_time(Duration::milliseconds(40)).await;
            };

            tokio::join!(task0_handle, task1_handle, main_handle);
        })
        .await
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
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #3: +10ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #6: +20ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #7: +30ms:
              "bar" Ingest:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #8: +30ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=110ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #9: +50ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=110ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #10: +50ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #11: +80ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #12: +80ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #13: +80ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=2, since=80ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #14: +100ms:
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
            harness.now(),
            ingest_dataset_binding(&bar_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;
    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness.aligned_now();

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
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
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 30ms, finish at 40ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Main simulation script
            let main_handle = async {
                // 50ms: Pause flow config before next flow runs
                harness.advance_time(Duration::milliseconds(50)).await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(50),
                        &ingest_dataset_binding(&foo_id),
                    )
                    .await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(50),
                        &ingest_dataset_binding(&bar_id),
                    )
                    .await;

                // 100ms: Wake up after initially planned "bar" scheduling but before planned
                // "foo" scheduling
                harness.advance_time(Duration::milliseconds(50)).await;
                harness
                    .resume_flow(
                        start_time + Duration::milliseconds(100),
                        &ingest_dataset_binding(&foo_id),
                    )
                    .await;
                harness
                    .resume_flow(
                        start_time + Duration::milliseconds(100),
                        &ingest_dataset_binding(&bar_id),
                    )
                    .await;

                // 150ms: finish
                harness.advance_time(Duration::milliseconds(50)).await;
            };

            tokio::join!(task0_handle, task1_handle, main_handle);
        })
        .await
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
                Flow ID = 1 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #3: +10ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #6: +30ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #7: +40ms:
              "bar" Ingest:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #8: +40ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #9: +50ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #10: +50ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #11: +100ms:
              "bar" Ingest:
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #12: +100ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #13: +100ms:
              "bar" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 3 Finished Aborted
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #14: +120ms:
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

    for dataset_id in [&baz_id, &bar_id, &foo_id] {
        harness
            .set_flow_trigger(
                harness.now(),
                ingest_dataset_binding(dataset_id),
                FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
                FlowTriggerStopPolicy::default(),
            )
            .await;
    }

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());
    test_flow_listener.define_dataset_display_name(baz_id.clone(), "baz".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
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
            let task0_handle = task0_driver.run();

            // Task 1: "bar" start running at 30ms, finish at 40ms with failure
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Failed(TaskError::empty_recoverable()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "baz" start running at 50ms, finish at 70ms with cancellation
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::milliseconds(50),
                finish_in_with: Some((Duration::milliseconds(20), TaskOutcome::Cancelled)),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: baz_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Manual abort for "baz" at 60ms
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(2),
                abort_since_start: Duration::milliseconds(60),
            });
            let abort0_handle = abort0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(80)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                abort0_handle,
                main_handle
            );
        })
        .await
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
                Flow ID = 1 Waiting AutoPolling
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #3: +0ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #4: +10ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #5: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #6: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #7: +30ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #8: +40ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=0ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #9: +50ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #10: +60ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=80ms)
                Flow ID = 0 Finished Success

            #11: +80ms:
              "bar" Ingest:
                Flow ID = 1 Finished Failed
              "baz" Ingest:
                Flow ID = 2 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=80ms)
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_respect_last_success_time_for_root_dataset_when_activate_configuration() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Remember start time
    let start_time = harness.aligned_now();

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
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
            let task0_handle = task0_driver.run();

            // Main simulation script
            let main_handle = async {
                // 50ms: Pause flow config before next flow runs
                harness.advance_time(Duration::milliseconds(50)).await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(50),
                        &ingest_dataset_binding(&foo_id),
                    )
                    .await;

                // 100ms: Wake up before planned "foo" scheduling
                harness.advance_time(Duration::milliseconds(50)).await;
                harness
                    .resume_flow(
                        start_time + Duration::milliseconds(100),
                        &ingest_dataset_binding(&foo_id),
                    )
                    .await;

                // 150ms: finish
                harness.advance_time(Duration::milliseconds(50)).await;
            };

            tokio::join!(task0_handle, main_handle);
        })
        .await
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
                Flow ID = 0 Finished Success

            #4: +20ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #5: +50ms:
              "foo" Ingest:
                Flow ID = 1 Finished Aborted
                Flow ID = 0 Finished Success

            #6: +100ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 1 Finished Aborted
                Flow ID = 0 Finished Success

            #7: +120ms:
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=1, since=120ms)
                Flow ID = 1 Finished Aborted
                Flow ID = 0 Finished Success

      "#
        ),
        format!("{}", test_flow_listener.as_ref())
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

    let foo_flow_binding = ingest_dataset_binding(&foo_id);

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Manual trigger for "foo" at 20ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding.clone(),
                run_since_start: Duration::milliseconds(20),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Manual trigger for "foo" at 30ms
            let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding.clone(),
                run_since_start: Duration::milliseconds(30),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger1_handle = trigger1_driver.run();

            // Manual trigger for "foo" at 70ms
            let trigger2_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(70),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger2_handle = trigger2_driver.run();

            // Task 0: "foo" start running at 40ms, finish at 50ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(40),
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

            // Task 1: "foo" start running at 250ms, finish at 260ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(250),
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

            // Task 2: "foo" start running at 260ms, finish at 160ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(520),
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

            let task0_handle = task0_driver.run();
            let task1_handle = task1_driver.run();
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(100)).await;

                harness
                    .set_flow_trigger(
                        harness.now(),
                        ingest_dataset_binding(&foo_id),
                        FlowTriggerRule::Schedule(Duration::milliseconds(60).into()),
                        FlowTriggerStopPolicy::default(),
                    )
                    .await;

                harness.advance_time(Duration::milliseconds(1000)).await;
            };

            tokio::join!(
                trigger0_handle,
                trigger1_handle,
                trigger2_handle,
                task0_handle,
                task1_handle,
                task2_handle,
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

        #3: +40ms:
          "foo" Ingest:
            Flow ID = 0 Running(task=0)

        #4: +50ms:
          "foo" Ingest:
            Flow ID = 0 Finished Success

        #5: +70ms:
          "foo" Ingest:
            Flow ID = 1 Waiting Manual Throttling(for=200ms, wakeup=250ms, shifted=70ms)
            Flow ID = 0 Finished Success

        #6: +250ms:
          "foo" Ingest:
            Flow ID = 1 Waiting Manual Executor(task=1, since=250ms)
            Flow ID = 0 Finished Success

        #7: +250ms:
          "foo" Ingest:
            Flow ID = 1 Running(task=1)
            Flow ID = 0 Finished Success

        #8: +260ms:
          "foo" Ingest:
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #9: +260ms:
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Throttling(for=200ms, wakeup=460ms, shifted=320ms)
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #10: +460ms:
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Executor(task=2, since=460ms)
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #11: +520ms:
          "foo" Ingest:
            Flow ID = 2 Running(task=2)
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #12: +530ms:
          "foo" Ingest:
            Flow ID = 2 Finished Success
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #13: +530ms:
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Throttling(for=200ms, wakeup=730ms, shifted=590ms)
            Flow ID = 2 Finished Success
            Flow ID = 1 Finished Success
            Flow ID = 0 Finished Success

        #14: +730ms:
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
async fn test_flow_failing_to_schedule_does_not_block_later_flows() {
    const UNREGISTERED_FLOW_TYPE: &str = "dev.kamu.flow.test.unregistered";

    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let start_time = harness.aligned_now();

    // No controller is registered for this flow type, so scheduling it always fails
    harness
        .schedule_flow_for_activation(
            &FlowBinding::new(
                UNREGISTERED_FLOW_TYPE,
                FlowScopeDataset::make_scope(&foo_id),
            ),
            start_time + Duration::milliseconds(10),
        )
        .await;

    // Activates later than the failing flow
    harness
        .schedule_flow_for_activation(
            &ingest_dataset_binding(&foo_id),
            start_time + Duration::milliseconds(20),
        )
        .await;

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 30ms, finish at 40ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(30),
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

            let sim_handle = harness.advance_time(Duration::milliseconds(60));
            tokio::join!(foo_task0_handle, sim_handle);
        })
        .await
        .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" <unknown>:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=10ms)
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=20ms)

            #1: +20ms:
              "foo" <unknown>:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=10ms)
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=0, since=20ms)

            #2: +30ms:
              "foo" <unknown>:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=10ms)
              "foo" Ingest:
                Flow ID = 1 Running(task=0)

            #3: +40ms:
              "foo" <unknown>:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=10ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    // Activated right at its moment, as time is virtual; the failing flow is
    // retried
    let ingest_flow_type = ingest_dataset_binding(&foo_id).flow_type;
    assert_eq!(harness.flow_activations(&ingest_flow_type, "activated"), 1);
    assert!(harness.flow_activations(UNREGISTERED_FLOW_TYPE, "failed") >= 1);
    assert_eq!(harness.flow_activation_delay_samples(), 1);
    assert!(harness.flow_activation_delays_total_seconds() < 0.001);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_due_flows_activated_in_pages_past_failing_ones() {
    const UNREGISTERED_FLOW_TYPE: &str = "dev.kamu.flow.test.unregistered";

    // One flow per page: failing flows stay due, and fill the first pages
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        activation_batch_size: Some(1),
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

    let start_time = harness.aligned_now();

    // No controller is registered for this flow type, so scheduling it always fails
    for dataset_id in [&foo_id, &bar_id] {
        harness
            .schedule_flow_for_activation(
                &FlowBinding::new(
                    UNREGISTERED_FLOW_TYPE,
                    FlowScopeDataset::make_scope(dataset_id),
                ),
                start_time + Duration::milliseconds(10),
            )
            .await;
    }

    for dataset_id in [&foo_id, &bar_id] {
        harness
            .schedule_flow_for_activation(
                &ingest_dataset_binding(dataset_id),
                start_time + Duration::milliseconds(20),
            )
            .await;
    }

    harness
        .simulate_flow_scenario(|| async {
            harness.advance_time(Duration::milliseconds(30)).await;
        })
        .await
        .unwrap();

    // Activated right at their moment, in the same pass as the failing flows'
    // retries
    let ingest_flow_type = ingest_dataset_binding(&foo_id).flow_type;
    assert_eq!(harness.flow_activations(&ingest_flow_type, "activated"), 2);
    assert!(harness.flow_activations(UNREGISTERED_FLOW_TYPE, "failed") >= 2);
    assert_eq!(harness.flow_activation_delay_samples(), 2);
    assert!(harness.flow_activation_delays_total_seconds() < 0.001);
    assert!(harness.task_exists(TaskID::new(0)).await);
    assert!(harness.task_exists(TaskID::new(1)).await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_independent_flows_due_at_same_moment() {
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

    let start_time = harness.aligned_now();

    harness
        .schedule_flow_for_activation(
            &ingest_dataset_binding(&foo_id),
            start_time + Duration::milliseconds(50),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    harness
        .simulate_flow_scenario(|| async {
            harness.advance_time(Duration::milliseconds(20)).await;

            // Scheduled later, for the same moment
            harness
                .schedule_flow_for_activation(
                    &ingest_dataset_binding(&bar_id),
                    start_time + Duration::milliseconds(50),
                )
                .await;

            harness.advance_time(Duration::milliseconds(30)).await;

            // Both activated at that moment, in the order of flow IDs
            assert!(harness.task_exists(TaskID::new(0)).await);
            assert!(harness.task_exists(TaskID::new(1)).await);

            harness.advance_time(Duration::milliseconds(20)).await;
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=50ms)

            #1: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=50ms)

            #2: +50ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=50ms)

            #3: +50ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=50ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=50ms)

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_activation_moment_between_scheduling_steps() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let start_time = harness.aligned_now();

    // Not a multiple of the scheduling step
    harness
        .schedule_flow_for_activation(
            &ingest_dataset_binding(&foo_id),
            start_time + Duration::milliseconds(25),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    harness
        .simulate_flow_scenario(|| async {
            harness.advance_time(Duration::milliseconds(20)).await;
            assert!(!harness.task_exists(TaskID::new(0)).await);

            // The first step past the activation moment activates the flow
            harness.advance_time(Duration::milliseconds(10)).await;
            assert!(harness.task_exists(TaskID::new(0)).await);

            harness.advance_time(Duration::milliseconds(20)).await;
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=25ms)

            #1: +25ms:
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=25ms)

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
