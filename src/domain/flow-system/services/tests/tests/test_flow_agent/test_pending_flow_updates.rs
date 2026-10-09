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
async fn test_flow_scheduled_earlier_than_awaited_activation() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let start_time = harness.aligned_now();

    // The agent will sleep towards this activation
    harness
        .schedule_flow_for_activation(
            &ingest_dataset_binding(&foo_id),
            start_time + Duration::milliseconds(100),
        )
        .await;

    harness
        .simulate_flow_scenario(|| async {
            harness.advance_time(Duration::milliseconds(30)).await;

            // Scheduled while the agent sleeps, and activates earlier than it awaits
            harness
                .schedule_flow_for_activation(
                    &compaction_dataset_binding(&foo_id),
                    start_time + Duration::milliseconds(50),
                )
                .await;

            // The task exists right at the earlier moment, not only at the awaited one
            harness.advance_time(Duration::milliseconds(20)).await;
            assert!(harness.task_exists(TaskID::new(0)).await);
            assert!(!harness.task_exists(TaskID::new(1)).await);

            harness.advance_time(Duration::milliseconds(60)).await;
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

            #1: +30ms:
              "foo" HardCompaction:
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=100ms)

            #2: +50ms:
              "foo" HardCompaction:
                Flow ID = 1 Waiting AutoPolling Executor(task=0, since=50ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=100ms)

            #3: +100ms:
              "foo" HardCompaction:
                Flow ID = 1 Waiting AutoPolling Executor(task=0, since=50ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=1, since=100ms)

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_schedule_made_earlier_while_flow_waits() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    harness
        .set_flow_trigger(
            harness.now(),
            foo_flow_binding.clone(),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: runs 10..20ms, the next run then waits until 120ms
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

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(50)).await;

                // The rule changes while the flow waits for its scheduled activation at 120ms:
                // the flow follows the new schedule, 40ms after the last run
                harness
                    .set_flow_trigger(
                        harness.now(),
                        foo_flow_binding.clone(),
                        FlowTriggerRule::Schedule(Duration::milliseconds(40).into()),
                        FlowTriggerStopPolicy::default(),
                    )
                    .await;

                harness.advance_time(Duration::milliseconds(10)).await;
                assert!(harness.task_exists(TaskID::new(1)).await);

                harness.advance_time(Duration::milliseconds(90)).await;
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=60ms)
                Flow ID = 0 Finished Success

            #6: +60ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=60ms)
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_schedule_made_later_while_flow_waits() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    harness
        .set_flow_trigger(
            harness.now(),
            foo_flow_binding.clone(),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: runs 10..20ms, the next run then waits until 120ms
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

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(50)).await;

                // The rule changes while the flow waits for its scheduled activation at 120ms:
                // the flow is postponed, to 200ms after the last run
                harness
                    .set_flow_trigger(
                        harness.now(),
                        foo_flow_binding.clone(),
                        FlowTriggerRule::Schedule(Duration::milliseconds(200).into()),
                        FlowTriggerStopPolicy::default(),
                    )
                    .await;

                harness.advance_time(Duration::milliseconds(80)).await;
                assert!(!harness.task_exists(TaskID::new(1)).await);

                harness.advance_time(Duration::milliseconds(100)).await;
                assert!(harness.task_exists(TaskID::new(1)).await);
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=220ms)
                Flow ID = 0 Finished Success

            #6: +220ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=220ms)
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_config_change_reaches_waiting_flow() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_flow_binding = ingest_dataset_binding(&foo_id);

    let start_time = harness.aligned_now();
    let flow_id = harness
        .schedule_flow_for_activation(&foo_flow_binding, start_time + Duration::milliseconds(100))
        .await;

    let new_ingest_rule = FlowConfigRuleIngest {
        fetch_uncacheable: true,
        fetch_next_iteration: false,
    };
    let new_retry_policy = RetryPolicy {
        max_attempts: 2,
        min_delay_seconds: 1,
        backoff_type: RetryBackoffType::Fixed,
    };

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: formed at 100ms with the configuration set while the flow waited
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: None,
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: true,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(30)).await;

                harness
                    .set_dataset_flow_ingest(
                        foo_flow_binding.clone(),
                        new_ingest_rule.clone(),
                        Some(new_retry_policy),
                    )
                    .await;

                let flow_state = harness.flow_state(flow_id).await;
                pretty_assertions::assert_eq!(
                    Some(FlowConfigSnapshot::configured(
                        new_ingest_rule.clone().into_flow_config()
                    )),
                    flow_state.config_snapshot
                );
                pretty_assertions::assert_eq!(Some(new_retry_policy), flow_state.retry_policy);

                harness.advance_time(Duration::milliseconds(90)).await;
            };

            tokio::join!(task0_handle, main_handle);
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_config_change_keeps_rule_of_flow_with_formed_task() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_flow_binding = ingest_dataset_binding(&foo_id);

    let start_time = harness.aligned_now();
    let flow_id = harness
        .schedule_flow_for_activation(&foo_flow_binding, start_time + Duration::milliseconds(50))
        .await;

    let new_retry_policy = RetryPolicy {
        max_attempts: 2,
        min_delay_seconds: 1,
        backoff_type: RetryBackoffType::Fixed,
    };

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: formed at 50ms, before the configuration changes
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(70),
                finish_in_with: None,
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(60)).await;
                assert!(harness.task_exists(TaskID::new(0)).await);

                // The rule stays, while the retry policy still decides on attempts to come
                harness
                    .set_dataset_flow_ingest(
                        foo_flow_binding.clone(),
                        FlowConfigRuleIngest {
                            fetch_uncacheable: true,
                            fetch_next_iteration: false,
                        },
                        Some(new_retry_policy),
                    )
                    .await;

                let flow_state = harness.flow_state(flow_id).await;
                pretty_assertions::assert_eq!(None, flow_state.config_snapshot);
                pretty_assertions::assert_eq!(Some(new_retry_policy), flow_state.retry_policy);

                harness.advance_time(Duration::milliseconds(20)).await;
            };

            tokio::join!(task0_handle, main_handle);
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_config_change_keeps_forced_snapshot() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_flow_binding = ingest_dataset_binding(&foo_id);

    let forced_snapshot = FlowConfigSnapshot::forced(
        FlowConfigRuleIngest {
            fetch_uncacheable: true,
            fetch_next_iteration: false,
        }
        .into_flow_config(),
    );

    let start_time = harness.aligned_now();
    let flow_id = harness
        .schedule_flow_for_activation_with_config_snapshot(
            &foo_flow_binding,
            start_time + Duration::milliseconds(100),
            Some(forced_snapshot.clone()),
        )
        .await;

    let new_retry_policy = RetryPolicy {
        max_attempts: 2,
        min_delay_seconds: 1,
        backoff_type: RetryBackoffType::Fixed,
    };

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: formed at 100ms with the forced configuration
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: None,
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: true,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(30)).await;

                // The rule stays forced, while the retry policy still follows the configuration
                harness
                    .set_dataset_flow_ingest(
                        foo_flow_binding.clone(),
                        FlowConfigRuleIngest {
                            fetch_uncacheable: false,
                            fetch_next_iteration: false,
                        },
                        Some(new_retry_policy),
                    )
                    .await;

                let flow_state = harness.flow_state(flow_id).await;
                pretty_assertions::assert_eq!(
                    Some(forced_snapshot.clone()),
                    flow_state.config_snapshot
                );
                pretty_assertions::assert_eq!(Some(new_retry_policy), flow_state.retry_policy);

                harness.advance_time(Duration::milliseconds(90)).await;
            };

            tokio::join!(task0_handle, main_handle);
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_retry_policy_change_reaches_retrying_flow() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;
    let foo_flow_binding = ingest_dataset_binding(&foo_id);

    let ingest_rule = FlowConfigRuleIngest {
        fetch_uncacheable: false,
        fetch_next_iteration: false,
    };
    harness
        .set_dataset_flow_ingest(
            foo_flow_binding.clone(),
            ingest_rule.clone(),
            Some(RetryPolicy {
                max_attempts: 2,
                min_delay_seconds: 1,
                backoff_type: RetryBackoffType::Fixed,
            }),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

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

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(500)).await;

                // While the flow waits for its retry, the policy allows one attempt less:
                // the planned retry still runs, and its failure is final
                harness
                    .set_dataset_flow_ingest(
                        foo_flow_binding.clone(),
                        ingest_rule.clone(),
                        Some(RetryPolicy {
                            max_attempts: 1,
                            min_delay_seconds: 1,
                            backoff_type: RetryBackoffType::Fixed,
                        }),
                    )
                    .await;

                harness.advance_time(Duration::milliseconds(1600)).await;
                assert!(!harness.task_exists(TaskID::new(2)).await);
            };

            tokio::join!(trigger0_handle, task0_handle, task1_handle, main_handle);
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
                Flow ID = 0 Finished Failed

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_rule_lowered_while_flow_waits() {
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
    let bar_flow_binding = transform_dataset_binding(&bar_id);

    let initial_rule = ReactiveRule::new(
        BatchingRule::try_buffering(100, Duration::milliseconds(300)).unwrap(),
        BreakingChangeRule::NoAction,
    );
    harness
        .set_flow_trigger(
            harness.now(),
            bar_flow_binding.clone(),
            FlowTriggerRule::Reactive(initial_rule),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // "bar" waits for 100 records until 300ms, and has 5 so far
    harness
        .save_batching_flow(&bar_flow_binding, &foo_id, 5, initial_rule)
        .await;

    harness
        .simulate_flow_scenario(|| async {
            harness.advance_time(Duration::milliseconds(20)).await;

            // 3 records are enough now: the flow runs without waiting for more inputs
            harness
                .set_flow_trigger(
                    harness.now(),
                    bar_flow_binding.clone(),
                    FlowTriggerRule::Reactive(ReactiveRule::new(
                        BatchingRule::try_buffering(3, Duration::milliseconds(300)).unwrap(),
                        BreakingChangeRule::NoAction,
                    )),
                    FlowTriggerStopPolicy::default(),
                )
                .await;

            harness.advance_time(Duration::milliseconds(10)).await;
            assert!(harness.task_exists(TaskID::new(0)).await);
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batching_interval_extended_while_flow_waits() {
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
    let bar_flow_binding = transform_dataset_binding(&bar_id);

    let initial_rule = ReactiveRule::new(
        BatchingRule::try_buffering(100, Duration::milliseconds(100)).unwrap(),
        BreakingChangeRule::NoAction,
    );
    harness
        .set_flow_trigger(
            harness.now(),
            bar_flow_binding.clone(),
            FlowTriggerRule::Reactive(initial_rule),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // "bar" waits for 100 records until 100ms, and has 5 so far
    let start_time = harness.now();
    let flow_id = harness
        .save_batching_flow(&bar_flow_binding, &foo_id, 5, initial_rule)
        .await;

    let extended_rule = ReactiveRule::new(
        BatchingRule::try_buffering(100, Duration::milliseconds(200)).unwrap(),
        BreakingChangeRule::NoAction,
    );

    harness
        .simulate_flow_scenario(|| async {
            harness.advance_time(Duration::milliseconds(20)).await;

            // The flow may batch longer: its deadline moves from 100ms to 200ms
            harness
                .set_flow_trigger(
                    harness.now(),
                    bar_flow_binding.clone(),
                    FlowTriggerRule::Reactive(extended_rule),
                    FlowTriggerStopPolicy::default(),
                )
                .await;

            let Some(FlowStartCondition::Reactive(reactive_condition)) =
                harness.flow_state(flow_id).await.start_condition
            else {
                panic!("Flow must still batch its inputs");
            };
            pretty_assertions::assert_eq!(extended_rule, reactive_condition.active_rule);
            pretty_assertions::assert_eq!(
                start_time + Duration::milliseconds(200),
                reactive_condition.batching_deadline
            );

            harness.advance_time(Duration::milliseconds(130)).await;
            assert!(!harness.task_exists(TaskID::new(0)).await);

            harness.advance_time(Duration::milliseconds(60)).await;
            assert!(harness.task_exists(TaskID::new(0)).await);
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
