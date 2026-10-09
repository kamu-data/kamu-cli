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

use crate::tests::{FlowHarness, FlowSystemTestListener, TaskDriverArgs};

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
async fn test_schedule_trigger_modified_while_flow_waits() {
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
                // the flow is re-evaluated, and runs right away, as throttling allows it
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=120ms) Activating(at=50ms)
                Flow ID = 0 Finished Success

            #6: +50ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=50ms)
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
async fn test_config_change_skips_flow_with_formed_task() {
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

                harness
                    .set_dataset_flow_ingest(
                        foo_flow_binding.clone(),
                        FlowConfigRuleIngest {
                            fetch_uncacheable: true,
                            fetch_next_iteration: false,
                        },
                        Some(RetryPolicy {
                            max_attempts: 2,
                            min_delay_seconds: 1,
                            backoff_type: RetryBackoffType::Fixed,
                        }),
                    )
                    .await;

                let flow_state = harness.flow_state(flow_id).await;
                pretty_assertions::assert_eq!(None, flow_state.config_snapshot);
                pretty_assertions::assert_eq!(None, flow_state.retry_policy);

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
