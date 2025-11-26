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
use kamu_adapter_flow_dataset::*;
use kamu_adapter_task_dataset::*;
use kamu_core::{CompactionResult, PullResult, ResetResult, TransformStatus};
use kamu_datasets::DatasetEntryServiceExt;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::*;
use odf::dataset::MetadataChainIncrementInterval;

use super::{
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
    let start_time = harness
        .now()
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

    let foo_flow_binding = ingest_dataset_binding(&foo_id);
    let bar_flow_binding = ingest_dataset_binding(&bar_id);

    // Note: only "foo" has auto-schedule, "bar" hasn't
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
                    fetch_uncacheable: false,
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
                    fetch_uncacheable: false,
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
                    fetch_uncacheable: true,
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
                maybe_forced_flow_config_rule: Some(
                    FlowConfigRuleIngest {
                        fetch_uncacheable: true,
                        fetch_next_iteration: false,
                    }
                    .into_flow_config(),
                ),
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
async fn test_manual_ingest_with_compaction_trigger() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let ingest_flow_binding = ingest_dataset_binding(&foo_id);
    let compaction_flow_binding = compaction_dataset_binding(&foo_id);

    harness
        .set_flow_trigger(
            harness.now(),
            compaction_flow_binding.clone(),
            FlowTriggerRule::Schedule(Duration::milliseconds(120).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    harness
        .simulate_flow_scenario(|| async {
            let ingest_task_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
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
            let ingest_task_handle = ingest_task_driver.run();

            let compaction_task_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(130),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: foo_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                }
                .into_logical_plan(),
            });
            let compaction_task_handle = compaction_task_driver.run();

            let manual_ingest_trigger =
                harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                    flow_binding: ingest_flow_binding.clone(),
                    run_since_start: Duration::milliseconds(10),
                    initiator_id: None,
                    maybe_forced_flow_config_rule: None,
                });
            let manual_ingest_handle = manual_ingest_trigger.run();

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(200)).await;
            };

            tokio::join!(
                ingest_task_handle,
                compaction_task_handle,
                manual_ingest_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)
              "foo" Ingest:
                Flow ID = 1 Waiting Manual

            #3: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)
              "foo" Ingest:
                Flow ID = 1 Waiting Manual Executor(task=1, since=10ms)

            #4: +20ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)
              "foo" Ingest:
                Flow ID = 1 Running(task=1)

            #5: +30ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success

            #6: +130ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)
              "foo" Ingest:
                Flow ID = 1 Finished Success

            #7: +140ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 1 Finished Success

            #8: +140ms:
              "foo" HardCompaction:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=260ms)
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 1 Finished Success

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
                Flow ID = 4 Waiting Input(foo) Throttling(for=20ms, wakeup=100ms, shifted=60ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Running(task=3)
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

    let foo_flow_binding = compaction_dataset_binding(&foo_id);
    let bar_flow_binding = compaction_dataset_binding(&bar_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 20ms, finish at 30ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: foo_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(60),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: bar_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Manual trigger for "foo" at 10ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Manual trigger for "bar" at 50ms
            let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: bar_flow_binding,
                run_since_start: Duration::milliseconds(50),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger1_handle = trigger1_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(100)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
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

            #1: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual

            #2: +10ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #3: +20ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)

            #4: +30ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            #5: +50ms:
              "bar" HardCompaction:
                Flow ID = 1 Waiting Manual
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            #6: +50ms:
              "bar" HardCompaction:
                Flow ID = 1 Waiting Manual Executor(task=1, since=50ms)
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            #7: +60ms:
              "bar" HardCompaction:
                Flow ID = 1 Running(task=1)
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            #8: +70ms:
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

    harness
        .set_dataset_flow_reset_rule(
            reset_dataset_binding(&foo_id),
            FlowConfigRuleReset {
                new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
            },
        )
        .await;

    let foo_flow_binding = reset_dataset_binding(&foo_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 20ms, finish at 110ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((
                    Duration::milliseconds(90),
                    TaskOutcome::Success(
                        TaskResultDatasetReset {
                            reset_result: ResetResult {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetReset {
                    dataset_id: foo_id.clone(),
                    // By default, should reset to seed block
                    new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                    old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Manual trigger for "foo" at 10ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(250)).await;
            };

            tokio::join!(task0_handle, trigger0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" Reset:
                Flow ID = 0 Waiting Manual

            #2: +10ms:
              "foo" Reset:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #3: +20ms:
              "foo" Reset:
                Flow ID = 0 Running(task=0)

            #4: +110ms:
              "foo" Reset:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_reset_to_metadata() {
    let harness = FlowHarness::new();

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = reset_to_metadata_dataset_binding(&foo_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 20ms, finish at 110ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((
                    Duration::milliseconds(90),
                    TaskOutcome::Success(
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                                new_num_blocks: 3,
                                old_num_blocks: 7,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetResetToMetadata {
                    dataset_id: foo_id.clone(),
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Manual trigger for "foo" at 10ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(250)).await;
            };

            tokio::join!(task0_handle, trigger0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Waiting Manual

            #2: +10ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #3: +20ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Running(task=0)

            #4: +110ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_trigger_derivatives_reactively() {
    // foo.bar: evaluated after enabling trigger
    // foo.baz: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(2)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
        ..Default::default()
    });

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
    let foo_qux_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.qux"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_reset_rule(
            reset_dataset_binding(&foo_id),
            FlowConfigRuleReset {
                new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
            },
        )
        .await;

    // Enable auto-updates on foo.bar with recovery on breaking changes
    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&foo_bar_id),
            FlowTriggerRule::Reactive(ReactiveRule {
                for_new_data: BatchingRule::immediate(),
                for_breaking_change: BreakingChangeRule::Recover,
            }),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enable auto-updates on foo.baz without recovery on breaking changes
    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&foo_baz_id),
            FlowTriggerRule::Reactive(ReactiveRule {
                for_new_data: BatchingRule::immediate(),
                for_breaking_change: BreakingChangeRule::NoAction,
            }),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Don't enable auto-updates on foo.qux

    let foo_flow_binding = reset_dataset_binding(&foo_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener.define_dataset_display_name(foo_baz_id.clone(), "foo_baz".to_string());
    test_flow_listener.define_dataset_display_name(foo_qux_id.clone(), "foo_qux".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(50),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" reset start running at 50ms, finish at 90ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(50),
                finish_in_with: Some((
                    Duration::milliseconds(40),
                    TaskOutcome::Success(
                        TaskResultDatasetReset {
                            reset_result: ResetResult {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetReset {
                    dataset_id: foo_id.clone(),
                    new_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                    old_head_hash: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo_bar" start running at 110ms, finish at 180ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_bar_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some((
                    Duration::milliseconds(70),
                    TaskOutcome::Success(
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-2"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-2"),
                                old_num_blocks: 5,
                                new_num_blocks: 4,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetResetToMetadata {
                    dataset_id: foo_bar_id.clone(),
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(400)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, task1_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
          #0: +0ms:

          #1: +50ms:
            "foo" Reset:
              Flow ID = 0 Waiting Manual

          #2: +50ms:
            "foo" Reset:
              Flow ID = 0 Waiting Manual Executor(task=0, since=50ms)

          #3: +50ms:
            "foo" Reset:
              Flow ID = 0 Running(task=0)

          #4: +90ms:
            "foo" Reset:
              Flow ID = 0 Finished Success

          #5: +90ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Waiting Input(foo)

          #6: +90ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Waiting Input(foo) Executor(task=1, since=90ms)

          #7: +110ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Running(task=1)

          #8: +180ms:
            "foo" Reset:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
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

    harness
        .set_dataset_flow_compaction_rule(
            compaction_dataset_binding(&foo_id),
            FlowConfigRuleCompact::try_new(max_slice_size, max_slice_records).unwrap(),
        )
        .await;

    let foo_flow_binding = compaction_dataset_binding(&foo_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 30ms, finish at 40ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: foo_id.clone(),
                    max_slice_size: Some(max_slice_size),
                    max_slice_records: Some(max_slice_records),
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(20),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(80)).await;
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
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual

            #2: +20ms:
              "foo" HardCompaction:
                Flow ID = 0 Waiting Manual Executor(task=0, since=20ms)

            #3: +30ms:
              "foo" HardCompaction:
                Flow ID = 0 Running(task=0)

            #4: +40ms:
              "foo" HardCompaction:
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_hard_compaction_trigger_derivatives_reactively() {
    let max_slice_size = 1_000_000u64;
    let max_slice_records = 1000u64;

    // foo.bar: evaluated after enabling trigger
    // foo.baz: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(2)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
        ..Default::default()
    });

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
    let foo_qux_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("foo.qux"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    harness
        .set_dataset_flow_compaction_rule(
            compaction_dataset_binding(&foo_id),
            FlowConfigRuleCompact::try_new(max_slice_size, max_slice_records).unwrap(),
        )
        .await;

    // Enable auto-updates on foo.bar with recovery on breaking changes
    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&foo_bar_id),
            FlowTriggerRule::Reactive(ReactiveRule {
                for_new_data: BatchingRule::immediate(),
                for_breaking_change: BreakingChangeRule::Recover,
            }),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enable auto-updates on foo.baz without recovery on breaking changes
    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&foo_baz_id),
            FlowTriggerRule::Reactive(ReactiveRule {
                for_new_data: BatchingRule::immediate(),
                for_breaking_change: BreakingChangeRule::NoAction,
            }),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Don't enable auto-updates on foo.qux

    let foo_flow_binding = compaction_dataset_binding(&foo_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener.define_dataset_display_name(foo_baz_id.clone(), "foo_baz".to_string());
    test_flow_listener.define_dataset_display_name(foo_qux_id.clone(), "foo_qux".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(50),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 50ms, finish at 90ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(50),
                finish_in_with: Some((
                    Duration::milliseconds(40),
                    TaskOutcome::Success(
                        TaskResultDatasetHardCompact {
                            compaction_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                                old_num_blocks: 5,
                                new_num_blocks: 4,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: foo_id.clone(),
                    max_slice_size: Some(max_slice_size),
                    max_slice_records: Some(max_slice_records),
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo_bar" start running at 110, finish at 150ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_bar_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some((
                    Duration::milliseconds(40),
                    TaskOutcome::Success(
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-3"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-3"),
                                old_num_blocks: 8,
                                new_num_blocks: 3,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetResetToMetadata {
                    dataset_id: foo_bar_id.clone(),
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(200)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, task1_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
          #0: +0ms:

          #1: +50ms:
            "foo" HardCompaction:
              Flow ID = 0 Waiting Manual

          #2: +50ms:
            "foo" HardCompaction:
              Flow ID = 0 Waiting Manual Executor(task=0, since=50ms)

          #3: +50ms:
            "foo" HardCompaction:
              Flow ID = 0 Running(task=0)

          #4: +90ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success

          #5: +90ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Waiting Input(foo)

          #6: +90ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Waiting Input(foo) Executor(task=1, since=90ms)

          #7: +110ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Running(task=1)

          #8: +150ms:
            "foo" HardCompaction:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref()),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_keep_metadata_only_with_reactive_updates() {
    // foo.bar: evaluated after enabling trigger
    // foo.bar.baz: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(2)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
        ..Default::default()
    });

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
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&foo_bar_id),
            FlowTriggerRule::Reactive(ReactiveRule {
                for_new_data: BatchingRule::immediate(),
                for_breaking_change: BreakingChangeRule::Recover,
            }),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&foo_bar_baz_id),
            FlowTriggerRule::Reactive(ReactiveRule {
                for_new_data: BatchingRule::immediate(),
                for_breaking_change: BreakingChangeRule::Recover,
            }),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let foo_flow_binding = reset_to_metadata_dataset_binding(&foo_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener
        .define_dataset_display_name(foo_bar_baz_id.clone(), "foo_bar_baz".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(50),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 50ms, finish at 90ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(50),
                finish_in_with: Some((
                    Duration::milliseconds(40),
                    TaskOutcome::Success(
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                                old_num_blocks: 5,
                                new_num_blocks: 4,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetResetToMetadata {
                    dataset_id: foo_id.clone(),
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo_bar" start running at 110ms, finish at 180ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(foo_bar_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some((
                    Duration::milliseconds(70),
                    TaskOutcome::Success(
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-2"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-2"),
                                old_num_blocks: 5,
                                new_num_blocks: 4,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetResetToMetadata {
                    dataset_id: foo_bar_id.clone(),
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo_bar_baz" start running at 200ms, finish at 240ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_bar_baz_id.clone()),
                run_since_start: Duration::milliseconds(200),
                finish_in_with: Some((
                    Duration::milliseconds(40),
                    TaskOutcome::Success(
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice-3"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-3"),
                                old_num_blocks: 8,
                                new_num_blocks: 3,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetResetToMetadata {
                    dataset_id: foo_bar_baz_id.clone(),
                }
                .into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(300)).await;
            };

            tokio::join!(
                trigger0_handle,
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

          #1: +50ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Waiting Manual

          #2: +50ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Waiting Manual Executor(task=0, since=50ms)

          #3: +50ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Running(task=0)

          #4: +90ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success

          #5: +90ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Waiting Input(foo)

          #6: +90ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Waiting Input(foo) Executor(task=1, since=90ms)

          #7: +110ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Running(task=1)

          #8: +180ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Finished Success

          #9: +180ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Finished Success
            "foo_bar_baz" ResetToMetadata:
              Flow ID = 2 Waiting Input(foo_bar)

          #10: +180ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Finished Success
            "foo_bar_baz" ResetToMetadata:
              Flow ID = 2 Waiting Input(foo_bar) Executor(task=2, since=180ms)

          #11: +200ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Finished Success
            "foo_bar_baz" ResetToMetadata:
              Flow ID = 2 Running(task=2)

          #12: +240ms:
            "foo" ResetToMetadata:
              Flow ID = 0 Finished Success
            "foo_bar" ResetToMetadata:
              Flow ID = 1 Finished Success
            "foo_bar_baz" ResetToMetadata:
              Flow ID = 2 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_keep_metadata_only_without_reactive_updates() {
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

    let foo_flow_binding = reset_to_metadata_dataset_binding(&foo_id);

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(foo_bar_id.clone(), "foo_bar".to_string());
    test_flow_listener
        .define_dataset_display_name(foo_bar_baz_id.clone(), "foo_bar_baz".to_string());

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 0: "foo" start running at 20ms, finish at 90ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(20),
                finish_in_with: Some((
                    Duration::milliseconds(70),
                    TaskOutcome::Success(
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: CompactionResult::Success {
                                old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                                new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                                old_num_blocks: 5,
                                new_num_blocks: 4,
                            },
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetResetToMetadata {
                    dataset_id: foo_id.clone(),
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(150)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:

            #1: +10ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Waiting Manual

            #2: +10ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Waiting Manual Executor(task=0, since=10ms)

            #3: +20ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Running(task=0)

            #4: +90ms:
              "foo" ResetToMetadata:
                Flow ID = 0 Finished Success

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
    let start_time = harness
        .now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

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
    let start_time = harness
        .now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

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
            harness.now(),
            ingest_dataset_binding(&bar_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(70).into()),
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

            let main_handle = async {
                // 50ms: deleting "foo" in QUEUED state
                harness.advance_time(Duration::milliseconds(50)).await;
                harness.issue_dataset_deleted(&foo_id).await;

                // 120ms: deleting "bar" in SCHEDULED state
                harness.advance_time(Duration::milliseconds(70)).await;
                harness.issue_dataset_deleted(&bar_id).await;

                // 140ms: finish
                harness.advance_time(Duration::milliseconds(20)).await;
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
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #9: +50ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #10: +100ms:
              "bar" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #11: +120ms:
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
async fn test_derived_dataset_triggered_after_input_change() {
    // bar: evaluated after enabling trigger
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

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
            FlowTriggerStopPolicy::default(),
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

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
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
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo" start running at 110ms, finish at 120ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
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
                                has_more: false,
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

            // Task 2: "bar" start running at 130ms, finish at 140ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(130),
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

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(220)).await;
            };

            tokio::join!(task0_handle, task1_handle, task2_handle, main_handle);
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #5: +100ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=100ms)
                Flow ID = 0 Finished Success

            #6: +110ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #7: +120ms:
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #8: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(3/1, until=1120ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #9: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(3/1, until=1120ms) Activating(at=120ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #10: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(3/1, until=1120ms) Activating(at=120ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #11: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=120ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #12: +130ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #13: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #14: +200ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_derived_dataset_trigger_at_startup_with_external_change_detected() {
    // bar: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| {
            Ok(TransformStatus::NewInputDataAvailable {
                input_advancements: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: odf::DatasetID::new_seeded_ed25519(b"foo"),
                    new_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    prev_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    prev_offset: Some(5),
                    new_offset: Some(8),
                }],
            })
        });

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

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
            FlowTriggerStopPolicy::default(),
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

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
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
                                has_more: false,
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
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 130ms, finish at 140ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(130),
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
            let task3_handle = task3_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(220)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                task3_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Batching(3/1, until=1000ms) Activating(at=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Batching(3/1, until=1000ms) Activating(at=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #3: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Running(task=0)

            #4: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Executor(task=1, since=0ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #6: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #7: +40ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #8: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 0 Finished Success

            #9: +110ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 0 Finished Success

            #10: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #11: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(3/1, until=1120ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #12: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(3/1, until=1120ms) Activating(at=120ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #13: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(3/1, until=1120ms) Activating(at=120ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #14: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Executor(task=3, since=120ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #15: +130ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Running(task=3)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #16: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #17: +200ms:
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
            let task0_handle = task0_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(170)).await;
            };

            tokio::join!(
                trigger0_handle,
                trigger1_handle,
                trigger2_handle,
                task0_handle,
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
                Flow ID = 1 Waiting Manual Throttling(for=100ms, wakeup=150ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #6: +150ms:
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
    // baz: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        awaiting_step: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS)), // 10ms,
        mandatory_throttling_period: Some(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS * 10)), /* 100ms */
        mock_dataset_changes: Some(MockDatasetIncrementQueryService::with_increment_between(
            MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 7,
                updated_watermark: None,
            },
        )),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&bar_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(150).into()),
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

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&baz_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(1, Duration::hours(24)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

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
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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

            // Task 1: "bar" start running at 40, finish at 50ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(40),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"fbar-new-slice"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "baz" start running at 60ms, finish at 80ms (simulate longer run)
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::milliseconds(60),
                finish_in_with: Some((
                    Duration::milliseconds(20),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: baz_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
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
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-newest-slice"),
                                has_more: false,
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
            let task3_handle = task3_driver.run();

            // Task 4: "baz" start running at 220ms, finish at 230ms
            let task4_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(4),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "5")]),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::milliseconds(220),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: baz_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task4_handle = task4_driver.run();

            // Task 5: "bar" start running at 250ms, finish at 260ms
            let task5_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(5),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(250),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-newest-slice"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task5_handle = task5_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(500)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                task3_handle,
                task4_handle,
                task5_handle,
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
              "baz" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(7/1, until=86400020ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #6: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(7/1, until=86400020ms) Activating(at=20ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #7: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(7/1, until=86400020ms) Activating(at=20ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #8: +20ms:
              "bar" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
              "baz" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=20ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #9: +40ms:
              "bar" Ingest:
                Flow ID = 1 Running(task=1)
              "baz" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=20ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #10: +50ms:
              "bar" Ingest:
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=20ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #11: +50ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=20ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #12: +60ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 2 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #13: +80ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #14: +80ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Waiting Input(bar) Throttling(for=100ms, wakeup=180ms, shifted=50ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Throttling(for=100ms, wakeup=120ms, shifted=70ms)
                Flow ID = 0 Finished Success

            #15: +120ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Waiting Input(bar) Throttling(for=100ms, wakeup=180ms, shifted=50ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=120ms)
                Flow ID = 0 Finished Success

            #16: +130ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Waiting Input(bar) Throttling(for=100ms, wakeup=180ms, shifted=50ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Running(task=3)
                Flow ID = 0 Finished Success

            #17: +140ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Waiting Input(bar) Throttling(for=100ms, wakeup=180ms, shifted=50ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #18: +140ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Waiting Input(bar) Throttling(for=100ms, wakeup=180ms, shifted=50ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #19: +180ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Waiting Input(bar) Executor(task=4, since=180ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #20: +200ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=5, since=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Waiting Input(bar) Executor(task=4, since=180ms)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #21: +220ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=5, since=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Running(task=4)
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #22: +230ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=5, since=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Throttling(for=100ms, wakeup=240ms, shifted=190ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #23: +240ms:
              "bar" Ingest:
                Flow ID = 4 Waiting AutoPolling Executor(task=5, since=200ms)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #24: +250ms:
              "bar" Ingest:
                Flow ID = 4 Running(task=5)
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #25: +260ms:
              "bar" Ingest:
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #26: +260ms:
              "bar" Ingest:
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 7 Waiting Input(bar) Batching(7/1, until=86400260ms)
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #27: +260ms:
              "bar" Ingest:
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 7 Waiting Input(bar) Throttling(for=100ms, wakeup=330ms, shifted=260ms)
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #28: +260ms:
              "bar" Ingest:
                Flow ID = 8 Waiting AutoPolling Schedule(wakeup=410ms)
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 7 Waiting Input(bar) Throttling(for=100ms, wakeup=330ms, shifted=260ms)
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #29: +330ms:
              "bar" Ingest:
                Flow ID = 8 Waiting AutoPolling Schedule(wakeup=410ms)
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 7 Waiting Input(bar) Executor(task=7, since=330ms)
                Flow ID = 5 Finished Success
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Executor(task=6, since=240ms)
                Flow ID = 3 Finished Success
                Flow ID = 0 Finished Success

            #30: +410ms:
              "bar" Ingest:
                Flow ID = 8 Waiting AutoPolling Executor(task=8, since=410ms)
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 7 Waiting Input(bar) Executor(task=7, since=330ms)
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
    let mut seq_dataset_changes = mockall::Sequence::new();

    // foo: reading after task 0
    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    // foo: reading after task 1
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 7,
                updated_watermark: None,
            })
        });
    // bar: reading after task 2
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 12,
                updated_watermark: None,
            })
        });

    // foo: reading after task 3
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    // foo: reading after task 4
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    // bar: reading after task 5
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 10,
                updated_watermark: None,
            })
        });

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();

    // bar: evaluated after enabling trigger
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(10, Duration::milliseconds(120)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
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
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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

            // Task 1: "foo" start running at 80ms, finish at 90ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(80),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                                has_more: false,
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

            // Task 2: "bar" start running at 100ms, finish at 110ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(100),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task2_handle = task2_driver.run();

            // Task 3: "foo" start running at 150ms, finish at 160ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(150),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice-2",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-3"),
                                has_more: false,
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
            let task3_handle = task3_driver.run();

            // Task 4: "foo" start running at 210ms, finish at 220ms
            let task4_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(4),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "5")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(210),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice-2",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-3"),
                                has_more: false,
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
            let task4_handle = task4_driver.run();

            // Task 5: "bar" start running at 230ms, finish at 240ms
            let task5_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(5),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(230),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task5_handle = task5_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(260)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                task3_handle,
                task4_handle,
                task5_handle,
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
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=140ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=140ms) Activating(at=140ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #6: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=140ms) Activating(at=140ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #7: +70ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=140ms) Activating(at=140ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=1, since=70ms)
                Flow ID = 0 Finished Success

            #8: +80ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=140ms) Activating(at=140ms)
              "foo" Ingest:
                Flow ID = 2 Running(task=1)
                Flow ID = 0 Finished Success

            #9: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=140ms) Activating(at=140ms)
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #10: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(12/10, until=140ms) Activating(at=140ms)
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #11: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(12/10, until=140ms) Activating(at=90ms)
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #12: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(12/10, until=140ms) Activating(at=90ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #13: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Executor(task=2, since=90ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #14: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #15: +110ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #16: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #17: +150ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Running(task=3)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #18: +160ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #19: +160ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/10, until=280ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #20: +160ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/10, until=280ms) Activating(at=280ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #21: +160ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/10, until=280ms) Activating(at=280ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Waiting AutoPolling Schedule(wakeup=210ms)
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #22: +210ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/10, until=280ms) Activating(at=280ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Waiting AutoPolling Executor(task=4, since=210ms)
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #23: +210ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/10, until=280ms) Activating(at=280ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Running(task=4)
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #24: +220ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/10, until=280ms) Activating(at=280ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Finished Success
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #25: +220ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(10/10, until=280ms) Activating(at=280ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Finished Success
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #26: +220ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(10/10, until=280ms) Activating(at=220ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 5 Finished Success
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #27: +220ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(10/10, until=280ms) Activating(at=220ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Schedule(wakeup=270ms)
                Flow ID = 5 Finished Success
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #28: +220ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Executor(task=5, since=220ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Schedule(wakeup=270ms)
                Flow ID = 5 Finished Success
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #29: +230ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Running(task=5)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Schedule(wakeup=270ms)
                Flow ID = 5 Finished Success
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #30: +240ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 6 Waiting AutoPolling Schedule(wakeup=270ms)
                Flow ID = 5 Finished Success
                Flow ID = 3 Finished Success
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
    let mut seq_dataset_changes = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    // foo: reading after task 0
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });

    // foo: reading after task 1
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 3,
                updated_watermark: None,
            })
        });
    // bar: reading after task 3
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 8,
                updated_watermark: None,
            })
        });

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();

    // bar: evaluated after enabling trigger
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(10, Duration::milliseconds(150)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
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
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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

            // Task 1: "foo" start running at 80ms, finish at 90ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(80),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                                has_more: false,
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

            // Task 2 is scheduled, but never runs

            // Task 3: "bar" start running at 180, finish at 190ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(180),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task3_handle = task3_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(250)).await;
            };

            tokio::join!(task0_handle, task1_handle, task3_handle, main_handle);
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
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=170ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #6: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #7: +70ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=1, since=70ms)
                Flow ID = 0 Finished Success

            #8: +80ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 2 Running(task=1)
                Flow ID = 0 Finished Success

            #9: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #10: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(8/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #11: +90ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(8/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #12: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(8/10, until=170ms) Activating(at=170ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=2, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #13: +170ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Executor(task=3, since=170ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=2, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #14: +180ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=3)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=2, since=140ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #15: +190ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=2, since=140ms)
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
    let mut seq_dataset_changes = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    // foo: reading after task 0
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 0,
                updated_watermark: None,
            })
        });

    // foo: reading after task 1
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 0, // no records, just watermark
                updated_watermark: Some(Utc::now()),
            })
        });
    // bar: reading after task 3
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 0, // no records, just watermark
                updated_watermark: Some(Utc::now()),
            })
        });

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    // bar: evaluated after enabling trigger
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(40).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(10, Duration::milliseconds(200)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
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
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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

            // Task 1: "foo" start running at 70ms, finish at 80ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(70),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                                has_more: false,
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

            // Task 2 is scheduled, but never runs

            // Task 3: "bar" start running at 230ms, finish at 240ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(230),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task3_handle = task3_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(300)).await;
            };

            tokio::join!(task0_handle, task1_handle, task3_handle, main_handle);
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
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms)
          "foo" Ingest:
            Flow ID = 0 Finished Success

        #5: +20ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 0 Finished Success

        #6: +20ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Schedule(wakeup=60ms)
            Flow ID = 0 Finished Success

        #7: +60ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 2 Waiting AutoPolling Executor(task=1, since=60ms)
            Flow ID = 0 Finished Success

        #8: +70ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 2 Running(task=1)
            Flow ID = 0 Finished Success

        #9: +80ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #10: +80ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #11: +80ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #12: +120ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Batching(0/10, until=220ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Executor(task=2, since=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #13: +220ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Waiting Input(foo) Executor(task=3, since=220ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Executor(task=2, since=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #14: +230ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Running(task=3)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Executor(task=2, since=120ms)
            Flow ID = 2 Finished Success
            Flow ID = 0 Finished Success

        #15: +240ms:
          "bar" ExecuteTransform:
            Flow ID = 1 Finished Success
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Executor(task=2, since=120ms)
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
    let mut seq_dataset_changes = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    // 'foo': reading after task 0
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    // 'bar': reading after task 1
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });

    // 'foo' : reading after task 2
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 7,
                updated_watermark: None,
            })
        });
    // 'bar': reading after task 3
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 2,
                num_records: 8,
                updated_watermark: None,
            })
        });
    // 'foo' : reading after task 4
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 7,
                updated_watermark: None,
            })
        });
    // 'baz' : reading after task 5
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut seq_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 32,
                updated_watermark: None,
            })
        });

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    // 'baz': evaluated after enabling trigger
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&bar_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(120).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&baz_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(26, Duration::milliseconds(300)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

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
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Task 2: "foo" start running at 110ms, finish at 120ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                                has_more: false,
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
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 160ms, finish at 170ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(160),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice-2"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task3_handle = task3_driver.run();

            // Task 4: "foo" start running at 210ms, finish at 220ms
            let task4_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(4),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "5")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(210),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice-2"),
                                has_more: false,
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
            let task4_handle = task4_driver.run();

            // Task 5: "baz" start running at 230ms, finish at 240ms
            let task5_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(5),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::milliseconds(230),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"baz-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"baz-new-slice-2"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: baz_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task5_handle = task5_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(400)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                task3_handle,
                task4_handle,
                task5_handle,
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
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(5/26, until=320ms)
          "foo" Ingest:
            Flow ID = 0 Finished Success

        #6: +20ms:
          "bar" Ingest:
            Flow ID = 1 Waiting AutoPolling Executor(task=1, since=0ms)
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(5/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 0 Finished Success

        #7: +20ms:
          "bar" Ingest:
            Flow ID = 1 Running(task=1)
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(5/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 0 Finished Success

        #8: +20ms:
          "bar" Ingest:
            Flow ID = 1 Running(task=1)
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(5/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #9: +30ms:
          "bar" Ingest:
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(5/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #10: +30ms:
          "bar" Ingest:
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(10/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #11: +30ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(10/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Schedule(wakeup=100ms)
            Flow ID = 0 Finished Success

        #12: +100ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(10/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Waiting AutoPolling Executor(task=2, since=100ms)
            Flow ID = 0 Finished Success

        #13: +110ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(10/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Running(task=2)
            Flow ID = 0 Finished Success

        #14: +120ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(10/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #15: +120ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(17/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #16: +120ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Schedule(wakeup=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(17/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #17: +150ms:
          "bar" Ingest:
            Flow ID = 4 Waiting AutoPolling Executor(task=3, since=150ms)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(17/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #18: +160ms:
          "bar" Ingest:
            Flow ID = 4 Running(task=3)
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(17/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #19: +170ms:
          "bar" Ingest:
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(17/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #20: +170ms:
          "bar" Ingest:
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(25/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #21: +170ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(25/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Waiting AutoPolling Schedule(wakeup=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #22: +200ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(25/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Waiting AutoPolling Executor(task=4, since=200ms)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #23: +210ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(25/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Running(task=4)
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #24: +220ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(25/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #25: +220ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(32/26, until=320ms) Activating(at=320ms)
          "foo" Ingest:
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #26: +220ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(32/26, until=320ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #27: +220ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Batching(32/26, until=320ms) Activating(at=220ms)
          "foo" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #28: +220ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Waiting Input(foo) Executor(task=5, since=220ms)
          "foo" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #29: +230ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Running(task=5)
          "foo" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #30: +240ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Schedule(wakeup=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #31: +290ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Executor(task=6, since=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 7 Waiting AutoPolling Schedule(wakeup=300ms)
            Flow ID = 5 Finished Success
            Flow ID = 3 Finished Success
            Flow ID = 0 Finished Success

        #32: +300ms:
          "bar" Ingest:
            Flow ID = 6 Waiting AutoPolling Executor(task=6, since=290ms)
            Flow ID = 4 Finished Success
            Flow ID = 1 Finished Success
          "baz" ExecuteTransform:
            Flow ID = 2 Finished Success
          "foo" Ingest:
            Flow ID = 7 Waiting AutoPolling Executor(task=7, since=300ms)
            Flow ID = 5 Finished Success
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

    let foo_flow_binding = compaction_dataset_binding(&foo_id);
    let bar_flow_binding = compaction_dataset_binding(&bar_id);

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
                    Duration::milliseconds(20),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: foo_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(60),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: bar_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Manual trigger for "foo" at 10ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: Some(foo_account_id.clone()),
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Manual trigger for "bar" at 50ms
            let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: bar_flow_binding,
                run_since_start: Duration::milliseconds(50),
                initiator_id: Some(bar_account_id.clone()),
                maybe_forced_flow_config_rule: None,
            });
            let trigger1_handle = trigger1_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(100)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                trigger0_handle,
                trigger1_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    let foo_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_scoped_flow_initiators(FlowScopeDataset::query_for_single_dataset(&foo_id))
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        *std::slice::from_ref(&foo_account_id),
        *foo_dataset_initiators_list
    );

    let bar_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_scoped_flow_initiators(FlowScopeDataset::query_for_single_dataset(&bar_id))
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        *std::slice::from_ref(&bar_account_id),
        *bar_dataset_initiators_list
    );
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

    let foo_flow_binding = compaction_dataset_binding(&foo_id);
    let bar_flow_binding = compaction_dataset_binding(&bar_id);

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
                    Duration::milliseconds(20),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: foo_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(60),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetHardCompact {
                    dataset_id: bar_id.clone(),
                    max_slice_size: None,
                    max_slice_records: None,
                }
                .into_logical_plan(),
            });
            let task1_handle = task1_driver.run();

            // Manual trigger for "foo" at 10ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(10),
                initiator_id: Some(foo_account_id.clone()),
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Manual trigger for "bar" at 50ms
            let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: bar_flow_binding,
                run_since_start: Duration::milliseconds(50),
                initiator_id: Some(bar_account_id.clone()),
                maybe_forced_flow_config_rule: None,
            });
            let trigger1_handle = trigger1_driver.run();

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(100)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                trigger0_handle,
                trigger1_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    let foo_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_scoped_flow_initiators(FlowScopeDataset::query_for_single_dataset(&foo_id))
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        *std::slice::from_ref(&foo_account_id),
        *foo_dataset_initiators_list
    );

    let bar_dataset_initiators_list: Vec<_> = harness
        .flow_query_service
        .list_scoped_flow_initiators(FlowScopeDataset::query_for_single_dataset(&bar_id))
        .await
        .unwrap()
        .matched_stream
        .try_collect()
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        *std::slice::from_ref(&bar_account_id),
        *bar_dataset_initiators_list
    );

    let foo_datasets: Vec<_> = harness
        .dataset_entry_service
        .get_owned_dataset_ids(&foo_account_id)
        .await
        .unwrap();

    let all_datasets_with_flow: Vec<_> = harness
        .flow_query_service
        .filter_flow_scopes_having_flows(
            &foo_datasets
                .iter()
                .map(FlowScopeDataset::make_scope)
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|flow_scope| FlowScopeDataset::new(&flow_scope).dataset_id())
        .collect();

    pretty_assertions::assert_eq!([foo_id], *all_datasets_with_flow);

    let bar_datasets: Vec<_> = harness
        .dataset_entry_service
        .get_owned_dataset_ids(&bar_account_id)
        .await
        .unwrap();

    let all_datasets_with_flow: Vec<_> = harness
        .flow_query_service
        .filter_flow_scopes_having_flows(
            &bar_datasets
                .iter()
                .map(FlowScopeDataset::make_scope)
                .collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|flow_scope| FlowScopeDataset::new(&flow_scope).dataset_id())
        .collect();

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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
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

            // Manual abort for "foo" at 80ms
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(1),
                abort_since_start: Duration::milliseconds(80),
            });
            let abort0_handle = abort0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, abort0_handle, sim_handle);
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #5: +80ms:
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
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

            // Manual abort for "foo" at 90ms
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(1),
                abort_since_start: Duration::milliseconds(90),
            });
            let abort0_handle = abort0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, abort0_handle, sim_handle);
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #5: +70ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=70ms)
                Flow ID = 0 Finished Success

            #6: +90ms:
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: start running at 10ms, finish at 110ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(100),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let foo_task0_handle = foo_task0_driver.run();

            // Manual abort for "foo" at 50ms, which is within task run period
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(0),
                abort_since_start: Duration::milliseconds(50),
            });
            let abort0_handle = abort0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(foo_task0_handle, abort0_handle, sim_handle);
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
    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    mock_dataset_changes
        .expect_get_increment_between()
        .times(2)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    let mut evaluation_seq = mockall::Sequence::new();
    // bar: initially when enabling a trigger
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .in_sequence(&mut evaluation_seq)
        .returning(|_| Ok(TransformStatus::UpToDate));
    // bar: after foo finishes task 0
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .in_sequence(&mut evaluation_seq)
        .returning(|_| {
            Ok(TransformStatus::NewInputDataAvailable {
                input_advancements: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: odf::DatasetID::new_seeded_ed25519(b"foo"),
                    new_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    prev_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    prev_offset: Some(5),
                    new_offset: Some(8),
                }],
            })
        });

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
        ..Default::default()
    });

    // Create a "foo" root dataset, and configure ingestion schedule every 50ms
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    // Create a "bar" derived dataset, and configure reactive trigger
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::empty()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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
            let foo_task0_handle = foo_task0_driver.run();

            // Task 1: "bar" start running at 30ms, finish at 70ms
            let bar_task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(30),
                finish_in_with: Some((
                    Duration::milliseconds(40),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let bar_task1_handle = bar_task1_driver.run();

            // Manual abort for "foo" at 50ms, which is after flow 0 has finished, and
            // before flow 1 has started This should stop the trigger of "foo"
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(2),
                abort_since_start: Duration::milliseconds(50),
            });
            let abort0_handle = abort0_driver.run();

            // Manual abort for "bar" at 50ms.
            // This would only interrupt the "bar" task, but not it's reactive trigger
            let abort1_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(1),
                abort_since_start: Duration::milliseconds(50),
            });
            let abort1_handle = abort1_driver.run();

            // Manual launch of "foo" at 130ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: ingest_dataset_binding(&foo_id),
                run_since_start: Duration::milliseconds(130),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            // Task 2: "foo" start running at 140ms, finish at 150ms
            let foo_task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "3")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(140),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-newer-slice"),
                                has_more: false,
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
            let foo_task2_handle = foo_task2_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(250));
            tokio::join!(
                foo_task0_handle,
                bar_task1_handle,
                abort0_handle,
                abort1_handle,
                trigger0_handle,
                foo_task2_handle,
                sim_handle
            );
        })
        .await
        .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

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
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/0, until=20ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/0, until=20ms) Activating(at=20ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #6: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/0, until=20ms) Activating(at=20ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #7: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Executor(task=1, since=20ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #8: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=70ms)
                Flow ID = 0 Finished Success

            #9: +50ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #10: +50ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #11: +130ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Waiting Manual
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #12: +130ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Waiting Manual Executor(task=2, since=130ms)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #13: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Running(task=2)
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #14: +150ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #15: +150ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/0, until=150ms)
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #16: +150ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Batching(5/0, until=150ms) Activating(at=150ms)
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Aborted
                Flow ID = 0 Finished Success

            #17: +150ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting Input(foo) Executor(task=3, since=150ms)
                Flow ID = 1 Finished Aborted
              "foo" Ingest:
                Flow ID = 3 Finished Success
                Flow ID = 2 Finished Aborted
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
    let start_time = harness
        .now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

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
async fn test_respect_last_success_time_for_derived_dataset_when_activate_configuration() {
    // foo: reading after task 0
    // bar: reading after task 1
    // foo: reading after task 2
    // foo: reading after reactivation of bar
    // bar: reading after task 3
    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    mock_dataset_changes
        .expect_get_increment_between()
        .times(5)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });

    // bar: queried at startup and at enable time
    let mut seq_eval_transform = mockall::Sequence::new();

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .in_sequence(&mut seq_eval_transform)
        .returning(|_| Ok(TransformStatus::UpToDate));
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .in_sequence(&mut seq_eval_transform)
        .returning(|_| {
            Ok(TransformStatus::NewInputDataAvailable {
                input_advancements: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: odf::DatasetID::new_seeded_ed25519(b"foo"),
                    new_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    prev_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    prev_offset: Some(5),
                    new_offset: Some(10),
                }],
            })
        });

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(mock_dataset_changes),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(100).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(1, Duration::milliseconds(300)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

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
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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

            // Task 2: "foo" start running at 110ms, finish at 120ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(foo_id.clone()),
                run_since_start: Duration::milliseconds(110),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-new-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-newest-slice"),
                                has_more: false,
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
            let task2_handle = task2_driver.run();

            // Task 3: "bar" start running at 180ms, finish at 190ms
            let task3_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(3),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(180),
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
            let task3_handle = task3_driver.run();

            // Main simulation script
            let main_handle = async {
                // 60ms: Pause flow config before next "foo" runs
                harness.advance_time(Duration::milliseconds(60)).await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(60),
                        &transform_dataset_binding(&bar_id),
                    )
                    .await;

                // 170ms: Wake up after planned "foo" run
                harness.advance_time(Duration::milliseconds(110)).await;
                harness
                    .resume_flow(
                        start_time + Duration::milliseconds(170),
                        &transform_dataset_binding(&bar_id),
                    )
                    .await;

                // 270ms: finish
                harness.advance_time(Duration::milliseconds(100)).await;
            };

            tokio::join!(
                task0_handle,
                task1_handle,
                task2_handle,
                task3_handle,
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
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/1, until=320ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #5: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/1, until=320ms) Activating(at=20ms)
              "foo" Ingest:
                Flow ID = 0 Finished Success

            #6: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Batching(5/1, until=320ms) Activating(at=20ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #7: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting Input(foo) Executor(task=1, since=20ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #8: +30ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #9: +40ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=120ms)
                Flow ID = 0 Finished Success

            #10: +110ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 0 Finished Success

            #11: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=120ms)
                Flow ID = 0 Finished Success

            #12: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #13: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=220ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #14: +170ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting ExternallyDetectedChange Batching(5/1, until=470ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=220ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #15: +170ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting ExternallyDetectedChange Batching(5/1, until=470ms) Activating(at=170ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=220ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #16: +170ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Waiting ExternallyDetectedChange Executor(task=3, since=170ms)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=220ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #17: +180ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Running(task=3)
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=220ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #18: +190ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=220ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

            #19: +220ms:
              "bar" ExecuteTransform:
                Flow ID = 4 Finished Success
                Flow ID = 1 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=4, since=220ms)
                Flow ID = 2 Finished Success
                Flow ID = 0 Finished Success

      "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_restart_batching_condition_deadline_on_each_reactivation() {
    let mut sequence_dataset_changes = mockall::Sequence::new();
    let mut sequence_transform_evaluator = mockall::Sequence::new();

    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    // foo: checked after task 0
    mock_dataset_changes
        .expect_get_increment_between()
        .times(3)
        .in_sequence(&mut sequence_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });
    // bar: checked after task 1
    mock_dataset_changes
        .expect_get_increment_between()
        .times(1)
        .in_sequence(&mut sequence_dataset_changes)
        .returning(|_, _, _| {
            Ok(MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            })
        });

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    // bar: checked initially
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .in_sequence(&mut sequence_transform_evaluator)
        .returning(|_| Ok(TransformStatus::UpToDate));
    // bar: checked twice after resuming
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(2)
        .in_sequence(&mut sequence_transform_evaluator)
        .returning(|_| {
            Ok(TransformStatus::NewInputDataAvailable {
                input_advancements: vec![odf::metadata::ExecuteTransformInput {
                    dataset_id: odf::DatasetID::new_seeded_ed25519(b"foo"),
                    new_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-new-slice")),
                    prev_block_hash: Some(odf::Multihash::from_digest_sha3_256(b"foo-old-slice")),
                    prev_offset: Some(0),
                    new_offset: Some(5),
                }],
            })
        });

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(300).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(100, Duration::milliseconds(100)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    // Remember start time
    let start_time = harness
        .now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

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
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"foo-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                                has_more: false,
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

            // Task 1: "bar" start running at 180ms, finish at 190ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "4")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(180),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(
                        TaskResultDatasetUpdate {
                            pull_result: PullResult::Updated {
                                old_head: Some(odf::Multihash::from_digest_sha3_256(
                                    b"bar-old-slice",
                                )),
                                new_head: odf::Multihash::from_digest_sha3_256(b"bar-new-slice"),
                                has_more: false,
                            },
                            data_increment: None,
                        }
                        .into_task_result(),
                    ),
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
                // 50ms: Pause "bar" flow config before next "foo" runs
                harness.advance_time(Duration::milliseconds(50)).await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(50),
                        &transform_dataset_binding(&bar_id),
                    )
                    .await;

                // 80ms: Wake up "bar"
                harness.advance_time(Duration::milliseconds(80)).await;
                harness
                    .resume_flow(
                        start_time + Duration::milliseconds(80),
                        &transform_dataset_binding(&bar_id),
                    )
                    .await;

                // 120ms: Pause "bar" again
                harness.advance_time(Duration::milliseconds(40)).await;
                harness
                    .pause_flow(
                        start_time + Duration::milliseconds(120),
                        &transform_dataset_binding(&bar_id),
                    )
                    .await;

                // 170ms: Resume "bar"
                harness.advance_time(Duration::milliseconds(50)).await;
                harness
                    .resume_flow(
                        start_time + Duration::milliseconds(170),
                        &transform_dataset_binding(&bar_id),
                    )
                    .await;

                // 400ms: finish
                harness.advance_time(Duration::milliseconds(230)).await;
            };

            tokio::join!(task0_handle, task1_handle, main_handle);
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
            "bar" ExecuteTransform:
              Flow ID = 1 Waiting Input(foo) Batching(5/100, until=120ms)
            "foo" Ingest:
              Flow ID = 0 Finished Success

          #5: +20ms:
            "bar" ExecuteTransform:
              Flow ID = 1 Waiting Input(foo) Batching(5/100, until=120ms) Activating(at=120ms)
            "foo" Ingest:
              Flow ID = 0 Finished Success

          #6: +20ms:
            "bar" ExecuteTransform:
              Flow ID = 1 Waiting Input(foo) Batching(5/100, until=120ms) Activating(at=120ms)
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #7: +50ms:
            "bar" ExecuteTransform:
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #8: +80ms:
            "bar" ExecuteTransform:
              Flow ID = 3 Waiting ExternallyDetectedChange Batching(5/100, until=180ms)
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #9: +80ms:
            "bar" ExecuteTransform:
              Flow ID = 3 Waiting ExternallyDetectedChange Batching(5/100, until=180ms) Activating(at=180ms)
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #10: +170ms:
            "bar" ExecuteTransform:
              Flow ID = 3 Finished Aborted
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #11: +170ms:
            "bar" ExecuteTransform:
              Flow ID = 4 Waiting ExternallyDetectedChange Batching(5/100, until=270ms)
              Flow ID = 3 Finished Aborted
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #12: +170ms:
            "bar" ExecuteTransform:
              Flow ID = 4 Waiting ExternallyDetectedChange Batching(5/100, until=270ms) Activating(at=270ms)
              Flow ID = 3 Finished Aborted
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #13: +180ms:
            "bar" ExecuteTransform:
              Flow ID = 4 Running(task=1)
              Flow ID = 3 Finished Aborted
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #14: +190ms:
            "bar" ExecuteTransform:
              Flow ID = 4 Finished Success
              Flow ID = 3 Finished Aborted
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #15: +270ms:
            "bar" ExecuteTransform:
              Flow ID = 4 Waiting ExternallyDetectedChange Executor(task=1, since=270ms)
              Flow ID = 3 Finished Aborted
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Schedule(wakeup=320ms)
              Flow ID = 0 Finished Success

          #16: +320ms:
            "bar" ExecuteTransform:
              Flow ID = 4 Finished Success
              Flow ID = 3 Finished Aborted
              Flow ID = 1 Finished Aborted
            "foo" Ingest:
              Flow ID = 2 Waiting AutoPolling Executor(task=2, since=320ms)
              Flow ID = 0 Finished Success

          "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_recover_pending_batching_condition_deadline_after_reboot() {
    let harness = FlowHarness::new();

    // Create a "foo" root dataset
    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    // Create a "bar" derived dataset
    let bar_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("bar"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    // Create a "baz" derived dataset
    let baz_id = harness
        .create_derived_dataset(
            odf::DatasetAlias {
                dataset_name: odf::DatasetName::new_unchecked("baz"),
                account_name: None,
            },
            vec![foo_id.clone()],
        )
        .await;

    let bar_transform_binding = transform_dataset_binding(&bar_id);
    let baz_transform_binding = transform_dataset_binding(&baz_id);

    // Remember start time
    let start_time = harness
        .now()
        .duration_round(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS))
        .unwrap();

    // Set reactive trigger for "baz"
    harness
        .set_flow_trigger(
            start_time - Duration::milliseconds(400),
            baz_transform_binding.clone(),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(100, Duration::milliseconds(300)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Set reactive trigger for "bar"
    harness
        .set_flow_trigger(
            start_time - Duration::milliseconds(200),
            bar_transform_binding.clone(),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(100, Duration::milliseconds(300)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Mimic we are recovering from server restart,
    // where a waiting flow for "bar" existed already and has a deadline in future,
    // but smaller than full deadline from restart
    let flow_id_bar = harness.flow_event_store.new_flow_id().await.unwrap();
    harness
        .flow_event_store
        .save_events(
            &flow_id_bar,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_time - Duration::milliseconds(200),
                    flow_id: flow_id_bar,
                    flow_binding: bar_transform_binding.clone(),
                    activation_cause: FlowActivationCause::ResourceUpdate(
                        FlowActivationCauseResourceUpdate {
                            activation_time: start_time - Duration::milliseconds(200),
                            resource_type: DATASET_RESOURCE_TYPE.to_string(),
                            changes: ResourceChanges::NewData(ResourceDataChanges {
                                blocks_added: 1,
                                records_added: 5,
                                new_watermark: None,
                            }),
                            details: serde_json::to_value(DatasetResourceUpdateDetails {
                                dataset_id: foo_id.clone(),
                                source: DatasetUpdateSource::ExternallyDetectedChange,
                                old_head_maybe: None,
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                            })
                            .unwrap(),
                        },
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    event_time: start_time - Duration::milliseconds(200),
                    flow_id: flow_id_bar,
                    flow_binding: bar_transform_binding.clone(),
                    start_condition: FlowStartCondition::Reactive(FlowStartConditionReactive {
                        active_rule: ReactiveRule::new(
                            BatchingRule::try_buffering(100, Duration::milliseconds(300)).unwrap(),
                            BreakingChangeRule::NoAction,
                        ),
                        batching_deadline: start_time + Duration::milliseconds(100),
                        last_activation_cause_index: 0,
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    event_time: start_time - Duration::milliseconds(200),
                    flow_id: flow_id_bar,
                    flow_binding: bar_transform_binding.clone(),
                    scheduled_for_activation_at: start_time + Duration::milliseconds(100),
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Mimic flow for "baz" also existed, but deadline is already in the past
    let flow_id_baz = harness.flow_event_store.new_flow_id().await.unwrap();
    harness
        .flow_event_store
        .save_events(
            &flow_id_baz,
            None,
            vec![
                FlowEventInitiated {
                    event_time: start_time - Duration::milliseconds(400),
                    flow_id: flow_id_baz,
                    flow_binding: baz_transform_binding.clone(),
                    activation_cause: FlowActivationCause::ResourceUpdate(
                        FlowActivationCauseResourceUpdate {
                            activation_time: start_time - Duration::milliseconds(400),
                            resource_type: DATASET_RESOURCE_TYPE.to_string(),
                            changes: ResourceChanges::NewData(ResourceDataChanges {
                                blocks_added: 1,
                                records_added: 5,
                                new_watermark: None
                            }),
                            details: serde_json::to_value(DatasetResourceUpdateDetails {
                                dataset_id: foo_id.clone(),
                                source: DatasetUpdateSource::ExternallyDetectedChange,
                                old_head_maybe: None,
                                new_head: odf::Multihash::from_digest_sha3_256(b"foo-new-slice"),
                            }).unwrap(),
                        }
                    ),
                    config_snapshot: None,
                    retry_policy: None,
                }
                .into(),
                FlowEventStartConditionUpdated {
                    event_time: start_time - Duration::milliseconds(400),
                    flow_id: flow_id_baz,
                    flow_binding: baz_transform_binding.clone(),
                    start_condition: FlowStartCondition::Reactive(FlowStartConditionReactive {
                        active_rule: ReactiveRule::new(
                          BatchingRule::try_buffering(100, Duration::milliseconds(300)).unwrap(),
                          BreakingChangeRule::NoAction,
                        ),
                        batching_deadline: start_time - Duration::milliseconds(100), // in the past
                        last_activation_cause_index: 0,
                    }),
                    last_activation_cause_index: 0,
                }
                .into(),
                FlowEventScheduledForActivation {
                    event_time: start_time - Duration::milliseconds(400),
                    flow_id: flow_id_baz,
                    flow_binding: baz_transform_binding.clone(),
                    scheduled_for_activation_at: start_time - Duration::milliseconds(100), // in the past
                }
                .into(),
            ],
        )
        .await
        .unwrap();

    // Run scheduler concurrently with simulation script
    harness
        .simulate_flow_scenario(|| async {
            // "baz" Task 0: start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(
                    METADATA_TASK_FLOW_ID,
                    flow_id_baz.to_string(),
                )]),
                dataset_id: Some(baz_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(10),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: baz_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // "bar" Task 1: start running at 110ms, finish at 120ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(
                    METADATA_TASK_FLOW_ID,
                    flow_id_bar.to_string(),
                )]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(110),
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

            // Main simulation boundary - 130ms total
            let sim_handle = harness.advance_time(Duration::milliseconds(150));
            tokio::join!(task0_handle, task1_handle, sim_handle);
        })
        .await
        .unwrap();

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());
    test_flow_listener.define_dataset_display_name(baz_id.clone(), "baz".to_string());

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +-100ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting ExternallyDetectedChange Batching(5/100, until=100ms) Activating(at=100ms)
              "baz" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Executor(task=0, since=-100ms)

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting ExternallyDetectedChange Batching(5/100, until=100ms) Activating(at=100ms)
              "baz" ExecuteTransform:
                Flow ID = 1 Waiting ExternallyDetectedChange Batching(5/100, until=-100ms) Activating(at=-100ms)

            #2: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting ExternallyDetectedChange Batching(5/100, until=100ms) Activating(at=100ms)
              "baz" ExecuteTransform:
                Flow ID = 1 Running(task=0)

            #3: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting ExternallyDetectedChange Batching(5/100, until=100ms) Activating(at=100ms)
              "baz" ExecuteTransform:
                Flow ID = 1 Finished Success

            #4: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting ExternallyDetectedChange Executor(task=1, since=100ms)
              "baz" ExecuteTransform:
                Flow ID = 1 Finished Success

            #5: +110ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Running(task=1)
              "baz" ExecuteTransform:
                Flow ID = 1 Finished Success

            #6: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "baz" ExecuteTransform:
                Flow ID = 1 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

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
async fn test_dependencies_flow_trigger_instantly_with_zero_batching_rule() {
    // bar: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(1)
        .returning(|_| Ok(TransformStatus::UpToDate));

    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mock_dataset_changes: Some(MockDatasetIncrementQueryService::with_increment_between(
            MetadataChainIncrementInterval {
                num_blocks: 1,
                num_records: 0,
                updated_watermark: None,
            },
        )),
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
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
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    harness
        .set_flow_trigger(
            harness.now(),
            transform_dataset_binding(&bar_id),
            FlowTriggerRule::Reactive(ReactiveRule::empty()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // Enforce dependency graph initialization

    // Flow listener will collect snapshots at important moments of time
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
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Task 1: "foo" start running at 110ms, finish at 120ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
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
                                has_more: false,
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

            // Task 2: "bar" start running at 130ms, finish at 140ms
            let task2_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(2),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "2")]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(130),
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

            // Main simulation script
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(220)).await;
            };

            tokio::join!(task0_handle, task1_handle, task2_handle, main_handle);
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
                Flow ID = 1 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 0 Finished Success

            #5: +100ms:
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=1, since=100ms)
                Flow ID = 0 Finished Success

            #6: +110ms:
              "foo" Ingest:
                Flow ID = 1 Running(task=1)
                Flow ID = 0 Finished Success

            #7: +120ms:
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #8: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(0/0, until=120ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #9: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(0/0, until=120ms) Activating(at=120ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #10: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Batching(0/0, until=120ms) Activating(at=120ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #11: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Waiting Input(foo) Executor(task=2, since=120ms)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #12: +130ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Running(task=2)
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #13: +140ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            #14: +200ms:
              "bar" ExecuteTransform:
                Flow ID = 2 Finished Success
              "foo" Ingest:
                Flow ID = 3 Waiting AutoPolling Executor(task=3, since=200ms)
                Flow ID = 1 Finished Success
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
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

// TODO next:
//  - derived more than 1 level
