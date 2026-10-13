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
use kamu_core::{CompactionResult, ResetResult, TransformStatus};
use kamu_flow_system::*;
use kamu_task_system::*;

use crate::tests::{
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    ManualFlowActivationArgs,
    SCHEDULING_ALIGNMENT_MS,
    TaskDriverArgs,
};

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

    pretty_assertions::assert_eq!("", harness.activation_links_report().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_trigger_derivatives_reactively() {
    // foo.bar: evaluated after enabling trigger and after reset to metadata
    // foo.baz: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(3)
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Reset Flow ID = 0 => "foo_bar" ResetToMetadata Flow ID = 1
            "#
        ),
        harness.activation_links_report().await
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

    // foo.bar: evaluated after enabling trigger and after reset to metadata
    // foo.baz: evaluated after enabling trigger
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(3)
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" HardCompaction Flow ID = 0 => "foo_bar" ResetToMetadata Flow ID = 1
            "#
        ),
        harness.activation_links_report().await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_manual_trigger_keep_metadata_only_with_reactive_updates() {
    // foo.bar: evaluated after enabling trigger and after reset to metadata
    // foo.bar.baz: evaluated after enabling trigger and after reset to metadata
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
        .times(4)
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" ResetToMetadata Flow ID = 0 => "foo_bar" ResetToMetadata Flow ID = 1
            "foo_bar" ResetToMetadata Flow ID = 1 => "foo_bar_baz" ResetToMetadata Flow ID = 2
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!("", harness.activation_links_report().await);
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
