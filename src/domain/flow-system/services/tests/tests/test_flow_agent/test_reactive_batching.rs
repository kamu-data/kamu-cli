// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


use chrono::{Duration, Utc};
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
    SCHEDULING_ALIGNMENT_MS,
    TaskDriverArgs,
};

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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 1 => "bar" ExecuteTransform Flow ID = 2
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 2 => "bar" ExecuteTransform Flow ID = 3
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "baz" ExecuteTransform Flow ID = 2
            "bar" Ingest Flow ID = 1 => "baz" ExecuteTransform Flow ID = 5
            "foo" Ingest Flow ID = 3 => "baz" ExecuteTransform Flow ID = 5
            "bar" Ingest Flow ID = 4 => "baz" ExecuteTransform Flow ID = 7
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "bar" ExecuteTransform Flow ID = 1
            "foo" Ingest Flow ID = 2 => "bar" ExecuteTransform Flow ID = 1
            "foo" Ingest Flow ID = 3 => "bar" ExecuteTransform Flow ID = 4
            "foo" Ingest Flow ID = 5 => "bar" ExecuteTransform Flow ID = 4
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "bar" ExecuteTransform Flow ID = 1
            "foo" Ingest Flow ID = 2 => "bar" ExecuteTransform Flow ID = 1
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "bar" ExecuteTransform Flow ID = 1
            "foo" Ingest Flow ID = 2 => "bar" ExecuteTransform Flow ID = 1
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "baz" ExecuteTransform Flow ID = 2
            "bar" Ingest Flow ID = 1 => "baz" ExecuteTransform Flow ID = 2
            "foo" Ingest Flow ID = 3 => "baz" ExecuteTransform Flow ID = 2
            "bar" Ingest Flow ID = 4 => "baz" ExecuteTransform Flow ID = 2
            "foo" Ingest Flow ID = 5 => "baz" ExecuteTransform Flow ID = 2
            "#
        ),
        harness.activation_links_report().await
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
        .times(3)
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "bar" ExecuteTransform Flow ID = 1
            "#
        ),
        harness.activation_links_report().await
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "bar" ExecuteTransform Flow ID = 1
            "#
        ),
        harness.activation_links_report().await
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
    let start_time = harness.aligned_now();

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

    pretty_assertions::assert_eq!("", harness.activation_links_report().await);
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 1 => "bar" ExecuteTransform Flow ID = 2
            "#
        ),
        harness.activation_links_report().await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reactive_trigger_with_pending_flow_reacts_after_restart() {
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
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

    // Triggers set before the restart
    harness
        .set_flow_trigger(
            harness.now(),
            ingest_dataset_binding(&foo_id),
            FlowTriggerRule::Schedule(Duration::milliseconds(80).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let bar_transform_binding = transform_dataset_binding(&bar_id);
    harness
        .set_flow_trigger(
            harness.now(),
            bar_transform_binding.clone(),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(1, Duration::seconds(1)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // "bar" had a pending flow when the server went down
    let bar_flow_id = harness
        .schedule_flow_for_activation(
            &bar_transform_binding,
            harness.now() + Duration::milliseconds(50),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: "foo" start running at 10ms, finish at 20ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]),
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

            // Task 1: the pending "bar" flow, start running at 60ms, finish at 70ms
            let task1_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(1),
                task_metadata: TaskMetadata::from(vec![(
                    METADATA_TASK_FLOW_ID,
                    bar_flow_id.to_string(),
                )]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(60),
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

            // Task 2: "foo" start running at 110ms, finish at 120ms with new data
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

            let main_handle = harness.advance_time(Duration::milliseconds(150));

            tokio::join!(task0_handle, task1_handle, task2_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 1 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 1 Running(task=0)

            #3: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 1 Finished Success

            #4: +20ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=50ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success

            #5: +50ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Executor(task=1, since=50ms)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success

            #6: +60ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Running(task=1)
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success

            #7: +70ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Schedule(wakeup=100ms)
                Flow ID = 1 Finished Success

            #8: +100ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 2 Waiting AutoPolling Executor(task=2, since=100ms)
                Flow ID = 1 Finished Success

            #9: +110ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 2 Running(task=2)
                Flow ID = 1 Finished Success

            #10: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 1 Finished Success

            #11: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(3/1, until=1120ms)
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 1 Finished Success

            #12: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(3/1, until=1120ms) Activating(at=120ms)
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 2 Finished Success
                Flow ID = 1 Finished Success

            #13: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Batching(3/1, until=1120ms) Activating(at=120ms)
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 1 Finished Success

            #14: +120ms:
              "bar" ExecuteTransform:
                Flow ID = 3 Waiting Input(foo) Executor(task=3, since=120ms)
                Flow ID = 0 Finished Success
              "foo" Ingest:
                Flow ID = 4 Waiting AutoPolling Schedule(wakeup=200ms)
                Flow ID = 2 Finished Success
                Flow ID = 1 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 2 => "bar" ExecuteTransform Flow ID = 3
            "#
        ),
        harness.activation_links_report().await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_restored_sensor_hears_input_change_delivered_after_restart() {
    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
    mock_transform_flow_evaluator
        .expect_evaluate_transform_status()
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

    let bar_transform_binding = transform_dataset_binding(&bar_id);
    harness
        .set_flow_trigger(
            harness.now(),
            bar_transform_binding.clone(),
            FlowTriggerRule::Reactive(ReactiveRule::new(
                BatchingRule::try_buffering(1, Duration::seconds(1)).unwrap(),
                BreakingChangeRule::NoAction,
            )),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    // "bar" had a pending flow when the server went down, due right away
    let bar_flow_id = harness
        .schedule_flow_for_activation(&bar_transform_binding, harness.now())
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

    let foo_old_head = odf::Multihash::from_digest_sha3_256(b"foo-old-slice");
    let foo_pushed_head = odf::Multihash::from_digest_sha3_256(b"foo-pushed-slice");

    harness
        .simulate_flow_scenario(|| async {
            // Task 0: the pending "bar" flow, start running at 10ms, finish at 40ms
            let task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(
                    METADATA_TASK_FLOW_ID,
                    bar_flow_id.to_string(),
                )]),
                dataset_id: Some(bar_id.clone()),
                run_since_start: Duration::milliseconds(10),
                finish_in_with: Some((
                    Duration::milliseconds(30),
                    TaskOutcome::Success(TaskResult::empty()),
                )),
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: bar_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // "foo" was pushed to while the server was down; the outbox delivers
            // that while the "bar" task runs
            let push_handle = harness.issue_dataset_ingested_over_http(
                Duration::milliseconds(20),
                &foo_id,
                &foo_old_head,
                &foo_pushed_head,
            );

            let main_handle = harness.advance_time(Duration::milliseconds(60));

            tokio::join!(task0_handle, push_handle, main_handle);
        })
        .await
        .unwrap();

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            #0: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Schedule(wakeup=0ms)

            #1: +0ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Waiting AutoPolling Executor(task=0, since=0ms)

            #2: +10ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Running(task=0)

            #3: +40ms:
              "bar" ExecuteTransform:
                Flow ID = 0 Finished Success

            #4: +40ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting HttpIngest Throttling(for=20ms, wakeup=60ms, shifted=20ms)
                Flow ID = 0 Finished Success

            #5: +60ms:
              "bar" ExecuteTransform:
                Flow ID = 1 Waiting HttpIngest Executor(task=1, since=60ms)
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    pretty_assertions::assert_eq!("", harness.activation_links_report().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO next:
//  - derived more than 1 level
