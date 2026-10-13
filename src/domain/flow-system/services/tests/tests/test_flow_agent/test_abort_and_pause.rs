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
use kamu_core::{PullResult, TransformStatus};
use kamu_datasets::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::*;
use odf::dataset::MetadataChainIncrementInterval;

use crate::tests::{
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    ManualFlowAbortArgs,
    ManualFlowActivationArgs,
    TaskDriverArgs,
};

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

    // Counted as aborted only, even though its cancelled task finishes later
    let ingest_flow_type = ingest_dataset_binding(&foo_id).flow_type;
    assert_eq!(harness.aborted_flows(&ingest_flow_type), 1);
    assert_eq!(harness.completed_flows(&ingest_flow_type, "success"), 0);
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

    pretty_assertions::assert_eq!(
        indoc::indoc!(
            r#"
            "foo" Ingest Flow ID = 0 => "bar" ExecuteTransform Flow ID = 1
            "foo" Ingest Flow ID = 3 => "bar" ExecuteTransform Flow ID = 4
            "#
        ),
        harness.activation_links_report().await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_abort_flow_waiting_for_retry() {
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

            // Task 0: "foo" start running at 30ms, fail at 40ms, retry planned at 1040ms
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

            // Abort the flow at 500ms, while it waits for the retry
            let abort0_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(0),
                abort_since_start: Duration::milliseconds(500),
            });
            let abort0_handle = abort0_driver.run();

            // Main simulation script: well past the planned retry
            let main_handle = async {
                harness.advance_time(Duration::milliseconds(1100)).await;
            };

            tokio::join!(trigger0_handle, task0_handle, abort0_handle, main_handle);
        })
        .await
        .unwrap();

    // The retry never happened
    assert!(!harness.task_exists(TaskID::new(1)).await);

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

            #5: +500ms:
              "foo" Ingest:
                Flow ID = 0 Finished Aborted

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_trigger_while_flow_waits_for_executor() {
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
            FlowTriggerRule::Schedule(Duration::milliseconds(50).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

    harness
        .simulate_flow_scenario(|| async {
            // Task 0 is created at 0ms, but no executor picks it up
            harness.advance_time(Duration::milliseconds(30)).await;
            assert!(harness.task_exists(TaskID::new(0)).await);

            harness.pause_flow(harness.now(), &foo_flow_binding).await;
            assert!(harness.task_cancellation_requested(TaskID::new(0)).await);

            // Nothing is scheduled any more
            harness.advance_time(Duration::milliseconds(150)).await;
            assert!(!harness.task_exists(TaskID::new(1)).await);
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

            #2: +30ms:
              "foo" Ingest:
                Flow ID = 0 Finished Aborted

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_abort_flow_waiting_in_throttling() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        mandatory_throttling_period: Some(Duration::milliseconds(100)),
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let foo_flow_binding = ingest_dataset_binding(&foo_id);

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
                expected_logical_plan: LogicalPlanDatasetUpdate {
                    dataset_id: foo_id.clone(),
                    fetch_uncacheable: false,
                }
                .into_logical_plan(),
            });
            let task0_handle = task0_driver.run();

            // Manual trigger for "foo" at 50ms: throttled until 140ms
            let trigger1_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(50),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger1_handle = trigger1_driver.run();

            // Abort the throttled flow at 80ms
            let abort1_driver = harness.manual_flow_abort_driver(ManualFlowAbortArgs {
                flow_id: FlowID::new(1),
                abort_since_start: Duration::milliseconds(80),
            });
            let abort1_handle = abort1_driver.run();

            let main_handle = async {
                harness.advance_time(Duration::milliseconds(200)).await;
            };

            tokio::join!(
                trigger0_handle,
                task0_handle,
                trigger1_handle,
                abort1_handle,
                main_handle
            );
        })
        .await
        .unwrap();

    // The throttled activation never happened
    assert!(!harness.task_exists(TaskID::new(1)).await);

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
                Flow ID = 0 Finished Success

            #5: +50ms:
              "foo" Ingest:
                Flow ID = 1 Waiting Manual Throttling(for=100ms, wakeup=140ms, shifted=50ms)
                Flow ID = 0 Finished Success

            #6: +80ms:
              "foo" Ingest:
                Flow ID = 1 Finished Aborted
                Flow ID = 0 Finished Success

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
