// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


use chrono::Duration;
use futures::TryStreamExt;
use kamu_accounts::{AccountConfig, CurrentAccountSubject};
use kamu_adapter_flow_dataset::*;
use kamu_adapter_task_dataset::*;
use kamu_core::{PullResult, TransformStatus};
use kamu_datasets::DatasetEntryServiceExt;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::*;
use odf::dataset::MetadataChainIncrementInterval;

use crate::tests::{
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    ManualFlowActivationArgs,
    TaskDriverArgs,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_all_flow_initiators() {
    let foo = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("foo"));
    let bar = AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("bar"));

    let subject_foo = CurrentAccountSubject::new_test_with(&foo.account_name);
    let subject_bar = CurrentAccountSubject::new_test_with(&bar.account_name);

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

    let subject_foo = CurrentAccountSubject::new_test_with(&foo.account_name);
    let subject_bar = CurrentAccountSubject::new_test_with(&bar.account_name);

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

    let foo_account_id = odf::metadata::testing::account_id(subject_foo.account_name());
    let bar_account_id = odf::metadata::testing::account_id(subject_bar.account_name());

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

    pretty_assertions::assert_eq!("", harness.activation_links_report().await);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_duration_starts_at_manual_activation_before_schedule() {
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
            FlowTriggerRule::Schedule(Duration::milliseconds(90).into()),
            FlowTriggerStopPolicy::default(),
        )
        .await;

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());

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

            // Task 1: "foo" start running at 60ms, finish at 70ms
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

            // Manual trigger for "foo" at 40ms, while its next run is planned at 110ms
            let trigger0_driver = harness.manual_flow_trigger_driver(ManualFlowActivationArgs {
                flow_binding: foo_flow_binding,
                run_since_start: Duration::milliseconds(40),
                initiator_id: None,
                maybe_forced_flow_config_rule: None,
            });
            let trigger0_handle = trigger0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(100));
            tokio::join!(task0_handle, task1_handle, trigger0_handle, sim_handle);
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

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    // Flow 0 ran 0..20ms, flow 1 40..70ms. Flow 1 was first planned at 110ms,
    // after it completed: measured from that plan, it would count as 0
    let ingest_flow_type = ingest_dataset_binding(&foo_id).flow_type;
    assert_eq!(harness.completed_flows(&ingest_flow_type, "success"), 2);
    harness.assert_completed_flows_duration_seconds(&ingest_flow_type, "success", 0.05);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_flow_duration_starts_at_activation_before_batching_deadline() {
    let mut seq_dataset_changes = mockall::Sequence::new();
    let mut mock_dataset_changes = MockDatasetIncrementQueryService::new();
    // foo: reading after task 0, then task 1, then bar after task 2
    for num_records in [5, 7, 12] {
        mock_dataset_changes
            .expect_get_increment_between()
            .times(1)
            .in_sequence(&mut seq_dataset_changes)
            .returning(move |_, _, _| {
                Ok(MetadataChainIncrementInterval {
                    num_blocks: 1,
                    num_records,
                    updated_watermark: None,
                })
            });
    }

    let mut mock_transform_flow_evaluator = MockTransformFlowEvaluator::new();
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

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    test_flow_listener.define_dataset_display_name(bar_id.clone(), "bar".to_string());

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

            let sim_handle = harness.advance_time(Duration::milliseconds(120));
            tokio::join!(task0_handle, task1_handle, task2_handle, sim_handle);
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

            "#
        ),
        format!("{}", test_flow_listener.as_ref())
    );

    // "bar" was first planned at its 140ms batching deadline, but ran 90..110ms
    // once enough records arrived: measured from that plan, it would count as 0
    let transform_flow_type = transform_dataset_binding(&bar_id).flow_type;
    assert_eq!(harness.completed_flows(&transform_flow_type, "success"), 1);
    harness.assert_completed_flows_duration_seconds(&transform_flow_type, "success", 0.02);

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
