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

use super::{
    FAILING_PROJECTOR_NAME,
    FLAKY_PROJECTOR_NAME,
    FLOW_SYSTEM_TEST_LISTENER_NAME,
    FlowHarness,
    FlowHarnessOverrides,
    FlowSystemTestListener,
    TaskDriverArgs,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_failing_projector_reported_without_blocking_others() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        with_failing_projector: true,
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let start_time = harness.aligned_now();
    harness
        .schedule_flow_for_activation(
            &ingest_dataset_binding(&foo_id),
            start_time + Duration::milliseconds(10),
        )
        .await;

    harness
        .simulate_flow_scenario(|| async {
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
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
            let foo_task0_handle = foo_task0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(50));
            tokio::join!(foo_task0_handle, sim_handle);
        })
        .await
        .unwrap();

    // The failing projector stays failing, the others keep up
    assert!(harness.is_projector_failing(FAILING_PROJECTOR_NAME));
    assert!(!harness.is_projector_failing(FLOW_SYSTEM_TEST_LISTENER_NAME));

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    assert!(
        format!("{}", test_flow_listener.as_ref()).contains("Finished Success"),
        "{test_flow_listener}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_projector_recovers_after_failed_batch() {
    let harness = FlowHarness::with_overrides(FlowHarnessOverrides {
        with_flaky_projector: true,
        ..Default::default()
    });

    let foo_id = harness
        .create_root_dataset(odf::DatasetAlias {
            dataset_name: odf::DatasetName::new_unchecked("foo"),
            account_name: None,
        })
        .await;

    let start_time = harness.aligned_now();
    harness
        .schedule_flow_for_activation(
            &ingest_dataset_binding(&foo_id),
            start_time + Duration::milliseconds(10),
        )
        .await;

    harness
        .simulate_flow_scenario(|| async {
            let foo_task0_driver = harness.task_driver(TaskDriverArgs {
                task_id: TaskID::new(0),
                task_metadata: TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]),
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
            let foo_task0_handle = foo_task0_driver.run();

            let sim_handle = harness.advance_time(Duration::milliseconds(50));
            tokio::join!(foo_task0_handle, sim_handle);
        })
        .await
        .unwrap();

    // The failed batch is fetched again on a later wakeup, not skipped
    assert!(harness.has_flaky_projector_applied_failed_event());
    assert!(!harness.is_projector_failing(FLAKY_PROJECTOR_NAME));

    let test_flow_listener = harness.catalog.get_one::<FlowSystemTestListener>().unwrap();
    test_flow_listener.define_dataset_display_name(foo_id.clone(), "foo".to_string());
    assert!(
        format!("{}", test_flow_listener.as_ref()).contains("Finished Success"),
        "{test_flow_listener}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
