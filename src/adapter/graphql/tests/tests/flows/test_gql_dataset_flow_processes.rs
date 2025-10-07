// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::value;
use kamu_adapter_task_dataset::TaskResultDatasetUpdate;
use kamu_core::PullResult;
use kamu_datasets::DatasetIntervalIncrement;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::*;

use crate::utils::{BaseGQLFlowRunsHarness, FlowRunsHarnessOverrides};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_root_dataset() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Confirm initial primary process state
    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "UNCONFIGURED".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Configure trigger and confirm state transitions
    harness
        .set_time_delta_trigger(
            &foo_result.dataset_handle.id,
            "INGEST",
            (10, "MINUTES"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Pause trigger and confirm state transitions

    harness
        .pause_flow_trigger(&foo_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "PAUSED_MANUAL".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Resume trigger and confirm state transitions

    harness
        .resume_flow_trigger(&foo_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_derived_dataset() {
    let harness = FlowProcessesHarness::new().await;

    // Create datasets
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let bar_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Confirm initial primary process state

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "UNCONFIGURED".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Configure trigger and confirm state transitions

    harness
        .set_reactive_trigger_immediate(&bar_result.dataset_handle.id, "EXECUTE_TRANSFORM", false)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Pause trigger and confirm state transitions

    harness
        .pause_flow_trigger(&bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "PAUSED_MANUAL".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Resume trigger and confirm state transitions

    harness
        .resume_flow_trigger(&bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&schema, &bar_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_several_runs() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Configure trigger
    harness
        .set_time_delta_trigger(
            &foo_result.dataset_handle.id,
            "INGEST",
            (10, "MINUTES"),
            Some(value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 3
                    }
                }
            )),
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    // Mimic a successful 1st run
    harness
        .mimic_flow_run_with_outcome(
            "0",
            TaskOutcome::Success(
                TaskResultDatasetUpdate {
                    pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    // Read latest state

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );

    // Mimic a failed 2nd run
    harness
        .mimic_flow_run_with_outcome("1", TaskOutcome::Failed(TaskError::empty_recoverable()))
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "FAILING".to_string(),
            consecutive_failures: 1,
            maybe_auto_stop_reason: None,
        }
    );

    // Mimic a failed 3rd run
    harness
        .mimic_flow_run_with_outcome("2", TaskOutcome::Failed(TaskError::empty_recoverable()))
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "FAILING".to_string(),
            consecutive_failures: 2,
            maybe_auto_stop_reason: None,
        }
    );

    // 4th run succeeds
    harness
        .mimic_flow_run_with_outcome(
            "3",
            TaskOutcome::Success(
                TaskResultDatasetUpdate {
                    pull_result: PullResult::Updated {
                        old_head: Some(odf::Multihash::from_digest_sha3_256(b"new-slice")),
                        new_head: odf::Multihash::from_digest_sha3_256(b"new-slice-2"),
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "ACTIVE".to_string(),
            consecutive_failures: 0,
            maybe_auto_stop_reason: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_reach_auto_stop_via_failures_count() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Configure trigger
    harness
        .set_time_delta_trigger(
            &foo_result.dataset_handle.id,
            "INGEST",
            (10, "MINUTES"),
            Some(value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 3
                    }
                }
            )),
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    // Mimic 3 failures
    for i in 0..3 {
        harness
            .mimic_flow_run_with_outcome(
                &i.to_string(),
                TaskOutcome::Failed(TaskError::empty_recoverable()),
            )
            .await;

        // Sync events explicitly to active re-scheduling
        harness
            .flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();
    }

    // Must see auto-stopped state with 3 consecutive failures
    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "STOPPED_AUTO".to_string(),
            consecutive_failures: 3,
            maybe_auto_stop_reason: Some("STOP_POLICY".to_string()),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_reach_auto_stop_via_unrecoverable_failure() {
    let harness = FlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Configure trigger
    harness
        .set_time_delta_trigger(
            &foo_result.dataset_handle.id,
            "INGEST",
            (10, "MINUTES"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    // Mimic an unrecoverable failure
    harness
        .mimic_flow_run_with_outcome("0", TaskOutcome::Failed(TaskError::empty_unrecoverable()))
        .await;

    // Must see auto-stopped state with unrecoverable failure
    let response = harness
        .read_flow_process_state(&schema, &foo_result.dataset_handle.id)
        .await;
    let process_summary = FlowProcessesHarness::extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowProcessSummaryBasic {
            flow_type: "INGEST".to_string(),
            effective_state: "STOPPED_AUTO".to_string(),
            consecutive_failures: 1,
            maybe_auto_stop_reason: Some("UNRECOVERABLE_FAILURE".to_string()),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowRunsHarness, base_gql_flow_runs_harness)]
struct FlowProcessesHarness {
    base_gql_flow_runs_harness: BaseGQLFlowRunsHarness,
    flow_system_event_agent: Arc<dyn FlowSystemEventAgent>,
}

impl FlowProcessesHarness {
    async fn new() -> Self {
        let dataset_changes_mock =
            MockDatasetIncrementQueryService::with_increment_between(DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 10,
                updated_watermark: None,
            });

        let base_gql_flow_runs_harness =
            BaseGQLFlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
                dataset_changes_mock: Some(dataset_changes_mock),
            })
            .await;
        Self {
            flow_system_event_agent: base_gql_flow_runs_harness
                .catalog_anonymous
                .get_one::<dyn FlowSystemEventAgent>()
                .unwrap(),
            base_gql_flow_runs_harness,
        }
    }

    async fn read_flow_process_state(
        &self,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
    ) -> async_graphql::Value {
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await
            .unwrap();

        let request_code = r#"
            query($id: DatasetID!) {
                datasets {
                    byId (datasetId: $id) {
                        flows {
                            processes {
                                primary {
                                    flowType
                                    summary {
                                        effectiveState
                                        consecutiveFailures
                                        stopPolicy {
                                            __typename
                                        }
                                        lastSuccessAt
                                        lastAttemptAt
                                        lastFailureAt
                                        nextPlannedAt
                                        autoStoppedReason
                                        autoStoppedAt
                                    }
                                }
                            }
                        }
                    }
                }
            }
            "#;

        let response = schema
            .execute(
                async_graphql::Request::new(request_code)
                    .variables(async_graphql::Variables::from_value(value!({
                        "id": dataset_id.to_string(),
                    })))
                    .data(self.catalog_authorized.clone()),
            )
            .await;

        assert!(response.is_ok(), "{:?}", response.errors);

        response.data
    }

    fn extract_primary_flow_process(response: &async_graphql::Value) -> FlowProcessSummaryBasic {
        fn get_obj<'a>(
            value: &'a async_graphql::Value,
            key: &str,
        ) -> Option<&'a async_graphql::Value> {
            if let async_graphql::Value::Object(obj) = value {
                obj.get(key)
            } else {
                None
            }
        }

        // Navigate through the nested structure more compactly
        get_obj(response, "datasets")
            .and_then(|v| get_obj(v, "byId"))
            .and_then(|v| get_obj(v, "flows"))
            .and_then(|v| get_obj(v, "processes"))
            .and_then(|v| get_obj(v, "primary"))
            .map(|primary| FlowProcessSummaryBasic::from(primary.clone()))
            .unwrap_or_else(|| panic!("Invalid GraphQL response structure"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
struct FlowProcessSummaryBasic {
    flow_type: String,
    effective_state: String,
    consecutive_failures: i32,
    maybe_auto_stop_reason: Option<String>,
}

impl From<async_graphql::Value> for FlowProcessSummaryBasic {
    fn from(value: async_graphql::Value) -> Self {
        // Convert the GraphQL Value to a JSON Value for easier access
        let json_value = value.into_json().unwrap();

        let flow_type = json_value["flowType"].as_str().unwrap().to_string();

        let summary = &json_value["summary"];
        let effective_state = summary["effectiveState"].as_str().unwrap().to_string();

        let consecutive_failures =
            i32::try_from(summary["consecutiveFailures"].as_i64().unwrap()).unwrap();

        let maybe_auto_stop_reason = summary["autoStoppedReason"].as_str();

        Self {
            flow_type,
            effective_state,
            consecutive_failures,
            maybe_auto_stop_reason: maybe_auto_stop_reason.map(ToString::to_string),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
