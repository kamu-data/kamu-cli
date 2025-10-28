// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use async_graphql::value;
use kamu_adapter_task_dataset::TaskResultDatasetUpdate;
use kamu_core::{PullResult, TenancyConfig};
use kamu_datasets::DatasetIntervalIncrement;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_task_system::*;
use kamu_webhooks::*;
use uuid::Uuid;

use crate::utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_root_dataset() {
    let harness = DatasetFlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Confirm initial primary process state
    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "UNCONFIGURED".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
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
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "ACTIVE".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );

    // Pause trigger and confirm state transitions

    harness
        .pause_flow_trigger(&foo_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "PAUSED_MANUAL".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );

    // Resume trigger and confirm state transitions

    harness
        .resume_flow_trigger(&foo_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "ACTIVE".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_basic_process_state_actions_derived_dataset() {
    let harness = DatasetFlowProcessesHarness::new().await;

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
        .read_flow_process_state(&bar_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "UNCONFIGURED".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );

    // Configure trigger and confirm state transitions

    harness
        .set_reactive_trigger_immediate(&bar_result.dataset_handle.id, "EXECUTE_TRANSFORM", false)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&bar_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "ACTIVE".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );

    // Pause trigger and confirm state transitions

    harness
        .pause_flow_trigger(&bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&bar_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "PAUSED_MANUAL".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );

    // Resume trigger and confirm state transitions

    harness
        .resume_flow_trigger(&bar_result.dataset_handle.id, "EXECUTE_TRANSFORM")
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response = harness
        .read_flow_process_state(&bar_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "EXECUTE_TRANSFORM".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "ACTIVE".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_several_runs() {
    let harness = DatasetFlowProcessesHarness::new().await;

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
                        has_more: false,
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    // Read latest state

    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "ACTIVE".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );

    // Mimic a failed 2nd run
    harness
        .mimic_flow_run_with_outcome("1", TaskOutcome::Failed(TaskError::empty_recoverable()))
        .await;

    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "FAILING".to_string(),
                consecutive_failures: 1,
                maybe_auto_stop_reason: None,
            }
        }
    );

    // Mimic a failed 3rd run
    harness
        .mimic_flow_run_with_outcome("2", TaskOutcome::Failed(TaskError::empty_recoverable()))
        .await;

    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "FAILING".to_string(),
                consecutive_failures: 2,
                maybe_auto_stop_reason: None,
            }
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
                        has_more: false,
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "ACTIVE".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_reach_auto_stop_via_failures_count() {
    let harness = DatasetFlowProcessesHarness::new().await;

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
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "STOPPED_AUTO".to_string(),
                consecutive_failures: 3,
                maybe_auto_stop_reason: Some("STOP_POLICY".to_string()),
            }
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_reach_auto_stop_via_unrecoverable_failure() {
    let harness = DatasetFlowProcessesHarness::new().await;

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
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    let process_summary = harness.extract_primary_flow_process(&response);
    assert_eq!(
        process_summary,
        FlowPrimaryProcessSummary {
            flow_type: "INGEST".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "STOPPED_AUTO".to_string(),
                consecutive_failures: 1,
                maybe_auto_stop_reason: Some("UNRECOVERABLE_FAILURE".to_string()),
            }
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ingest_process_with_multiple_webhooks() {
    let harness = DatasetFlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Create multiple webhook subscriptions
    let webhook_names = ["alpha", "beta", "gamma", "delta"];
    let webhook_subscription_ids: HashMap<_, _> =
        futures::future::join_all(webhook_names.iter().map(|name| async {
            let id = harness
                .create_webhook_for_dataset_updates(&foo_result.dataset_handle.id, name)
                .await;
            ((*name).to_string(), id)
        }))
        .await
        .into_iter()
        .collect();

    // Read webhooks state
    let webhooks_state = harness
        .read_webhooks_processes(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    // Rollup checks
    let rollup = harness.extract_webhooks_rollup(&webhooks_state);
    pretty_assertions::assert_eq!(
        rollup,
        value!({
            "total": 4,
            "active": 4,
            "failing": 0,
            "paused": 0,
            "unconfigured": 0,
            "stopped": 0,
            "worstConsecutiveFailures": 0,
        })
    );

    // Subprocesses checks
    let subprocesses_by_id: HashMap<_, _> = harness
        .extract_webhooks_subprocesses(&webhooks_state)
        .into_iter()
        .map(|sp| (sp.id, sp))
        .collect();
    assert_eq!(subprocesses_by_id.len(), 4);

    for (webhook_name, webhook_subscription_id) in &webhook_subscription_ids {
        pretty_assertions::assert_eq!(
            subprocesses_by_id
                .get(webhook_subscription_id)
                .unwrap_or_else(|| panic!("Missing webhook {webhook_name}")),
            &FlowWebhookSubprocessSummary {
                id: *webhook_subscription_id,
                name: webhook_name.clone(),
                summary: FlowProcessSummaryBasic {
                    effective_state: "ACTIVE".to_string(),
                    consecutive_failures: 0,
                    maybe_auto_stop_reason: None,
                }
            }
        );
    }

    // Make the following changes:
    // - webhook "alpha" fails once
    // - webhook "beta" is paused
    // - webhook "gamma" fails with critical error and is marked unreachable
    // - webhook "delta" succeeds once

    let alpha_id = webhook_subscription_ids.get("alpha").unwrap();
    let beta_id = webhook_subscription_ids.get("beta").unwrap();
    let gamma_id = webhook_subscription_ids.get("gamma").unwrap();
    let delta_id = webhook_subscription_ids.get("delta").unwrap();

    harness
        .mimic_webhook_flow_failure(&foo_result.dataset_handle.id, *alpha_id, false)
        .await;
    harness.pause_webhook_subscription(*beta_id).await;
    harness
        .mimic_webhook_flow_failure(&foo_result.dataset_handle.id, *gamma_id, true)
        .await;
    harness
        .mimic_webhook_flow_success(&foo_result.dataset_handle.id, *delta_id)
        .await;

    // Read webhooks state after changes
    let webhooks_state = harness
        .read_webhooks_processes(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data;

    // Rollup checks
    let rollup = harness.extract_webhooks_rollup(&webhooks_state);
    pretty_assertions::assert_eq!(
        rollup,
        value!({
            "total": 4,
            "active": 1,
            "failing": 1,
            "paused": 1,
            "unconfigured": 0,
            "stopped": 1,
            "worstConsecutiveFailures": 1,
        })
    );

    // Subprocesses checks
    let subprocesses_by_id: HashMap<_, _> = harness
        .extract_webhooks_subprocesses(&webhooks_state)
        .into_iter()
        .map(|sp| (sp.id, sp))
        .collect();
    assert_eq!(subprocesses_by_id.len(), 4);

    pretty_assertions::assert_eq!(
        subprocesses_by_id.get(alpha_id).unwrap(),
        &FlowWebhookSubprocessSummary {
            id: *alpha_id,
            name: "alpha".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "FAILING".to_string(),
                consecutive_failures: 1,
                maybe_auto_stop_reason: None,
            }
        }
    );

    pretty_assertions::assert_eq!(
        subprocesses_by_id.get(beta_id).unwrap(),
        &FlowWebhookSubprocessSummary {
            id: *beta_id,
            name: "beta".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "PAUSED_MANUAL".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );

    pretty_assertions::assert_eq!(
        subprocesses_by_id.get(gamma_id).unwrap(),
        &FlowWebhookSubprocessSummary {
            id: *gamma_id,
            name: "gamma".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "STOPPED_AUTO".to_string(),
                consecutive_failures: 1,
                maybe_auto_stop_reason: Some("UNRECOVERABLE_FAILURE".to_string()),
            }
        }
    );

    pretty_assertions::assert_eq!(
        subprocesses_by_id.get(delta_id).unwrap(),
        &FlowWebhookSubprocessSummary {
            id: *delta_id,
            name: "delta".to_string(),
            summary: FlowProcessSummaryBasic {
                effective_state: "ACTIVE".to_string(),
                consecutive_failures: 0,
                maybe_auto_stop_reason: None,
            }
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_access_flow_process_state_anonymous() {
    let harness = DatasetFlowProcessesHarness::new().await;

    // Create root dataset
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // Anonymous access can't find a dataset
    let response = harness
        .read_flow_process_state(&foo_result.dataset_handle.id)
        .await
        .execute(&schema, &harness.catalog_anonymous)
        .await;
    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": null
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowRunsHarness, base_gql_flow_runs_harness)]
struct DatasetFlowProcessesHarness {
    base_gql_flow_runs_harness: BaseGQLFlowRunsHarness,
    flow_system_event_agent: Arc<dyn FlowSystemEventAgent>,
}

impl DatasetFlowProcessesHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(base_gql_harness.catalog());

        let base_gql_flow_runs_catalog = BaseGQLFlowRunsHarness::make_base_gql_flow_runs_catalog(
            &base_gql_flow_catalog,
            FlowRunsHarnessOverrides {
                dataset_changes_mock: Some(
                    MockDatasetIncrementQueryService::with_increment_between(
                        DatasetIntervalIncrement {
                            num_blocks: 1,
                            num_records: 10,
                            updated_watermark: None,
                        },
                    ),
                ),
            },
        );

        let base_gql_flow_runs_harness =
            BaseGQLFlowRunsHarness::new(base_gql_harness, base_gql_flow_runs_catalog.clone()).await;

        Self {
            base_gql_flow_runs_harness,
            flow_system_event_agent: base_gql_flow_runs_catalog
                .get_one::<dyn FlowSystemEventAgent>()
                .unwrap(),
        }
    }

    async fn read_flow_process_state(&self, dataset_id: &odf::DatasetID) -> GraphQLQueryRequest {
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

        let variables = async_graphql::Variables::from_value(value!({
            "id": dataset_id.to_string(),
        }));

        GraphQLQueryRequest::new(request_code, variables)
    }

    async fn read_webhooks_processes(&self, dataset_id: &odf::DatasetID) -> GraphQLQueryRequest {
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
                                webhooks {
                                    rollup {
                                        total
                                        active
                                        failing
                                        paused
                                        unconfigured
                                        stopped
                                        worstConsecutiveFailures
                                    }
                                    subprocesses {
                                        id
                                        name
                                        summary {
                                            effectiveState
                                            consecutiveFailures
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
            }
            "#;

        let variables = async_graphql::Variables::from_value(value!({
            "id": dataset_id.to_string(),
        }));

        GraphQLQueryRequest::new(request_code, variables)
    }

    fn extract_primary_flow_process(
        &self,
        response: &async_graphql::Value,
    ) -> FlowPrimaryProcessSummary {
        // Navigate through the nested structure more compactly
        get_gql_value_property(response, "datasets")
            .and_then(|v| get_gql_value_property(v, "byId"))
            .and_then(|v| get_gql_value_property(v, "flows"))
            .and_then(|v| get_gql_value_property(v, "processes"))
            .and_then(|v| get_gql_value_property(v, "primary"))
            .map(FlowPrimaryProcessSummary::from)
            .unwrap_or_else(|| panic!("Invalid GraphQL response structure"))
    }

    fn extract_webhooks_rollup(&self, response: &async_graphql::Value) -> async_graphql::Value {
        // Navigate through the nested structure more compactly
        get_gql_value_property(response, "datasets")
            .and_then(|v| get_gql_value_property(v, "byId"))
            .and_then(|v| get_gql_value_property(v, "flows"))
            .and_then(|v| get_gql_value_property(v, "processes"))
            .and_then(|v| get_gql_value_property(v, "webhooks"))
            .and_then(|v| get_gql_value_property(v, "rollup"))
            .cloned()
            .unwrap_or_else(|| panic!("Invalid GraphQL response structure"))
    }

    fn extract_webhooks_subprocesses(
        &self,
        response: &async_graphql::Value,
    ) -> Vec<FlowWebhookSubprocessSummary> {
        // Navigate through the nested structure more compactly
        let subprocesses_value = get_gql_value_property(response, "datasets")
            .and_then(|v| get_gql_value_property(v, "byId"))
            .and_then(|v| get_gql_value_property(v, "flows"))
            .and_then(|v| get_gql_value_property(v, "processes"))
            .and_then(|v| get_gql_value_property(v, "webhooks"))
            .and_then(|v| get_gql_value_property(v, "subprocesses"))
            .and_then(|v| {
                if let async_graphql::Value::List(list) = v {
                    Some(list.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| panic!("Invalid GraphQL response structure"));

        subprocesses_value
            .into_iter()
            .map(|subprocess_value| FlowWebhookSubprocessSummary::from(&subprocess_value))
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
struct FlowProcessSummaryBasic {
    effective_state: String,
    consecutive_failures: i32,
    maybe_auto_stop_reason: Option<String>,
}

impl From<&async_graphql::Value> for FlowProcessSummaryBasic {
    fn from(value: &async_graphql::Value) -> Self {
        let effective_state = get_gql_value_string_property(value, "effectiveState").unwrap();

        let consecutive_failures =
            i32::try_from(get_gql_value_i64_property(value, "consecutiveFailures").unwrap())
                .unwrap();

        let maybe_auto_stop_reason = get_gql_value_string_property(value, "autoStoppedReason");

        Self {
            effective_state,
            consecutive_failures,
            maybe_auto_stop_reason,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
struct FlowPrimaryProcessSummary {
    flow_type: String,
    summary: FlowProcessSummaryBasic,
}

impl From<&async_graphql::Value> for FlowPrimaryProcessSummary {
    fn from(value: &async_graphql::Value) -> Self {
        let flow_type = get_gql_value_string_property(value, "flowType").unwrap();

        let summary =
            FlowProcessSummaryBasic::from(get_gql_value_property(value, "summary").unwrap());

        Self { flow_type, summary }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
struct FlowWebhookSubprocessSummary {
    id: WebhookSubscriptionID,
    name: String,
    summary: FlowProcessSummaryBasic,
}

impl From<&async_graphql::Value> for FlowWebhookSubprocessSummary {
    fn from(value: &async_graphql::Value) -> Self {
        let id = WebhookSubscriptionID::new(
            Uuid::parse_str(&get_gql_value_string_property(value, "id").unwrap())
                .expect("Invalid UUID in webhook subprocess id"),
        );

        let name = get_gql_value_string_property(value, "name").unwrap();

        let summary =
            FlowProcessSummaryBasic::from(get_gql_value_property(value, "summary").unwrap());

        Self { id, name, summary }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
