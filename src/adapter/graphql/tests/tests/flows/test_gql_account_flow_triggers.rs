// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use async_graphql::value;
use indoc::indoc;
use kamu_accounts::{DEFAULT_ACCOUNT_NAME, DEFAULT_ACCOUNT_NAME_STR};
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_services::testing::{
    FakeDependencyGraphIndexer,
    MockDatasetIncrementQueryService,
};
use kamu_flow_system::FlowAgentConfig;
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;

use crate::utils::{BaseGQLDatasetHarness, BaseGQLFlowHarness};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_account_flows() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

    let harness = FlowTriggerHarness::new().await;

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let request_code =
        FlowTriggerHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

    // Should return list of flows for account
    let request_code = FlowTriggerHarness::list_flows_query(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                        },
                                        "startCondition": null,
                                    }
                                ],
                                "pageInfo": {
                                    "hasPreviousPage": false,
                                    "hasNextPage": false,
                                    "currentPage": 0,
                                    "totalPages": 1,
                                }
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_datasets_with_flow() {
    let harness = FlowTriggerHarness::new().await;

    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

    let bar_dataset_name = odf::DatasetName::new_unchecked("bar");
    let bar_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), bar_dataset_name.clone());

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let _bar_create_result = harness.create_root_dataset(bar_dataset_alias).await;

    let ingest_mutation_code =
        FlowTriggerHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id);
    let compaction_mutation_code =
        FlowTriggerHarness::trigger_compaction_flow_mutation(&create_result.dataset_handle.id);

    let schema = kamu_adapter_graphql::schema_quiet();

    let response = schema
        .execute(
            async_graphql::Request::new(ingest_mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(response.is_ok(), "{response:?}");

    let response = schema
        .execute(
            async_graphql::Request::new(compaction_mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(response.is_ok(), "{response:?}");

    // Pure datasets listing
    let request_code = indoc!(
        r#"
        {
            accounts {
                byName (name: "<account_name>") {
                    flows {
                        runs {
                            listDatasetsWithFlow {
                                nodes {
                                    id
                                }
                                pageInfo {
                                    currentPage
                                    totalPages
                                }
                            }
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<account_name>", DEFAULT_ACCOUNT_NAME_STR);

    let response = schema
        .execute(async_graphql::Request::new(request_code).data(harness.catalog_authorized.clone()))
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "runs": {
                            "listDatasetsWithFlow": {
                                "nodes": [
                                    {
                                        "id": create_result.dataset_handle.id,
                                    },
                                ],
                                "pageInfo": {
                                    "currentPage": 0,
                                    "totalPages": 1,
                                }
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_account_flows() {
    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_dataset_alias = odf::DatasetAlias::new(
        Some(DEFAULT_ACCOUNT_NAME.clone()),
        odf::DatasetName::new_unchecked("foo"),
    );

    let harness = FlowTriggerHarness::new().await;

    let foo_create_result = harness.create_root_dataset(foo_dataset_alias).await;

    let request_code =
        FlowTriggerHarness::trigger_ingest_flow_mutation(&foo_create_result.dataset_handle.id);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

    let request_code = FlowTriggerHarness::list_flows_query(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "datasetId": foo_create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                        },
                                        "startCondition": null,
                                    }
                                ],
                                "pageInfo": {
                                    "hasPreviousPage": false,
                                    "hasNextPage": false,
                                    "currentPage": 0,
                                    "totalPages": 1,
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    harness
        .set_time_delta_trigger(
            &foo_create_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let mutation_code = FlowTriggerHarness::pause_account_flows(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "triggers": {
                            "pauseAccountDatasetFlows": true
                        }
                    }
                }
            }
        })
    );

    let request_code =
        FlowTriggerHarness::all_paused_trigger_query(&foo_create_result.dataset_handle.id);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "triggers": {
                            "allPaused": true
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::resume_account_flows(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "triggers": {
                            "resumeAccountDatasetFlows": true
                        }
                    }
                }
            }
        })
    );

    let request_code =
        FlowTriggerHarness::all_paused_trigger_query(&foo_create_result.dataset_handle.id);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "triggers": {
                            "allPaused": false
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_triggers_all_paused() {
    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_dataset_alias = odf::DatasetAlias::new(
        Some(DEFAULT_ACCOUNT_NAME.clone()),
        odf::DatasetName::new_unchecked("foo"),
    );
    let bar_dataset_alias = odf::DatasetAlias::new(
        Some(DEFAULT_ACCOUNT_NAME.clone()),
        odf::DatasetName::new_unchecked("bar"),
    );

    let harness = FlowTriggerHarness::new().await;

    let foo_create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let bar_create_result = harness.create_root_dataset(bar_dataset_alias).await;

    let request_code =
        FlowTriggerHarness::trigger_ingest_flow_mutation(&foo_create_result.dataset_handle.id);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

    let request_code = FlowTriggerHarness::list_flows_query(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "datasetId": foo_create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",

                                        },
                                        "startCondition": null,
                                    }
                                ],
                                "pageInfo": {
                                    "hasPreviousPage": false,
                                    "hasNextPage": false,
                                    "currentPage": 0,
                                    "totalPages": 1,
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    harness
        .set_time_delta_trigger(
            &bar_create_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let request_code = FlowTriggerHarness::all_paused_account_triggers_query(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "triggers": {
                            "allPaused": false
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::pause_account_flows(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "triggers": {
                            "pauseAccountDatasetFlows": true
                        }
                    }
                }
            }
        })
    );

    let request_code = FlowTriggerHarness::all_paused_account_triggers_query(&DEFAULT_ACCOUNT_NAME);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "accounts": {
                "byName": {
                    "flows": {
                        "triggers": {
                            "allPaused": true
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowHarness, base_gql_flow_harness)]
struct FlowTriggerHarness {
    base_gql_flow_harness: BaseGQLFlowHarness,
}

impl FlowTriggerHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(&base_gql_harness);

        let account_triggers_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(&base_gql_flow_catalog);

            b.add_value(MockDatasetIncrementQueryService::default())
                .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
                .add_value(FlowAgentConfig::test_default())
                .add::<TaskSchedulerImpl>()
                .add::<InMemoryTaskEventStore>()
                .add::<FakeDependencyGraphIndexer>();

            kamu_flow_system_services::register_dependencies(&mut b);

            b.build()
        };

        let base_gql_flow_harness =
            BaseGQLFlowHarness::new(base_gql_harness, account_triggers_catalog).await;

        Self {
            base_gql_flow_harness,
        }
    }

    fn list_flows_query(account_name: &odf::AccountName) -> String {
        indoc!(
            r#"
        {
            accounts {
                byName (name: "<accountName>") {
                    flows {
                        runs {
                            listFlows {
                                nodes {
                                    flowId
                                    datasetId
                                    description {
                                        __typename
                                        ... on FlowDescriptionDatasetHardCompaction {
                                            compactionResult {
                                                ... on FlowDescriptionReorganizationSuccess {
                                                    originalBlocksCount
                                                    resultingBlocksCount
                                                    newHead
                                                }
                                                ... on FlowDescriptionReorganizationNothingToDo {
                                                    message
                                                }
                                            }
                                        }
                                        ... on FlowDescriptionDatasetExecuteTransform {
                                            transformResult {
                                                __typename
                                                ... on FlowDescriptionUpdateResultUpToDate {
                                                    uncacheable
                                                }
                                                ... on FlowDescriptionUpdateResultSuccess {
                                                    numBlocks
                                                    numRecords
                                                }
                                            }
                                        }
                                        ... on FlowDescriptionDatasetPollingIngest {
                                            ingestResult {
                                                __typename
                                                ... on FlowDescriptionUpdateResultUpToDate {
                                                    uncacheable
                                                }
                                                ... on FlowDescriptionUpdateResultSuccess {
                                                    numBlocks
                                                    numRecords
                                                }
                                            }
                                        }
                                        ... on FlowDescriptionDatasetPushIngest {
                                            sourceName
                                            ingestResult {
                                                __typename
                                                ... on FlowDescriptionUpdateResultUpToDate {
                                                    uncacheable
                                                }
                                                ... on FlowDescriptionUpdateResultSuccess {
                                                    numBlocks
                                                    numRecords
                                                }
                                            }
                                        }
                                    }
                                    status
                                    outcome {
                                        ...on FlowSuccessResult {
                                            message
                                        }
                                        ...on FlowAbortedResult {
                                            message
                                        }
                                        ...on FlowFailedError {
                                            reason {
                                                ...on TaskFailureReasonGeneral {
                                                    message
                                                    recoverable
                                                }
                                                ...on TaskFailureReasonInputDatasetCompacted {
                                                    message
                                                    inputDataset {
                                                        id
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    timing {
                                        awaitingExecutorSince
                                        runningSince
                                        lastAttemptFinishedAt
                                    }
                                    taskIds
                                    primaryActivationCause {
                                        __typename
                                        ... on FlowActivationCauseDatasetUpdate {
                                            dataset {
                                                id
                                                name
                                            }
                                        }
                                    }
                                    startCondition {
                                        __typename
                                        ... on FlowStartConditionReactive {
                                            accumulatedRecordsCount
                                            activeBatchingRule {
                                                __typename
                                                ... on FlowTriggerBatchingRuleBuffering {
                                                    minRecordsToAwait
                                                    maxBatchingInterval {
                                                        every
                                                        unit
                                                    }
                                                }
                                            }
                                            watermarkModified
                                            forBreakingChange
                                        }
                                        ... on FlowStartConditionThrottling {
                                            intervalSec
                                            wakeUpAt
                                            shiftedFrom
                                        }
                                        ... on FlowStartConditionExecutor {
                                            taskId
                                        }
                                    }
                                }
                                pageInfo {
                                    hasPreviousPage
                                    hasNextPage
                                    currentPage
                                    totalPages
                                }
                            }
                        }
                    }
                }
            }
        }
          "#
        )
        .replace("<accountName>", account_name.as_ref())
    }

    fn trigger_ingest_flow_mutation(id: &odf::DatasetID) -> String {
        indoc!(
            r#"
          mutation {
              datasets {
                  byId (datasetId: "<id>") {
                      flows {
                          runs {
                              triggerIngestFlow {
                                  __typename,
                                  message
                                  ... on TriggerFlowSuccess {
                                      flow {
                                          __typename
                                          flowId
                                          status
                                          outcome {
                                              ...on FlowSuccessResult {
                                                  message
                                              }
                                              ...on FlowAbortedResult {
                                                  message
                                              }
                                              ...on FlowFailedError {
                                                  reason {
                                                    ...on TaskFailureReasonGeneral {
                                                        message
                                                        recoverable
                                                    }
                                                    ...on TaskFailureReasonInputDatasetCompacted {
                                                          message
                                                          inputDataset {
                                                              id
                                                          }
                                                      }
                                                  }
                                              }
                                          }
                                      }
                                  }
                              }
                          }
                      }
                  }
              }
          }
          "#
        )
        .replace("<id>", &id.to_string())
    }

    fn trigger_compaction_flow_mutation(id: &odf::DatasetID) -> String {
        indoc!(
            r#"
          mutation {
              datasets {
                  byId (datasetId: "<id>") {
                      flows {
                          runs {
                              triggerCompactionFlow {
                                  __typename,
                                  message
                                  ... on TriggerFlowSuccess {
                                      flow {
                                          __typename
                                          flowId
                                          status
                                          outcome {
                                              ...on FlowSuccessResult {
                                                  message
                                              }
                                              ...on FlowAbortedResult {
                                                  message
                                              }
                                              ...on FlowFailedError {
                                                  reason {
                                                    ...on TaskFailureReasonGeneral {
                                                        message
                                                        recoverable
                                                    }
                                                  }
                                              }
                                          }
                                      }
                                  }
                              }
                          }
                      }
                  }
              }
          }
          "#
        )
        .replace("<id>", &id.to_string())
    }

    fn pause_account_flows(account_name: &odf::AccountName) -> String {
        indoc!(
            r#"
            mutation {
                accounts {
                    byName (accountName: "<name>") {
                        flows {
                            triggers {
                                pauseAccountDatasetFlows
                            }
                        }
                    }
                }
            }
        "#
        )
        .replace("<name>", account_name.as_ref())
    }

    fn resume_account_flows(account_name: &odf::AccountName) -> String {
        indoc!(
            r#"
            mutation {
                accounts {
                    byName (accountName: "<name>") {
                        flows {
                            triggers {
                                resumeAccountDatasetFlows
                            }
                        }
                    }
                }
            }
        "#
        )
        .replace("<name>", account_name.as_ref())
    }

    fn all_paused_trigger_query(id: &odf::DatasetID) -> String {
        indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
                                allPaused
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<id>", &id.to_string())
    }

    fn all_paused_account_triggers_query(account_name: &odf::AccountName) -> String {
        indoc!(
            r#"
            {
                accounts {
                    byName (name: "<account_name>") {
                        flows {
                            triggers {
                                allPaused
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<account_name>", account_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
