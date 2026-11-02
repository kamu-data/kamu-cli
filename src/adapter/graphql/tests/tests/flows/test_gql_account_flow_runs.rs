// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use indoc::indoc;
use kamu_accounts::{DEFAULT_ACCOUNT_NAME, DEFAULT_ACCOUNT_NAME_STR};
use pretty_assertions::assert_eq;

use crate::utils::{BaseGQLFlowRunsHarness, GraphQLQueryRequest};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_list_account_flows() {
    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

    let harness = AccountFlowRunsHarness::new().await;

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    harness
        .trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    // Should return list of flows for account
    let response = harness
        .list_flows_query(&DEFAULT_ACCOUNT_NAME)
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
    let harness = AccountFlowRunsHarness::new().await;

    let foo_dataset_name = odf::DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), foo_dataset_name.clone());

    let bar_dataset_name = odf::DatasetName::new_unchecked("bar");
    let bar_dataset_alias =
        odf::DatasetAlias::new(Some(DEFAULT_ACCOUNT_NAME.clone()), bar_dataset_name.clone());

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let _bar_create_result = harness.create_root_dataset(bar_dataset_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    harness
        .trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;
    harness
        .trigger_compaction_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    // Pure datasets listing

    assert_eq!(
        GraphQLQueryRequest::new(
            indoc!(
                r#"
                query($accountName: String!) {
                    accounts {
                        byName (name: $accountName) {
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
            ),
            async_graphql::Variables::from_json(serde_json::json!({
                "accountName": DEFAULT_ACCOUNT_NAME_STR,
            })),
        )
        .execute(&schema, &harness.catalog_authorized)
        .await
        .data,
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
async fn test_list_datasets_with_flow_includes_datasets_with_only_webhook_flows() {
    let harness = AccountFlowRunsHarness::new().await;

    let foo_id = harness.create_owned_root_dataset("foo").await;
    let bar_id = harness.create_owned_root_dataset("bar").await;
    let _baz_id = harness.create_owned_root_dataset("baz").await;

    harness
        .trigger_ingest_flow_mutation(&foo_id)
        .execute(&harness.schema, &harness.catalog_authorized)
        .await;

    let subscription_id = harness
        .create_webhook_for_dataset_updates(&bar_id, "bar-hook")
        .await;
    harness.trigger_webhook_flow(&bar_id, subscription_id).await;

    assert_eq!(
        harness.list_dataset_ids_with_flow().await,
        [bar_id.to_string(), foo_id.to_string()]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowRunsHarness, base_gql_flow_runs_harness)]
struct AccountFlowRunsHarness {
    base_gql_flow_runs_harness: BaseGQLFlowRunsHarness,
    schema: kamu_adapter_graphql::Schema,
}

impl AccountFlowRunsHarness {
    async fn new() -> Self {
        let base_gql_flow_runs_harness =
            BaseGQLFlowRunsHarness::with_overrides(Default::default()).await;
        Self {
            base_gql_flow_runs_harness,
            schema: kamu_adapter_graphql::schema_quiet(),
        }
    }

    async fn create_owned_root_dataset(&self, dataset_name: &str) -> odf::DatasetID {
        self.create_root_dataset(odf::DatasetAlias::new(
            Some(DEFAULT_ACCOUNT_NAME.clone()),
            odf::DatasetName::new_unchecked(dataset_name),
        ))
        .await
        .dataset_handle
        .id
    }

    /// IDs of datasets listed by `listDatasetsWithFlow` of the default account
    async fn list_dataset_ids_with_flow(&self) -> Vec<String> {
        let response = GraphQLQueryRequest::new(
            indoc!(
                r#"
                query($accountName: String!) {
                    accounts {
                        byName (name: $accountName) {
                            flows {
                                runs {
                                    listDatasetsWithFlow {
                                        nodes {
                                            id
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ),
            async_graphql::Variables::from_json(serde_json::json!({
                "accountName": DEFAULT_ACCOUNT_NAME_STR,
            })),
        )
        .execute(&self.schema, &self.catalog_authorized)
        .await;

        let json = response.data.into_json().unwrap();
        json["accounts"]["byName"]["flows"]["runs"]["listDatasetsWithFlow"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|node| node["id"].as_str().unwrap().to_string())
            .collect()
    }

    fn list_flows_query(&self, account_name: &odf::AccountName) -> GraphQLQueryRequest {
        let request_code = indoc!(
            r#"
            query($accountName: AccountName!) {
                accounts {
                    byName (name: $accountName) {
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
        );

        GraphQLQueryRequest::new(
            request_code,
            async_graphql::Variables::from_json(serde_json::json!({
                "accountName": account_name,
            })),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
