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
use chrono::Duration;
use indoc::indoc;
use kamu::MetadataQueryServiceImpl;
use kamu_accounts::{DEFAULT_ACCOUNT_NAME, DEFAULT_ACCOUNT_NAME_STR};
use kamu_adapter_flow_dataset::*;
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::FlowAgentConfig;
use kamu_flow_system_inmem::{
    InMemoryFlowConfigurationEventStore,
    InMemoryFlowEventStore,
    InMemoryFlowTriggerEventStore,
};
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

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
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
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
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",

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

    let mutation_code = FlowTriggerHarness::set_ingest_trigger_time_delta_mutation(
        &foo_create_result.dataset_handle.id,
        "INGEST",
        false,
        1,
        "DAYS",
    );
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

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
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",

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

    let mutation_code = FlowTriggerHarness::set_ingest_trigger_time_delta_mutation(
        &bar_create_result.dataset_handle.id,
        "INGEST",
        false,
        1,
        "DAYS",
    );
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

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

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct FlowTriggerHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
}

impl FlowTriggerHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<MetadataQueryServiceImpl>()
                .add_value(MockDatasetIncrementQueryService::default())
                .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
                .add::<InMemoryFlowConfigurationEventStore>()
                .add::<InMemoryFlowTriggerEventStore>()
                .add::<InMemoryFlowEventStore>()
                .add_value(FlowAgentConfig::new(
                    Duration::seconds(1),
                    Duration::minutes(1),
                ))
                .add::<TaskSchedulerImpl>()
                .add::<InMemoryTaskEventStore>()
                .add::<FlowSupportServiceImpl>();

            kamu_flow_system_services::register_dependencies(&mut b);

            b.build()
        };

        let (_, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            catalog_authorized,
        }
    }

    async fn create_root_dataset(&self, dataset_alias: odf::DatasetAlias) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(dataset_alias)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
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
                                                  ... on FlowDescriptionHardCompactionSuccess {
                                                      originalBlocksCount
                                                      resultingBlocksCount
                                                      newHead
                                                  }
                                                  ... on FlowDescriptionHardCompactionNothingToDo {
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
                                              inputRecordsCount
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
                                      primaryTrigger {
                                          __typename
                                          ... on FlowTriggerInputDatasetFlow {
                                              dataset {
                                                  id
                                                  name
                                              }
                                              flowType
                                              flowId
                                          }
                                          ... on FlowTriggerManual {
                                            __typename
                                          }
                                      }
                                      startCondition {
                                          __typename
                                          ... on FlowStartConditionBatching {
                                              accumulatedRecordsCount
                                              activeBatchingRule {
                                                  __typename
                                                  minRecordsToAwait
                                                  maxBatchingInterval {
                                                      every
                                                      unit
                                                  }
                                              }
                                              watermarkModified
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

    fn set_ingest_trigger_time_delta_mutation(
        id: &odf::DatasetID,
        dataset_flow_type: &str,
        paused: bool,
        every: u64,
        unit: &str,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: "<dataset_flow_type>",
                                    paused: <paused>,
                                    triggerInput: {
                                        schedule: {
                                            timeDelta: { every: <every>, unit: "<unit>" }
                                        }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        trigger {
                                            __typename
                                            paused
                                            schedule {
                                                __typename
                                                ... on TimeDelta {
                                                    every
                                                    unit
                                                }
                                            }
                                            batching {
                                                __typename
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
        .replace("<dataset_flow_type>", dataset_flow_type)
        .replace("<paused>", if paused { "true" } else { "false" })
        .replace("<every>", every.to_string().as_str())
        .replace("<unit>", unit)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
