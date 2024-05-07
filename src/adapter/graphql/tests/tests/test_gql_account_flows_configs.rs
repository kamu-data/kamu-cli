// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////

use std::sync::Arc;

use async_graphql::value;
use chrono::Duration;
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::testing::{
    MetadataFactory,
    MockDatasetActionAuthorizer,
    MockDatasetChangesService,
    MockDependencyGraphRepository,
    MockPollingIngestService,
    MockTransformService,
};
use kamu::{DatasetRepositoryLocalFs, DependencyGraphServiceInMemory};
use kamu_accounts::{
    set_random_jwt_secret,
    AuthenticationService,
    MockAuthenticationService,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME,
};
use kamu_core::*;
use kamu_flow_system::FlowServiceRunConfig;
use kamu_flow_system_inmem::{FlowConfigurationEventStoreInMem, FlowEventStoreInMem};
use kamu_flow_system_services::{FlowConfigurationServiceImpl, FlowServiceImpl};
use kamu_task_system_inmem::TaskSystemEventStoreInMemory;
use kamu_task_system_services::TaskSchedulerImpl;
use opendatafabric::{AccountName, DatasetAlias, DatasetID, DatasetKind, DatasetName};

use crate::utils::authentication_catalogs;

#[test_log::test(tokio::test)]
async fn test_list_account_flows() {
    let foo_account_name = DEFAULT_ACCOUNT_NAME.clone();
    let foo_account_id = DEFAULT_ACCOUNT_ID.clone();
    let foo_dataset_name = DatasetName::new_unchecked("foo");
    let foo_dataset_alias =
        DatasetAlias::new(Some(foo_account_name.clone()), foo_dataset_name.clone());
    let mock_authentication_service = MockAuthenticationService::with_custom_account(
        foo_account_id.clone(),
        foo_account_name.clone(),
    );

    let mock_dataset_action_authorizer = MockDatasetActionAuthorizer::allowing();
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
        mock_authentication_service: Some(mock_authentication_service),
        mock_dataset_action_authorizer: Some(mock_dataset_action_authorizer),
        ..Default::default()
    });

    let create_result = harness.create_root_dataset(foo_dataset_alias).await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let request_code =
        FlowConfigHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

    // Should return list of flows for account
    let request_code = FlowConfigHarness::list_flows_query(&foo_account_name);
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
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "ingestResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "finishedAt": null,
                                        },
                                        "tasks": [],
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

#[test_log::test(tokio::test)]
async fn test_pause_resume_account_flows() {
    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_account_name = DEFAULT_ACCOUNT_NAME.clone();
    let foo_account_id = DEFAULT_ACCOUNT_ID.clone();
    let foo_dataset_alias = DatasetAlias::new(
        Some(foo_account_name.clone()),
        DatasetName::new_unchecked("foo"),
    );
    let mock_authentication_service = MockAuthenticationService::with_custom_account(
        foo_account_id.clone(),
        foo_account_name.clone(),
    );

    let mock_dataset_action_authorizer = MockDatasetActionAuthorizer::allowing();
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
        mock_authentication_service: Some(mock_authentication_service),
        mock_dataset_action_authorizer: Some(mock_dataset_action_authorizer),
        ..Default::default()
    });
    let foo_create_result = harness.create_root_dataset(foo_dataset_alias).await;

    let request_code =
        FlowConfigHarness::trigger_flow_mutation(&foo_create_result.dataset_handle.id, "INGEST");
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

    let request_code = FlowConfigHarness::list_flows_query(&foo_account_name);
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
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "datasetId": foo_create_result.dataset_handle.id.to_string(),
                                            "ingestResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "finishedAt": null,
                                        },
                                        "tasks": [],
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

    let mutation_code = FlowConfigHarness::set_config_time_delta_mutation(
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

    let mutation_code = FlowConfigHarness::pause_account_flows(&foo_account_name);
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
                        "configs": {
                            "pauseAccountDatasetFlows": true
                        }
                    }
                }
            }
        })
    );

    let request_code =
        FlowConfigHarness::all_paused_config_query(&foo_create_result.dataset_handle.id);
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
                        "configs": {
                            "allPaused": true
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowConfigHarness::resume_account_flows(&foo_account_name);
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
                        "configs": {
                            "resumeAccountDatasetFlows": true
                        }
                    }
                }
            }
        })
    );

    let request_code =
        FlowConfigHarness::all_paused_config_query(&foo_create_result.dataset_handle.id);
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
                        "configs": {
                            "allPaused": false
                        }
                    }
                }
            }
        })
    );
}

struct FlowConfigHarness {
    _tempdir: tempfile::TempDir,
    _catalog_base: dill::Catalog,
    _catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
}

#[derive(Default)]
struct FlowRunsHarnessOverrides {
    dependency_graph_mock: Option<MockDependencyGraphRepository>,
    dataset_changes_mock: Option<MockDatasetChangesService>,
    transform_service_mock: Option<MockTransformService>,
    polling_service_mock: Option<MockPollingIngestService>,
    mock_authentication_service: Option<MockAuthenticationService>,
    mock_dataset_action_authorizer: Option<MockDatasetActionAuthorizer>,
}

impl FlowConfigHarness {
    fn with_overrides(overrides: FlowRunsHarnessOverrides) -> Self {
        set_random_jwt_secret();

        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let dataset_changes_mock = overrides.dataset_changes_mock.unwrap_or_default();
        let dependency_graph_mock = overrides.dependency_graph_mock.unwrap_or_default();
        let transform_service_mock = overrides.transform_service_mock.unwrap_or_default();
        let polling_service_mock = overrides.polling_service_mock.unwrap_or_default();
        let mock_authentication_service = overrides.mock_authentication_service.unwrap_or_default();
        let mock_dataset_action_authorizer =
            overrides.mock_dataset_action_authorizer.unwrap_or_default();

        let catalog_base = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(true),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add_value(dataset_changes_mock)
            .bind::<dyn DatasetChangesService, MockDatasetChangesService>()
            .add::<SystemTimeSourceDefault>()
            .add_value(mock_authentication_service)
            .bind::<dyn AuthenticationService, MockAuthenticationService>()
            .add_value(mock_dataset_action_authorizer)
            .bind::<dyn kamu::domain::auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(dependency_graph_mock)
            .bind::<dyn DependencyGraphRepository, MockDependencyGraphRepository>()
            .add::<FlowConfigurationServiceImpl>()
            .add::<FlowConfigurationEventStoreInMem>()
            .add::<FlowServiceImpl>()
            .add::<FlowEventStoreInMem>()
            .add_value(FlowServiceRunConfig::new(
                Duration::try_seconds(1).unwrap(),
                Duration::try_minutes(1).unwrap(),
            ))
            .add::<TaskSchedulerImpl>()
            .add::<TaskSystemEventStoreInMemory>()
            .add_value(transform_service_mock)
            .bind::<dyn TransformService, MockTransformService>()
            .add_value(polling_service_mock)
            .bind::<dyn PollingIngestService, MockPollingIngestService>()
            .build();

        // Init dataset with no sources
        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&catalog_base);

        let dataset_repo = catalog_authorized
            .get_one::<dyn DatasetRepository>()
            .unwrap();

        Self {
            _tempdir: tempdir,
            _catalog_base: catalog_base,
            _catalog_anonymous: catalog_anonymous,
            catalog_authorized,
            dataset_repo,
        }
    }

    async fn create_root_dataset(&self, dataset_alias: DatasetAlias) -> CreateDatasetResult {
        self.dataset_repo
            .create_dataset_from_snapshot(
                MetadataFactory::dataset_snapshot()
                    .kind(DatasetKind::Root)
                    .name(dataset_alias)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap()
    }

    fn list_flows_query(account_name: &AccountName) -> String {
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
                                      description {
                                          __typename
                                          ... on FlowDescriptionDatasetHardCompacting {
                                              datasetId
                                              compactingResult {
                                                  ... on FlowDescriptionHardCompactingSuccess {
                                                      originalBlocksCount
                                                      resultingBlocksCount
                                                      newHead
                                                  }
                                                  ... on FlowDescriptionHardCompactingNothingToDo {
                                                      message
                                                  }
                                              }
                                          }
                                          ... on FlowDescriptionDatasetExecuteTransform {
                                              datasetId
                                              transformResult {
                                                  numBlocks
                                                  numRecords
                                              }
                                          }
                                          ... on FlowDescriptionDatasetPollingIngest {
                                              datasetId
                                              ingestResult {
                                                  numBlocks
                                                  numRecords
                                              }
                                          }
                                          ... on FlowDescriptionDatasetPushIngest {
                                              datasetId
                                              sourceName
                                              inputRecordsCount
                                              ingestResult {
                                                  numBlocks
                                                  numRecords
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
                                                  ...on FlowFailedMessage {
                                                      message
                                                  }
                                                  ...on FlowDatasetCompactedFailedError {
                                                      message
                                                      rootDataset {
                                                          id
                                                      }
                                                  }
                                              }
                                          }
                                      }
                                      timing {
                                          awaitingExecutorSince
                                          runningSince
                                          finishedAt
                                      }
                                      tasks {
                                          taskId
                                          status
                                          outcome
                                      }
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

    fn trigger_flow_mutation(id: &DatasetID, dataset_flow_type: &str) -> String {
        indoc!(
            r#"
          mutation {
              datasets {
                  byId (datasetId: "<id>") {
                      flows {
                          runs {
                              triggerFlow (
                                  datasetFlowType: "<dataset_flow_type>",
                              ) {
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
                                                      ...on FlowFailedMessage {
                                                          message
                                                      }
                                                      ...on FlowDatasetCompactedFailedError {
                                                          message
                                                          rootDataset {
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
        .replace("<dataset_flow_type>", dataset_flow_type)
    }

    fn pause_account_flows(account_name: &AccountName) -> String {
        indoc!(
            r#"
            mutation {
                accounts {
                    byName (accountName: "<name>") {
                        flows {
                            configs {
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

    fn resume_account_flows(account_name: &AccountName) -> String {
        indoc!(
            r#"
            mutation {
                accounts {
                    byName (accountName: "<name>") {
                        flows {
                            configs {
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

    fn all_paused_config_query(id: &DatasetID) -> String {
        indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
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

    fn set_config_time_delta_mutation(
        id: &DatasetID,
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
                            configs {
                                setConfigSchedule (
                                    datasetFlowType: "<dataset_flow_type>",
                                    paused: <paused>,
                                    schedule: {
                                        timeDelta: { every: <every>, unit: "<unit>" }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        config {
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
                                            compacting {
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
