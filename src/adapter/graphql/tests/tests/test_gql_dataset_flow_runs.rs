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

use async_graphql::{value, Value};
use chrono::Utc;
use container_runtime::ContainerRuntime;
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::testing::{MetadataFactory, MockDependencyGraphRepository};
use kamu::{
    DataFormatRegistryImpl,
    DatasetRepositoryLocalFs,
    DependencyGraphServiceInMemory,
    EngineProvisionerNull,
    ObjectStoreRegistryImpl,
    PollingIngestServiceImpl,
};
use kamu_core::{
    auth,
    CreateDatasetResult,
    DatasetRepository,
    DependencyGraphRepository,
    PollingIngestService,
    SystemTimeSourceDefault,
};
use kamu_flow_system::{FlowID, FlowServiceRunConfig, FlowServiceTestDriver};
use kamu_flow_system_inmem::{
    FlowConfigurationEventStoreInMem,
    FlowConfigurationServiceInMemory,
    FlowEventStoreInMem,
    FlowServiceInMemory,
};
use kamu_task_system::{TaskEventFinished, TaskEventRunning, TaskID, TaskOutcome};
use kamu_task_system_inmem::{TaskSchedulerInMemory, TaskSystemEventStoreInMemory};
use opendatafabric::{DatasetID, DatasetKind, FAKE_ACCOUNT_ID};

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_trigger_ingest_root_dataset() {
    let harness = FlowRunsHarness::new();
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerFlow": {
                                "__typename": "TriggerFlowSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": "0",
                                    "status": "QUEUED",
                                    "outcome": Value::Null
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let request_code = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "ingestedRecordsCount": Value::Null,
                                        },
                                        "status": "QUEUED",
                                        "outcome": Value::Null,
                                        "timing": {
                                            "runningSince": Value::Null,
                                            "finishedAt": Value::Null,
                                        },
                                        "tasks": [],
                                        "initiator": {
                                            "id": FAKE_ACCOUNT_ID,
                                            "accountName": auth::DEFAULT_ACCOUNT_NAME,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": FAKE_ACCOUNT_ID,
                                                "accountName": auth::DEFAULT_ACCOUNT_NAME,
                                            }
                                        },
                                        "startCondition": Value::Null,
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

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_trigger_execute_query_derived_dataset() {
    let harness = FlowRunsHarness::new();
    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_QUERY",
    );

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerFlow": {
                                "__typename": "TriggerFlowSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": "0",
                                    "status": "QUEUED",
                                    "outcome": Value::Null
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let request_code = FlowRunsHarness::list_flows_query(&create_derived_result.dataset_handle.id);
    let response = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetExecuteQuery",
                                            "datasetId": create_derived_result.dataset_handle.id.to_string(),
                                            "transformedRecordsCount": Value::Null,
                                        },
                                        "status": "QUEUED",
                                        "outcome": Value::Null,
                                        "timing": {
                                            "runningSince": Value::Null,
                                            "finishedAt": Value::Null,
                                        },
                                        "tasks": [],
                                        "initiator": {
                                            "id": FAKE_ACCOUNT_ID,
                                            "accountName": auth::DEFAULT_ACCOUNT_NAME,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": FAKE_ACCOUNT_ID,
                                                "accountName": auth::DEFAULT_ACCOUNT_NAME,
                                            }
                                        },
                                        "startCondition": Value::Null,
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

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_incorrect_dataset_kinds_for_flow_type() {
    let harness = FlowRunsHarness::new();

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    ////

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_root_result.dataset_handle.id,
        "EXECUTE_QUERY",
    );

    let schema = kamu_adapter_graphql::schema_quiet();

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerFlow": {
                                "__typename": "FlowIncompatibleDatasetKind",
                                "message": "Expected a Derivative dataset, but a Root dataset was provided",
                            }
                        }
                    }
                }
            }
        })
    );

    ////

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_derived_result.dataset_handle.id, "INGEST");

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerFlow": {
                                "__typename": "FlowIncompatibleDatasetKind",
                                "message": "Expected a Root dataset, but a Derivative dataset was provided",
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_ingest_root_dataset() {
    let harness = FlowRunsHarness::new();
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    harness.mimic_flow_scheduled(flow_id).await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{:?}", res);
    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelScheduledTasks": {
                                "__typename": "CancelScheduledTasksSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": flow_id,
                                    "status": "FINISHED",
                                    "outcome": "ABORTED"
                                }
                            }
                        }
                    }
                }
            }
        })
    );

}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_transform_derived_dataset() {
    let harness = FlowRunsHarness::new();
    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_QUERY",
    );

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    harness.mimic_flow_scheduled(flow_id).await;

    let mutation_code = FlowRunsHarness::cancel_scheduled_tasks_mutation(
        &create_derived_result.dataset_handle.id,
        flow_id,
    );

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelScheduledTasks": {
                                "__typename": "CancelScheduledTasksSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": flow_id,
                                    "status": "FINISHED",
                                    "outcome": "ABORTED"
                                }
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_wrong_flow_id_fails() {
    let harness = FlowRunsHarness::new();
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, "5");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelScheduledTasks": {
                                "__typename": "FlowNotFound",
                                "message": "Flow '5' was not found",
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_foreign_flow_fails() {
    let harness = FlowRunsHarness::new();
    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_root_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    let mutation_code = FlowRunsHarness::cancel_scheduled_tasks_mutation(
        &create_derived_result.dataset_handle.id,
        flow_id,
    );

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelScheduledTasks": {
                                "__typename": "FlowNotFound",
                                "message": "Flow '0' was not found",
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_queued_flow_fails() {
    let harness = FlowRunsHarness::new();
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    let res_json = response.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    // Note: no scheduling!

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelScheduledTasks": {
                                "__typename": "FlowNotScheduled",
                                "message": format!("Flow '{flow_id}' was not scheduled yet"),
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_already_cancelled_flow() {
    let harness = FlowRunsHarness::new();
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    let res_json = response.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    harness.mimic_flow_scheduled(flow_id).await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);

    // Apply 2nd time
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);

    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelScheduledTasks": {
                                "__typename": "CancelScheduledTasksSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": flow_id,
                                    "status": "FINISHED",
                                    "outcome": "ABORTED"
                                }
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_already_succeeded_flow() {
    let mut dependency_graph_repo_mock = MockDependencyGraphRepository::new();
    dependency_graph_repo_mock
        .expect_list_dependencies_of_all_datasets()
        .return_once(|| Box::pin(futures::stream::empty()));

    let harness = FlowRunsHarness::new_custom(dependency_graph_repo_mock);
    let create_result: CreateDatasetResult = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    let flow_task_id = harness.mimic_flow_scheduled(flow_id).await;
    harness.mimic_task_running(flow_task_id).await;
    harness.mimic_task_completed(flow_task_id).await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{:?}", response);
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelScheduledTasks": {
                                "__typename": "CancelScheduledTasksSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": flow_id,
                                    "status": "FINISHED",
                                    "outcome": "SUCCESS"
                                }
                            }
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_operation_fails() {
    let harness = FlowRunsHarness::new();

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_codes = [
        FlowRunsHarness::trigger_flow_mutation(&create_root_result.dataset_handle.id, "INGEST"),
        FlowRunsHarness::trigger_flow_mutation(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_QUERY",
        ),
        FlowRunsHarness::cancel_scheduled_tasks_mutation(
            &create_root_result.dataset_handle.id,
            "0",
        ),
    ];

    let schema = kamu_adapter_graphql::schema_quiet();
    for mutation_code in mutation_codes {
        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code.clone())
                    .data(harness.catalog_anonymous.clone()),
            )
            .await;

        expect_anonymous_access_error(response);
    }
}

////////////////////////////////////////////////////////////////////////////////////////

struct FlowRunsHarness {
    _tempdir: tempfile::TempDir,
    _catalog_base: dill::Catalog,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
}

impl FlowRunsHarness {
    fn new() -> Self {
        Self::new_custom(MockDependencyGraphRepository::new())
    }

    fn new_custom(dependency_graph_repo: MockDependencyGraphRepository) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let run_info_dir = tempdir.path().join("run");
        let cache_dir = tempdir.path().join("cache");
        std::fs::create_dir(&run_info_dir).unwrap();
        std::fs::create_dir(&cache_dir).unwrap();

        let catalog_base = dill::CatalogBuilder::new()
            .add::<EventBus>()
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(tempdir.path().join("datasets"))
                    .with_multi_tenant(false),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .add::<SystemTimeSourceDefault>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceInMemory>()
            .add_value(dependency_graph_repo)
            .bind::<dyn DependencyGraphRepository, MockDependencyGraphRepository>()
            .add::<FlowConfigurationServiceInMemory>()
            .add::<FlowConfigurationEventStoreInMem>()
            .add::<FlowServiceInMemory>()
            .add::<FlowEventStoreInMem>()
            .add_value(FlowServiceRunConfig::new(chrono::Duration::seconds(1)))
            .add::<TaskSchedulerInMemory>()
            .add::<TaskSystemEventStoreInMemory>()
            .add_builder(
                PollingIngestServiceImpl::builder()
                    .with_cache_dir(cache_dir)
                    .with_run_info_dir(run_info_dir)
                    .with_container_runtime(Arc::new(ContainerRuntime::default()))
                    .with_data_format_registry(Arc::new(DataFormatRegistryImpl::new())),
            )
            .bind::<dyn PollingIngestService, PollingIngestServiceImpl>()
            .add::<EngineProvisionerNull>()
            .add::<ObjectStoreRegistryImpl>()
            .build();

        // Init dataset with no sources
        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&catalog_base);

        let dataset_repo = catalog_authorized
            .get_one::<dyn DatasetRepository>()
            .unwrap();

        Self {
            _tempdir: tempdir,
            _catalog_base: catalog_base,
            catalog_anonymous,
            catalog_authorized,
            dataset_repo,
        }
    }

    async fn create_root_dataset(&self) -> CreateDatasetResult {
        self.dataset_repo
            .create_dataset_from_snapshot(
                None,
                MetadataFactory::dataset_snapshot()
                    .kind(DatasetKind::Root)
                    .name("foo")
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
            )
            .await
            .unwrap()
    }

    async fn create_derived_dataset(&self) -> CreateDatasetResult {
        self.dataset_repo
            .create_dataset_from_snapshot(
                None,
                MetadataFactory::dataset_snapshot()
                    .name("bar")
                    .kind(DatasetKind::Derivative)
                    .push_event(MetadataFactory::set_transform(["foo"]).build())
                    .build(),
            )
            .await
            .unwrap()
    }

    async fn mimic_flow_scheduled(&self, flow_id: &str) -> TaskID {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowServiceTestDriver>()
            .unwrap();

        let flow_id = FlowID::new(flow_id.parse::<u64>().unwrap());
        flow_service_test_driver
            .mimic_flow_scheduled(flow_id)
            .await
            .unwrap()
    }

    async fn mimic_task_running(&self, task_id: TaskID) {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowServiceTestDriver>()
            .unwrap();
        flow_service_test_driver.mimic_running_started();

        let event_bus = self.catalog_authorized.get_one::<EventBus>().unwrap();
        event_bus
            .dispatch_event(TaskEventRunning {
                event_time: Utc::now(),
                task_id,
            })
            .await
            .unwrap();
    }

    async fn mimic_task_completed(&self, task_id: TaskID) {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowServiceTestDriver>()
            .unwrap();
        flow_service_test_driver.mimic_running_started();

        let event_bus = self.catalog_authorized.get_one::<EventBus>().unwrap();
        event_bus
            .dispatch_event(TaskEventFinished {
                event_time: Utc::now(),
                task_id,
                outcome: TaskOutcome::Success,
            })
            .await
            .unwrap();
    }

    fn extract_flow_id_from_trigger_response<'a>(response_json: &serde_json::Value) -> &str {
        response_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
            .as_str()
            .unwrap()
    }

    fn list_flows_query(id: &DatasetID) -> String {
        indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            runs {
                                listFlows {
                                    nodes {
                                        flowId
                                        description {
                                            __typename
                                            ... on FlowDescriptionDatasetCompaction {
                                                datasetId
                                                originalBlocksCount
                                                resultingBlocksCount
                                            }
                                            ... on FlowDescriptionDatasetExecuteQuery {
                                                datasetId
                                                transformedRecordsCount
                                            }
                                            ... on FlowDescriptionDatasetPollingIngest {
                                                datasetId
                                                ingestedRecordsCount
                                            }
                                            ... on FlowDescriptionDatasetPushIngest {
                                                datasetId
                                                sourceName
                                                inputRecordsCount
                                                ingestedRecordsCount
                                            }
                                        }
                                        status
                                        outcome
                                        timing {
                                            runningSince
                                            finishedAt
                                        }
                                        tasks {
                                            taskId
                                            status
                                            outcome
                                        }
                                        initiator {
                                            id
                                            accountName
                                        }
                                        primaryTrigger {
                                            __typename
                                            ... on FlowTriggerInputDatasetFlow {
                                                datasetId
                                                flowType
                                                flowId
                                            }
                                            ... on FlowTriggerManual {
                                                initiator {
                                                    id
                                                    accountName
                                                }
                                            }
                                        }
                                        startCondition {
                                            __typename
                                            ... on FlowStartConditionBatching {
                                                thresholdNewRecords
                                            }
                                            ... on FlowStartConditionThrottling {
                                                intervalSec
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
        .replace("<id>", &id.to_string())
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
                                            outcome
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

    fn cancel_scheduled_tasks_mutation(id: &DatasetID, flow_id: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            runs {
                                cancelScheduledTasks (
                                    flowId: "<flow_id>",
                                ) {
                                    __typename,
                                    message
                                    ... on CancelScheduledTasksSuccess {
                                        flow {
                                            __typename
                                            flowId
                                            status
                                            outcome
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
        .replace("<flow_id>", &flow_id.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
