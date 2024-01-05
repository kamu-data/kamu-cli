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
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::{DatasetRepositoryLocalFs, DependencyGraphServiceInMemory};
use kamu_core::{auth, CreateDatasetResult, DatasetRepository, SystemTimeSourceDefault};
use kamu_flow_system::{FlowEventStore, FlowServiceRunConfig};
use kamu_flow_system_inmem::{
    FlowConfigurationEventStoreInMem,
    FlowConfigurationServiceInMemory,
    FlowEventStoreInMem,
    FlowServiceInMemory,
};
use kamu_task_system::TaskID;
use kamu_task_system_inmem::{TaskSchedulerInMemory, TaskSystemEventStoreInMemory};
use opendatafabric::{DatasetID, DatasetKind};

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_trigger_ingest_root_dataset() {
    let harness = FlowRunsHarness::new();
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
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
    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{:?}", res);
    let res_json = res.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    let mutation_code =
        FlowRunsHarness::cancel_flow_mutation(&create_result.dataset_handle.id, flow_id);

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
                            "cancelFlow": {
                                "__typename": "CancelFlowSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": flow_id,
                                    "status": "FINISHED",
                                    "outcome": "CANCELLED"
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
    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{:?}", res);
    let res_json = res.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    let mutation_code =
        FlowRunsHarness::cancel_flow_mutation(&create_derived_result.dataset_handle.id, flow_id);

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
                            "cancelFlow": {
                                "__typename": "CancelFlowSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": flow_id,
                                    "status": "FINISHED",
                                    "outcome": "CANCELLED"
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
        FlowRunsHarness::cancel_flow_mutation(&create_result.dataset_handle.id, "5");

    let schema = kamu_adapter_graphql::schema_quiet();
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
                            "cancelFlow": {
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
    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{:?}", res);
    let res_json = res.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    let mutation_code =
        FlowRunsHarness::cancel_flow_mutation(&create_derived_result.dataset_handle.id, flow_id);

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
                            "cancelFlow": {
                                "__typename": "FlowUnmatchedInDataset",
                                "message": "Flow '0' does not belong to dataset 'bar'",
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
    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{:?}", res);
    let res_json = res.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    let mutation_code =
        FlowRunsHarness::cancel_flow_mutation(&create_result.dataset_handle.id, flow_id);

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{:?}", res);

    // Apply 2nd time
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
                            "cancelFlow": {
                                "__typename": "CancelFlowSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": flow_id,
                                    "status": "FINISHED",
                                    "outcome": "CANCELLED"
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
    let harness = FlowRunsHarness::new();
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    // TODO: extract flow ID
    assert!(res.is_ok(), "{:?}", res);
    let res_json = res.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    harness.mimic_flow_completed(flow_id).await;

    let mutation_code =
        FlowRunsHarness::cancel_flow_mutation(&create_result.dataset_handle.id, flow_id);

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{:?}", res);
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
        FlowRunsHarness::cancel_flow_mutation(&create_root_result.dataset_handle.id, "0"),
    ];

    let schema = kamu_adapter_graphql::schema_quiet();
    for mutation_code in mutation_codes {
        let res = schema
            .execute(
                async_graphql::Request::new(mutation_code.clone())
                    .data(harness.catalog_anonymous.clone()),
            )
            .await;

        expect_anonymous_access_error(res);
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
        let tempdir = tempfile::tempdir().unwrap();

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
            .add::<FlowConfigurationServiceInMemory>()
            .add::<FlowConfigurationEventStoreInMem>()
            .add::<FlowServiceInMemory>()
            .add::<FlowEventStoreInMem>()
            .add_value(FlowServiceRunConfig::new(chrono::Duration::seconds(1)))
            .add::<TaskSchedulerInMemory>()
            .add::<TaskSystemEventStoreInMemory>()
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

    async fn mimic_flow_completed(&self, flow_id: &str) {
        let flow_event_store = self
            .catalog_authorized
            .get_one::<dyn FlowEventStore>()
            .unwrap();

        use kamu_flow_system::{Flow, FlowID};

        let flow_id = FlowID::new(flow_id.parse::<u64>().unwrap());
        let mut flow = Flow::load(flow_id, flow_event_store.as_ref())
            .await
            .unwrap();

        let random_task_id = TaskID::new(12345);
        flow.on_task_scheduled(Utc::now(), random_task_id).unwrap();
        flow.on_task_finished(
            Utc::now(),
            random_task_id,
            kamu_task_system::TaskOutcome::Success,
        )
        .unwrap();

        flow.save(flow_event_store.as_ref()).await.unwrap();
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

    fn cancel_flow_mutation(id: &DatasetID, flow_id: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            runs {
                                cancelFlow (
                                    flowId: "<flow_id>",
                                ) {
                                    __typename,
                                    message
                                    ... on CancelFlowSuccess {
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
