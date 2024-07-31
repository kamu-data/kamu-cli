// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::sync::Arc;

use async_graphql::value;
use chrono::{DateTime, Duration, DurationRound, Utc};
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::Component;
use event_bus::EventBus;
use indoc::indoc;
use kamu::testing::{
    MetadataFactory,
    MockDatasetChangesService,
    MockDependencyGraphRepository,
    MockPollingIngestService,
    MockTransformService,
};
use kamu::{
    DatasetOwnershipServiceInMemory,
    DatasetRepositoryLocalFs,
    DependencyGraphServiceInMemory,
};
use kamu_accounts::{
    CurrentAccountSubject,
    JwtAuthenticationConfig,
    LoggedAccount,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME_STR,
};
use kamu_accounts_inmem::AccessTokenRepositoryInMemory;
use kamu_accounts_services::{AccessTokenServiceImpl, AuthenticationServiceImpl};
use kamu_core::{
    auth,
    CompactionResult,
    CreateDatasetResult,
    DatasetChangesService,
    DatasetIntervalIncrement,
    DatasetRepository,
    DependencyGraphRepository,
    PollingIngestService,
    PullResult,
    SystemTimeSourceDefault,
    TransformService,
};
use kamu_flow_system::{
    Flow,
    FlowEventStore,
    FlowID,
    FlowServiceRunConfig,
    FlowServiceTestDriver,
    FlowTrigger,
    FlowTriggerAutoPolling,
};
use kamu_flow_system_inmem::{FlowConfigurationEventStoreInMem, FlowEventStoreInMem};
use kamu_flow_system_services::{FlowConfigurationServiceImpl, FlowServiceImpl};
use kamu_task_system as ts;
use kamu_task_system_inmem::TaskSystemEventStoreInMemory;
use kamu_task_system_services::TaskSchedulerImpl;
use opendatafabric::{AccountID, DatasetID, DatasetKind, Multihash};

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_trigger_ingest_root_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: Some(MockDependencyGraphRepository::no_dependencies()),
        dataset_changes_mock: Some(MockDatasetChangesService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

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

    assert!(response.is_ok(), "{response:?}");
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
                                    "status": "WAITING",
                                    "outcome": null
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

    assert!(response.is_ok(), "{response:?}");
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
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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

    let schedule_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;

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
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": null,
                                            "finishedAt": null,
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "QUEUED",
                                                "outcome": null,
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": {
                                            "__typename": "FlowStartConditionExecutor",
                                            "taskId": "0",
                                        },
                                        "configSnapshot": null
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

    let running_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness.mimic_task_running(flow_task_id, running_time).await;

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
                                        "status": "RUNNING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "finishedAt": null,
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "RUNNING",
                                                "outcome": null,
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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

    let complete_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            complete_time,
            ts::TaskOutcome::Success(ts::TaskResult::UpdateDatasetResult(
                ts::TaskUpdateDatasetResult {
                    pull_result: PullResult::Updated {
                        old_head: Some(Multihash::from_digest_sha3_256(b"old-slice")),
                        new_head: Multihash::from_digest_sha3_256(b"new-slice"),
                    },
                },
            )),
        )
        .await;

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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "ingestResult": {
                                                "numBlocks": 1,
                                                "numRecords": 12,
                                            },
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "message": "SUCCESS",
                                        },
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "finishedAt": complete_time.to_rfc3339(),
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "FINISHED",
                                                "outcome": "SUCCESS"
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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
async fn test_trigger_execute_transform_derived_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: Some(MockDependencyGraphRepository::no_dependencies()),
        dataset_changes_mock: Some(MockDatasetChangesService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            },
        )),
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    );

    let schema = kamu_adapter_graphql::schema_quiet();
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
                                    "status": "WAITING",
                                    "outcome": null
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

    assert!(response.is_ok(), "{response:?}");
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
                                            "__typename": "FlowDescriptionDatasetExecuteTransform",
                                            "datasetId": create_derived_result.dataset_handle.id.to_string(),
                                            "transformResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "finishedAt": null,
                                        },
                                        "tasks": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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

    let schedule_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;

    let running_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness.mimic_task_running(flow_task_id, running_time).await;

    let complete_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            complete_time,
            ts::TaskOutcome::Success(ts::TaskResult::UpdateDatasetResult(
                ts::TaskUpdateDatasetResult {
                    pull_result: PullResult::Updated {
                        old_head: Some(Multihash::from_digest_sha3_256(b"old-slice")),
                        new_head: Multihash::from_digest_sha3_256(b"new-slice"),
                    },
                },
            )),
        )
        .await;

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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetExecuteTransform",
                                            "datasetId": create_derived_result.dataset_handle.id.to_string(),
                                            "transformResult": {
                                                "numBlocks": 1,
                                                "numRecords": 5,
                                            },
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "message": "SUCCESS"
                                        },
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "finishedAt": complete_time.to_rfc3339(),
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "FINISHED",
                                                "outcome": "SUCCESS",
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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
async fn test_trigger_compaction_root_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: Some(MockDependencyGraphRepository::no_dependencies()),
        dataset_changes_mock: Some(MockDatasetChangesService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "HARD_COMPACTION");

    let schema = kamu_adapter_graphql::schema_quiet();
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
                                    "status": "WAITING",
                                    "outcome": null
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

    assert!(response.is_ok(), "{response:?}");
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
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "compactionResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "finishedAt": null,
                                        },
                                        "tasks": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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

    let schedule_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;

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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "compactionResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": null,
                                            "finishedAt": null,
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "QUEUED",
                                                "outcome": null,
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": {
                                            "__typename": "FlowStartConditionExecutor",
                                            "taskId": "0",
                                        },
                                        "configSnapshot": null
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

    let running_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness.mimic_task_running(flow_task_id, running_time).await;

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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "compactionResult": null,
                                        },
                                        "status": "RUNNING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "finishedAt": null,
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "RUNNING",
                                                "outcome": null,
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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

    let complete_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();

    let new_head = Multihash::from_digest_sha3_256(b"new-slice");
    harness
        .mimic_task_completed(
            flow_task_id,
            complete_time,
            ts::TaskOutcome::Success(ts::TaskResult::CompactionDatasetResult(
                ts::TaskCompactionDatasetResult {
                    compaction_result: CompactionResult::Success {
                        old_head: Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: new_head.clone(),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                    },
                },
            )),
        )
        .await;

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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "compactionResult": {
                                                "originalBlocksCount": 5,
                                                "resultingBlocksCount": 4,
                                                "newHead": new_head
                                            },
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "message": "SUCCESS"
                                        },
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "finishedAt": complete_time.to_rfc3339(),
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "FINISHED",
                                                "outcome": "SUCCESS",
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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
async fn test_list_flows_with_filters_and_pagination() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_result = harness.create_root_dataset().await;

    let ingest_mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");
    let compaction_mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "HARD_COMPACTION");

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

    // Pure listing

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        runs {
                            listFlows {
                                nodes {
                                    flowId
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
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let response = schema
        .execute(async_graphql::Request::new(request_code).data(harness.catalog_authorized.clone()))
        .await;

    assert!(response.is_ok(), "{response:?}");
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
                                        "flowId": "1",
                                    },
                                    {
                                        "flowId": "0",
                                    }
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

    // Split on 2 pages by 1 element

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        runs {
                            listFlows(perPage: 1, page: 1) {
                                nodes {
                                    flowId
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
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let response = schema
        .execute(async_graphql::Request::new(request_code).data(harness.catalog_authorized.clone()))
        .await;

    assert!(response.is_ok(), "{response:?}");
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
                                    }
                                ],
                                "pageInfo": {
                                    "currentPage": 1,
                                    "totalPages": 2,
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    // Filter by flow type

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        runs {
                            listFlows(
                                filters: {
                                    byFlowType: "HARD_COMPACTION"
                                }
                            ) {
                                nodes {
                                    flowId
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
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let response = schema
        .execute(async_graphql::Request::new(request_code).data(harness.catalog_authorized.clone()))
        .await;

    assert!(response.is_ok(), "{response:?}");
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
                                        "flowId": "1",
                                    }
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

    // Filter by flow status

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        runs {
                            listFlows(
                                filters: {
                                    byStatus: "WAITING"
                                }
                            ) {
                                nodes {
                                    flowId
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
    .replace("<id>", &create_result.dataset_handle.id.to_string());

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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "1",
                                    },
                                    {
                                        "flowId": "0",
                                    }
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

    // Filter by initiator

    let request_code = indoc!(
        r#"
    {
        datasets {
            byId (datasetId: "<id>") {
                flows {
                    runs {
                        listFlows(
                            filters: {
                                byInitiator: { accounts: ["<account_ids>"]}
                            }
                        ) {
                            nodes {
                                flowId
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
    .replace("<id>", &create_result.dataset_handle.id.to_string())
    .replace(
        "<account_ids>",
        [DEFAULT_ACCOUNT_ID.to_string()].join(",").as_str(),
    );

    let response = schema
        .execute(async_graphql::Request::new(request_code).data(harness.catalog_authorized.clone()))
        .await;

    assert!(response.is_ok(), "{response:?}");
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
                                        "flowId": "1",
                                    },
                                    {
                                        "flowId": "0",
                                    }
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

    let request_code = indoc!(
        r#"
    {
        datasets {
            byId (datasetId: "<id>") {
                flows {
                    runs {
                        listFlows(
                            filters: {
                                byInitiator: { system: true }
                            }
                        ) {
                            nodes {
                                flowId
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
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let response = schema
        .execute(async_graphql::Request::new(request_code).data(harness.catalog_authorized.clone()))
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "listFlows": {
                                "nodes": [],
                                "pageInfo": {
                                    "currentPage": 0,
                                    "totalPages": 0,
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
async fn test_list_flow_initiators() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_result = harness.create_root_dataset().await;

    let ingest_mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "INGEST");
    let compaction_mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "HARD_COMPACTION");

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

    // Pure initiator listing
    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        runs {
                            listFlowInitiators {
                                nodes {
                                    accountName
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
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let response = schema
        .execute(async_graphql::Request::new(request_code).data(harness.catalog_authorized.clone()))
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "listFlowInitiators": {
                                "nodes": [
                                    {
                                        "accountName": "kamu",
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
async fn test_conditions_not_met_for_flows() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::without_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::without_active_polling_source()),
    })
    .await;
    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    ////

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_root_result.dataset_handle.id, "INGEST");

    let schema = kamu_adapter_graphql::schema_quiet();

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
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerFlow": {
                                "__typename": "FlowPreconditionsNotMet",
                                "message": "Flow didn't met preconditions: 'No SetPollingSource event defined'",
                            }
                        }
                    }
                }
            }
        })
    );

    ////

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    );

    let schema = kamu_adapter_graphql::schema_quiet();

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
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerFlow": {
                                "__typename": "FlowPreconditionsNotMet",
                                "message": "Flow didn't met preconditions: 'No SetTransform event defined'",
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
async fn test_incorrect_dataset_kinds_for_flow_type() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    ////

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_root_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    );

    let schema = kamu_adapter_graphql::schema_quiet();

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

    assert!(response.is_ok(), "{response:?}");
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

    ////

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_derived_result.dataset_handle.id,
        "HARD_COMPACTION",
    );

    let schema = kamu_adapter_graphql::schema_quiet();

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_ingest_root_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
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

    assert!(response.is_ok(), "{response:?}");
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness.mimic_task_running(task_id, Utc::now()).await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
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
                                    "outcome": {
                                        "message": "ABORTED"
                                    },
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
async fn test_cancel_running_transform_derived_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    );

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness.mimic_task_running(task_id, Utc::now()).await;

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

    assert!(response.is_ok(), "{response:?}");
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
                                    "outcome": {
                                        "message": "ABORTED"
                                    },
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
async fn test_cancel_hard_compaction_root_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_result = harness.create_root_dataset().await;

    let mutation_code =
        FlowRunsHarness::trigger_flow_mutation(&create_result.dataset_handle.id, "HARD_COMPACTION");

    let schema = kamu_adapter_graphql::schema_quiet();
    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness.mimic_task_running(task_id, Utc::now()).await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
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
                                    "outcome": {
                                        "message": "ABORTED"
                                    },
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
async fn test_cancel_wrong_flow_id_fails() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
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

    assert!(response.is_ok(), "{response:?}");
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_foreign_flow_fails() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
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

    assert!(response.is_ok(), "{response:?}");
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

    assert!(response.is_ok(), "{response:?}");
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cancel_waiting_flow() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
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

    assert!(response.is_ok(), "{response:?}");
    let res_json = response.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    // Note: no scheduling of tasks, waiting!

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

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
                                    "outcome": {
                                        "message": "ABORTED"
                                    },
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
async fn test_cancel_already_aborted_flow() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
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

    assert!(response.is_ok(), "{response:?}");
    let res_json = response.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerFlow"]["flow"]["flowId"]
        .as_str()
        .unwrap();

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness.mimic_task_running(task_id, Utc::now()).await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

    let response = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");

    // Apply 2nd time
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
                                    "outcome": {
                                        "message": "ABORTED"
                                    },
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
async fn test_cancel_already_succeeded_flow() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: Some(MockDependencyGraphRepository::no_dependencies()),
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
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

    assert!(response.is_ok(), "{response:?}");
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);

    let flow_task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness.mimic_task_running(flow_task_id, Utc::now()).await;
    harness
        .mimic_task_completed(
            flow_task_id,
            Utc::now(),
            ts::TaskOutcome::Success(ts::TaskResult::Empty),
        )
        .await;

    let mutation_code =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id);

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
                                    "outcome": {
                                        "message": "SUCCESS"
                                    },
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
async fn test_history_of_completed_flow() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: Some(MockDependencyGraphRepository::no_dependencies()),
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

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

    assert!(response.is_ok(), "{response:?}");
    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json);
    harness
        .mimic_flow_secondary_trigger(
            flow_id,
            FlowTrigger::AutoPolling(FlowTriggerAutoPolling {
                trigger_time: Utc::now(),
            }),
        )
        .await;

    let flow_task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness.mimic_task_running(flow_task_id, Utc::now()).await;
    harness
        .mimic_task_completed(
            flow_task_id,
            Utc::now(),
            ts::TaskOutcome::Success(ts::TaskResult::Empty),
        )
        .await;

    let query = FlowRunsHarness::flow_history_query(&create_result.dataset_handle.id, flow_id);

    let response = schema
        .execute(
            async_graphql::Request::new(query.clone()).data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(response.is_ok(), "{response:?}");
    assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "getFlow": {
                                "__typename": "GetFlowSuccess",
                                "message": "Success",
                                "flow": {
                                    "history": [
                                        {
                                            "__typename": "FlowEventInitiated",
                                            "eventId": "0",
                                            "trigger": {
                                                "__typename": "FlowTriggerManual"
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTriggerAdded",
                                            "eventId": "1",
                                            "trigger": {
                                                "__typename": "FlowTriggerAutoPolling"
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventStartConditionUpdated",
                                            "eventId": "2",
                                            "startCondition": {
                                                "__typename" : "FlowStartConditionExecutor"
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "3",
                                            "taskId": "0",
                                            "taskStatus": "QUEUED",
                                            "task": {
                                                "taskId": "0",
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "4",
                                            "taskId": "0",
                                            "taskStatus": "RUNNING",
                                            "task": {
                                                "taskId": "0",
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "5",
                                            "taskId": "0",
                                            "taskStatus": "FINISHED",
                                            "task": {
                                                "taskId": "0",
                                            }
                                        }
                                    ]
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
async fn test_execute_transfrom_flow_error_after_compaction() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: Some(MockDependencyGraphRepository::no_dependencies()),
        dataset_changes_mock: Some(MockDatasetChangesService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

    let create_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code = FlowRunsHarness::trigger_flow_with_compaction_config_mutation(
        &create_result.dataset_handle.id,
        "HARD_COMPACTION",
        10000,
        1_000_000,
    );

    let schema = kamu_adapter_graphql::schema_quiet();
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
                                    "status": "WAITING",
                                    "outcome": null
                                }
                            }
                        }
                    }
                }
            }
        })
    );
    let schedule_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let running_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness.mimic_task_running(flow_task_id, running_time).await;
    let complete_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();

    let new_head = Multihash::from_digest_sha3_256(b"new-slice");
    harness
        .mimic_task_completed(
            flow_task_id,
            complete_time,
            ts::TaskOutcome::Success(ts::TaskResult::CompactionDatasetResult(
                ts::TaskCompactionDatasetResult {
                    compaction_result: CompactionResult::Success {
                        old_head: Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: new_head.clone(),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                    },
                },
            )),
        )
        .await;

    let request_code = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id);
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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "0",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "compactionResult": {
                                                "originalBlocksCount": 5,
                                                "resultingBlocksCount": 4,
                                                "newHead": new_head
                                            },
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "message": "SUCCESS"
                                        },
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "finishedAt": complete_time.to_rfc3339(),
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "0",
                                                "status": "FINISHED",
                                                "outcome": "SUCCESS",
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": {
                                            "__typename": "FlowConfigurationCompactionRule",
                                            "compactionRule": {
                                                "maxSliceRecords": 10000,
                                                "maxSliceSize": 1_000_000,
                                            }
                                        },
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

    let mutation_code = FlowRunsHarness::trigger_flow_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    );
    let schema = kamu_adapter_graphql::schema_quiet();
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
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerFlow": {
                                "__typename": "TriggerFlowSuccess",
                                "message": "Success",
                                "flow": {
                                    "__typename": "Flow",
                                    "flowId": "1",
                                    "status": "WAITING",
                                    "outcome": null
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let schedule_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("1", schedule_time).await;
    let running_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness.mimic_task_running(flow_task_id, running_time).await;
    let complete_time = Utc::now()
        .duration_round(Duration::try_seconds(1).unwrap())
        .unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            complete_time,
            ts::TaskOutcome::Failed(ts::TaskError::UpdateDatasetError(
                ts::UpdateDatasetTaskError::RootDatasetCompacted(ts::RootDatasetCompactedError {
                    dataset_id: create_result.dataset_handle.id.clone(),
                }),
            )),
        )
        .await;

    let request_code = FlowRunsHarness::list_flows_query(&create_derived_result.dataset_handle.id);
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
                        "runs": {
                            "listFlows": {
                                "nodes": [
                                    {
                                        "flowId": "1",
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetExecuteTransform",
                                            "datasetId": create_derived_result.dataset_handle.id.to_string(),
                                            "transformResult": null,
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "reason": {
                                                "message": "Root dataset was compacted",
                                                "rootDataset": {
                                                    "id": create_result.dataset_handle.id.to_string()
                                                }
                                            }
                                        },
                                        "timing": {
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "finishedAt": null,
                                        },
                                        "tasks": [
                                            {
                                                "taskId": "1",
                                                "status": "FINISHED",
                                                "outcome": "FAILED",
                                            }
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null
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
async fn test_anonymous_operation_fails() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: None,
        dataset_changes_mock: None,
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_codes = [
        FlowRunsHarness::trigger_flow_mutation(&create_root_result.dataset_handle.id, "INGEST"),
        FlowRunsHarness::trigger_flow_mutation(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_config_snapshot_returned_correctly() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dependency_graph_mock: Some(MockDependencyGraphRepository::no_dependencies()),
        dataset_changes_mock: Some(MockDatasetChangesService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

    let create_result = harness.create_root_dataset().await;

    let mutation_code = FlowRunsHarness::trigger_flow_with_compaction_config_mutation(
        &create_result.dataset_handle.id,
        "HARD_COMPACTION",
        10000,
        1_000_000,
        false,
    );

    let schema = kamu_adapter_graphql::schema_quiet();
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
                                    "status": "WAITING",
                                    "outcome": null
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

    assert!(response.is_ok(), "{response:?}");
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
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "datasetId": create_result.dataset_handle.id.to_string(),
                                            "compactionResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "finishedAt": null,
                                        },
                                        "tasks": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryTrigger": {
                                            "__typename": "FlowTriggerManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": {
                                            "__typename": "FlowConfigurationCompactionRule",
                                            "compactionRule": {
                                                "maxSliceRecords": 10000,
                                                "maxSliceSize": 1_000_000,
                                                "recursive": false
                                            }
                                        },
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

struct FlowRunsHarness {
    _tempdir: tempfile::TempDir,
    _catalog_base: dill::Catalog,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
    dataset_repo: Arc<dyn DatasetRepository>,
}

#[derive(Default)]
struct FlowRunsHarnessOverrides {
    dependency_graph_mock: Option<MockDependencyGraphRepository>,
    dataset_changes_mock: Option<MockDatasetChangesService>,
    transform_service_mock: Option<MockTransformService>,
    polling_service_mock: Option<MockPollingIngestService>,
}

impl FlowRunsHarness {
    async fn with_overrides(overrides: FlowRunsHarnessOverrides) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let dataset_changes_mock = overrides.dataset_changes_mock.unwrap_or_default();
        let dependency_graph_mock = overrides.dependency_graph_mock.unwrap_or_default();
        let transform_service_mock = overrides.transform_service_mock.unwrap_or_default();
        let polling_service_mock = overrides.polling_service_mock.unwrap_or_default();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<EventBus>()
                .add_builder(
                    DatasetRepositoryLocalFs::builder()
                        .with_root(datasets_dir)
                        .with_multi_tenant(false),
                )
                .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
                .add_value(dataset_changes_mock)
                .bind::<dyn DatasetChangesService, MockDatasetChangesService>()
                .add::<SystemTimeSourceDefault>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
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
                .add::<AuthenticationServiceImpl>()
                .add::<AccessTokenServiceImpl>()
                .add::<AccessTokenRepositoryInMemory>()
                .add_value(JwtAuthenticationConfig::default())
                .add::<DatasetOwnershipServiceInMemory>()
                .add::<DatabaseTransactionRunner>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        // Init dataset with no sources
        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&catalog_base).await;

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

    fn logged_account_id(&self) -> AccountID {
        Self::logged_account_from_catalog(&self.catalog_authorized).account_id
    }

    fn logged_account_from_catalog(catalog: &dill::Catalog) -> LoggedAccount {
        let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
        if let CurrentAccountSubject::Logged(logged) = current_account_subject.as_ref() {
            logged.clone()
        } else {
            panic!("Expected logged current user");
        }
    }

    async fn create_root_dataset(&self) -> CreateDatasetResult {
        self.dataset_repo
            .create_dataset_from_snapshot(
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
                MetadataFactory::dataset_snapshot()
                    .name("bar")
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(["foo"])
                            .build(),
                    )
                    .build(),
            )
            .await
            .unwrap()
    }

    async fn mimic_flow_scheduled(
        &self,
        flow_id: &str,
        schedule_time: DateTime<Utc>,
    ) -> ts::TaskID {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowServiceTestDriver>()
            .unwrap();

        let flow_id = FlowID::new(flow_id.parse::<u64>().unwrap());
        flow_service_test_driver
            .mimic_flow_scheduled(flow_id, schedule_time)
            .await
            .unwrap()
    }

    async fn mimic_flow_secondary_trigger(&self, flow_id: &str, flow_trigger: FlowTrigger) {
        let flow_event_store = self
            .catalog_authorized
            .get_one::<dyn FlowEventStore>()
            .unwrap();

        let mut flow = Flow::load(
            FlowID::new(flow_id.parse::<u64>().unwrap()),
            flow_event_store.as_ref(),
        )
        .await
        .unwrap();

        flow.add_trigger_if_unique(Utc::now(), flow_trigger)
            .unwrap();
        flow.save(flow_event_store.as_ref()).await.unwrap();
    }

    async fn mimic_task_running(&self, task_id: ts::TaskID, event_time: DateTime<Utc>) {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowServiceTestDriver>()
            .unwrap();
        flow_service_test_driver.mimic_running_started();

        let task_event_store = self
            .catalog_anonymous
            .get_one::<dyn ts::TaskSystemEventStore>()
            .unwrap();

        let mut task = ts::Task::load(task_id, task_event_store.as_ref())
            .await
            .unwrap();
        task.run(event_time).unwrap();
        task.save(task_event_store.as_ref()).await.unwrap();

        let event_bus = self.catalog_authorized.get_one::<EventBus>().unwrap();
        event_bus
            .dispatch_event(ts::TaskEventRunning {
                event_time,
                task_id,
            })
            .await
            .unwrap();
    }

    async fn mimic_task_completed(
        &self,
        task_id: ts::TaskID,
        event_time: DateTime<Utc>,
        task_outcome: ts::TaskOutcome,
    ) {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowServiceTestDriver>()
            .unwrap();
        flow_service_test_driver.mimic_running_started();

        let task_event_store = self
            .catalog_anonymous
            .get_one::<dyn ts::TaskSystemEventStore>()
            .unwrap();

        let mut task = ts::Task::load(task_id, task_event_store.as_ref())
            .await
            .unwrap();
        task.finish(event_time, task_outcome.clone()).unwrap();
        task.save(task_event_store.as_ref()).await.unwrap();

        let event_bus = self.catalog_authorized.get_one::<EventBus>().unwrap();
        event_bus
            .dispatch_event(ts::TaskEventFinished {
                event_time,
                task_id,
                outcome: task_outcome,
            })
            .await
            .unwrap();
    }

    fn extract_flow_id_from_trigger_response(response_json: &serde_json::Value) -> &str {
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
                                            ... on FlowDescriptionDatasetHardCompaction {
                                                datasetId
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
                                        initiator {
                                            id
                                            accountName
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
                                                initiator {
                                                    id
                                                    accountName
                                                }
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
                                        configSnapshot {
                                            ... on FlowConfigurationBatching {
                                                maxBatchingInterval {
                                                    every
                                                    unit
                                                }
                                                minRecordsToAwait
                                                __typename
                                            }
                                            ... on FlowConfigurationScheduleRule {
                                                scheduleRule {
                                                    ... on TimeDelta {
                                                        every
                                                        unit
                                                    }
                                                    __typename
                                                }
                                                __typename
                                            }
                                            ... on FlowConfigurationCompactionRule {
                                                compactionRule {
                                                    ... on CompactionFull {
                                                        maxSliceRecords
                                                        maxSliceSize
                                                        recursive
                                                    }
                                                    ... on CompactionMetadataOnly {
                                                        recursive
                                                    }
                                                }
                                                __typename
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

    fn flow_history_query(id: &DatasetID, flow_id: &str) -> String {
        // Note: avoid extracting time-based properties in test
        indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            runs {
                                getFlow(flowId: "<flowId>") {
                                    __typename
                                    ... on GetFlowSuccess {
                                        message
                                        flow {
                                            history {
                                                __typename
                                                eventId
                                                ... on FlowEventInitiated {
                                                    trigger {
                                                        __typename
                                                    }
                                                }
                                                ... on FlowEventStartConditionUpdated {
                                                    startCondition {
                                                        __typename
                                                    }
                                                }
                                                ... on FlowEventTriggerAdded {
                                                    trigger {
                                                        __typename
                                                    }
                                                }
                                                ... on FlowEventTaskChanged {
                                                    taskId
                                                    taskStatus
                                                    task {
                                                        taskId
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
        .replace("<flowId>", flow_id)
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

    fn trigger_flow_with_compaction_config_mutation(
        id: &DatasetID,
        dataset_flow_type: &str,
        max_slice_records: u64,
        max_slice_size: u64,
        recursive: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            runs {
                                triggerFlow (
                                    datasetFlowType: "<dataset_flow_type>",
                                    flowRunConfiguration: {
                                        compaction: {
                                            full: {
                                                maxSliceRecords: <max_slice_records>,
                                                maxSliceSize: <max_slice_size>,
                                                recursive: <recursive>
                                            }
                                        }
                                    }
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
        .replace("<max_slice_records>", &max_slice_records.to_string())
        .replace("<max_slice_size>", &max_slice_size.to_string())
        .replace("<recursive>", if recursive { "true" } else { "false" })
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
        .replace("<flow_id>", flow_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
