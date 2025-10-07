// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use chrono::{DateTime, Duration, DurationRound, Utc};
use futures::TryStreamExt;
use indoc::indoc;
use kamu_accounts::{
    CurrentAccountSubject,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME_STR,
    LoggedAccount,
};
use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FLOW_TYPE_DATASET_INGEST,
};
use kamu_adapter_task_dataset::*;
use kamu_core::{CompactionResult, PullResult, ResetResult, TenancyConfig};
use kamu_datasets::{DatasetIncrementQueryService, DatasetIntervalIncrement};
use kamu_datasets_services::testing::{
    FakeDependencyGraphIndexer,
    MockDatasetIncrementQueryService,
};
use kamu_flow_system::*;
use kamu_task_system::{self as ts, TaskError};
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_task_system_services::TaskSchedulerImpl;
use messaging_outbox::{Outbox, OutboxExt, register_message_dispatcher};

use crate::utils::{
    BaseGQLDatasetHarness,
    BaseGQLFlowHarness,
    GraphQLQueryRequest,
    expect_anonymous_access_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_trigger_ingest_root_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response = FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerIngestFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                            "pollingSource": {
                                                "fetch": {
                                                    "__typename": "FetchStepUrl"
                                                }
                                            }
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]);

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                            "pollingSource": {
                                                "fetch": {
                                                    "__typename": "FetchStepUrl"
                                                }
                                            }
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": {
                                            "__typename": "FlowStartConditionExecutor",
                                            "taskId": "0",
                                        },
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                            "pollingSource": {
                                                "fetch": {
                                                    "__typename": "FetchStepUrl"
                                                }
                                            }
                                        },
                                        "status": "RUNNING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [
                                            "0"
                                        ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let complete_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            ts::TaskOutcome::Success(
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

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": {
                                                "__typename": "FlowDescriptionUpdateResultSuccess",
                                                "numBlocks": 1,
                                                "numRecords": 12,
                                            },
                                            "pollingSource": {
                                                "fetch": {
                                                    "__typename": "FetchStepUrl"
                                                }
                                            }
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "message": "SUCCESS",
                                        },
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": complete_time.to_rfc3339(),
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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
async fn test_trigger_reset_root_dataset_flow() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::default()),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias).await;

    use odf::dataset::MetadataChainExt;
    let root_dataset_blocks: Vec<_> = create_root_result
        .dataset
        .as_metadata_chain()
        .iter_blocks_interval(&create_root_result.head, None, false)
        .try_collect()
        .await
        .unwrap();

    let response = FlowRunsHarness::trigger_reset_flow_mutation(
        &create_root_result.dataset_handle.id,
        &root_dataset_blocks[1].0,
        &root_dataset_blocks[0].0,
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerResetFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]);

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let complete_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            ts::TaskOutcome::Success(
                TaskResultDatasetReset {
                    reset_result: ResetResult {
                        old_head: Some(root_dataset_blocks[0].0.clone()),
                        new_head: root_dataset_blocks[1].0.clone(),
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_root_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_root_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetReset",
                                            "resetResult": {
                                                "newHead": &root_dataset_blocks[1].0,
                                            },
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "message": "SUCCESS"
                                        },
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": complete_time.to_rfc3339(),
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": {
                                            "mode": {
                                                "newHeadHash": &root_dataset_blocks[1].0,
                                            },
                                            "oldHeadHash": &root_dataset_blocks[0].0,
                                        },
                                        "relatedTrigger": null,
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
async fn test_trigger_reset_root_dataset_flow_with_invalid_head() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::default()),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias).await;

    let new_invalid_head = odf::Multihash::from_digest_sha3_256(b"new_invalid_head");
    let old_invalid_head = odf::Multihash::from_digest_sha3_256(b"old_invalid_head");

    let response = FlowRunsHarness::trigger_reset_flow_mutation(
        &create_root_result.dataset_handle.id,
        &new_invalid_head,
        &old_invalid_head,
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerResetFlow": {
                                "__typename": "FlowPreconditionsNotMet",
                                "message": "Flow didn't met preconditions: 'New head hash not found'"
                            }
                        }
                    }
                }
            }
        })
    );

    use odf::dataset::MetadataChainExt;

    let root_dataset_blocks: Vec<_> = create_root_result
        .dataset
        .as_metadata_chain()
        .iter_blocks_interval(&create_root_result.head, None, false)
        .try_collect()
        .await
        .unwrap();

    let response = FlowRunsHarness::trigger_reset_flow_mutation(
        &create_root_result.dataset_handle.id,
        &root_dataset_blocks[0].0,
        &root_dataset_blocks[1].0,
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerResetFlow": {
                                "__typename": "FlowPreconditionsNotMet",
                                "message": "Flow didn't met preconditions: 'Provided head hash is already a head block'"
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
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            },
        )),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let response =
        FlowRunsHarness::trigger_transform_flow_mutation(&create_derived_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerTransformFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let response = FlowRunsHarness::list_flows_query(&create_derived_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_derived_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetExecuteTransform",
                                            "transformResult": null,
                                            "transform": {
                                                "inputs": [
                                                    {
                                                        "alias": "foo"
                                                    }
                                                ],
                                                "transform": {
                                                    "__typename": "TransformSql",
                                                    "engine": "some_engine"
                                                }
                                            }
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]);

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let complete_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            ts::TaskOutcome::Success(
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

    let response = FlowRunsHarness::list_flows_query(&create_derived_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_derived_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetExecuteTransform",
                                            "transformResult": {
                                                "__typename": "FlowDescriptionUpdateResultSuccess",
                                                "numBlocks": 1,
                                                "numRecords": 5,
                                            },
                                            "transform": {
                                                "inputs": [
                                                    {
                                                        "alias": "foo"
                                                    }
                                                ],
                                                "transform": {
                                                    "__typename": "TransformSql",
                                                    "engine": "some_engine"
                                                }
                                            }
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "message": "SUCCESS"
                                        },
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": complete_time.to_rfc3339(),
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response =
        FlowRunsHarness::trigger_compaction_flow_mutation(&create_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerCompactionFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "compactionResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]);

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "compactionResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": {
                                            "__typename": "FlowStartConditionExecutor",
                                            "taskId": "0",
                                        },
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "compactionResult": null,
                                        },
                                        "status": "RUNNING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let complete_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let new_head = odf::Multihash::from_digest_sha3_256(b"new-slice");
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            ts::TaskOutcome::Success(
                TaskResultDatasetHardCompact {
                    compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: new_head.clone(),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
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
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": complete_time.to_rfc3339(),
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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
async fn test_trigger_reset_to_metadata_root_dataset() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::default()),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response =
        FlowRunsHarness::trigger_reset_to_metadata_flow_mutation(&create_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerResetToMetadataFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetResetToMetadata",
                                            "resetToMetadataResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]);

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetResetToMetadata",
                                            "resetToMetadataResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": {
                                            "__typename": "FlowStartConditionExecutor",
                                            "taskId": "0",
                                        },
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetResetToMetadata",
                                            "resetToMetadataResult": null,
                                        },
                                        "status": "RUNNING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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

    let complete_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let new_head = odf::Multihash::from_digest_sha3_256(b"new-slice");
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            ts::TaskOutcome::Success(
                TaskResultDatasetResetToMetadata {
                    compaction_metadata_only_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: new_head.clone(),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetResetToMetadata",
                                            "resetToMetadataResult": {
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
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": complete_time.to_rfc3339(),
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    FlowRunsHarness::trigger_compaction_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
                                    byProcessType: {
                                        primary: {
                                            byFlowTypes: ["HARD_COMPACTION"]
                                        }
                                    }
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
    pretty_assertions::assert_eq!(
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    FlowRunsHarness::trigger_compaction_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
    pretty_assertions::assert_eq!(
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset_no_source(foo_alias).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness.create_derived_dataset_no_transform(bar_alias).await;

    ////

    let response =
        FlowRunsHarness::trigger_ingest_flow_mutation(&create_root_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerIngestFlow": {
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

    let response =
        FlowRunsHarness::trigger_transform_flow_mutation(&create_derived_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerTransformFlow": {
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    ////

    let response =
        FlowRunsHarness::trigger_transform_flow_mutation(&create_root_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerTransformFlow": {
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

    let response =
        FlowRunsHarness::trigger_ingest_flow_mutation(&create_derived_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerIngestFlow": {
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

    let response =
        FlowRunsHarness::trigger_compaction_flow_mutation(&create_derived_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerCompactionFlow": {
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response = FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response_json = response.data.into_json().unwrap();
    let flow_id =
        FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json, "triggerIngestFlow");
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness
        .mimic_task_running(task_id, flow_task_metadata, Utc::now())
        .await;

    // Cancelation
    let response =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
                                "__typename": "CancelFlowRunSuccess",
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let response =
        FlowRunsHarness::trigger_transform_flow_mutation(&create_derived_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(
        &response_json,
        "triggerTransformFlow",
    );
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness
        .mimic_task_running(task_id, flow_task_metadata, Utc::now())
        .await;

    // cancelation

    let response = FlowRunsHarness::cancel_scheduled_tasks_mutation(
        &create_derived_result.dataset_handle.id,
        flow_id,
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
                                "__typename": "CancelFlowRunSuccess",
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response =
        FlowRunsHarness::trigger_compaction_flow_mutation(&create_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(
        &response_json,
        "triggerCompactionFlow",
    );
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness
        .mimic_task_running(task_id, flow_task_metadata, Utc::now())
        .await;

    // cancelation

    let response =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
                                "__typename": "CancelFlowRunSuccess",
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, "5")
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let response =
        FlowRunsHarness::trigger_ingest_flow_mutation(&create_root_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    let response_json = response.data.into_json().unwrap();
    let flow_id =
        FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json, "triggerIngestFlow");

    // cancelation of foreign flow

    let response = FlowRunsHarness::cancel_scheduled_tasks_mutation(
        &create_derived_result.dataset_handle.id,
        flow_id,
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response = FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let res_json = response.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerIngestFlow"]["flow"]
        ["flowId"]
        .as_str()
        .unwrap();

    // Note: no scheduling of tasks, waiting!

    // Cancelation

    let response =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
                                "__typename": "CancelFlowRunSuccess",
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response = FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let res_json = response.data.into_json().unwrap();
    let flow_id = res_json["datasets"]["byId"]["flows"]["runs"]["triggerIngestFlow"]["flow"]
        ["flowId"]
        .as_str()
        .unwrap();
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

    let task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness
        .mimic_task_running(task_id, flow_task_metadata, Utc::now())
        .await;

    // First cancelation

    FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    // Apply 2nd time
    let response =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
                                "__typename": "CancelFlowRunSuccess",
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response = FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response_json = response.data.into_json().unwrap();
    let flow_id =
        FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json, "triggerIngestFlow");
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

    let flow_task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), Utc::now())
        .await;
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            Utc::now(),
            ts::TaskOutcome::Success(ts::TaskResult::empty()),
        )
        .await;

    // Cancelation

    let response =
        FlowRunsHarness::cancel_scheduled_tasks_mutation(&create_result.dataset_handle.id, flow_id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "cancelFlowRun": {
                                "__typename": "CancelFlowRunSuccess",
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
async fn test_history_of_completed_ingest_flow() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: None,
    })
    .await;
    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response = FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response_json = response.data.into_json().unwrap();
    let flow_id =
        FlowRunsHarness::extract_flow_id_from_trigger_response(&response_json, "triggerIngestFlow");
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

    harness
        .mimic_flow_secondary_activation_cause(
            flow_id,
            FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
                activation_time: Utc::now(),
            }),
        )
        .await;

    let flow_task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), Utc::now())
        .await;
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            Utc::now(),
            ts::TaskOutcome::Success(ts::TaskResult::empty()),
        )
        .await;

    let response = FlowRunsHarness::flow_history_query(&create_result.dataset_handle.id, flow_id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                            "eventId": "1",
                                            "activationCause": {
                                                "__typename": "FlowActivationCauseManual"
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventScheduledForActivation",
                                            "eventId": "2",
                                        },
                                        {
                                            "__typename": "FlowEventActivationCauseAdded",
                                            "eventId": "3",
                                            "activationCause": {
                                                "__typename": "FlowActivationCauseAutoPolling",
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventStartConditionUpdated",
                                            "eventId": "4",
                                            "startCondition": {
                                                "__typename" : "FlowStartConditionExecutor"
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "5",
                                            "taskId": "0",
                                            "taskStatus": "QUEUED",
                                            "task": {
                                                "taskId": "0",
                                            },
                                            "nextAttemptAt": null,
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "6",
                                            "taskId": "0",
                                            "taskStatus": "RUNNING",
                                            "task": {
                                                "taskId": "0",
                                            },
                                            "nextAttemptAt": null,
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "7",
                                            "taskId": "0",
                                            "taskStatus": "FINISHED",
                                            "task": {
                                                "taskId": "0",
                                            },
                                            "nextAttemptAt": null,
                                        },
                                        {
                                            "__typename": "FlowEventCompleted",
                                            "eventId": "8",
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
async fn test_history_of_completed_transform_flow() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_result = harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let bar_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let response = FlowRunsHarness::trigger_transform_flow_mutation(&bar_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let response_json = response.data.into_json().unwrap();
    let flow_id = FlowRunsHarness::extract_flow_id_from_trigger_response(
        &response_json,
        "triggerTransformFlow",
    );
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, flow_id)]);

    harness
        .mimic_flow_secondary_activation_cause(
            flow_id,
            FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                activation_time: Utc::now(),
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                changes: ResourceChanges::NewData(ResourceDataChanges {
                    blocks_added: 1,
                    records_added: 5,
                    new_watermark: None,
                }),
                details: serde_json::to_value(DatasetResourceUpdateDetails {
                    dataset_id: foo_result.dataset_handle.id.clone(),
                    source: DatasetUpdateSource::ExternallyDetectedChange,
                    new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                    old_head_maybe: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                })
                .unwrap(),
            }),
        )
        .await;

    harness
        .mimic_flow_secondary_activation_cause(
            flow_id,
            FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                activation_time: Utc::now(),
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                changes: ResourceChanges::NewData(ResourceDataChanges {
                    blocks_added: 1,
                    records_added: 5,
                    new_watermark: None,
                }),
                details: serde_json::to_value(DatasetResourceUpdateDetails {
                    dataset_id: foo_result.dataset_handle.id.clone(),
                    source: DatasetUpdateSource::UpstreamFlow {
                        flow_type: FLOW_TYPE_DATASET_INGEST.to_string(),
                        flow_id: FlowID::new(5),
                        maybe_flow_config_snapshot: None,
                    },
                    new_head: odf::Multihash::from_digest_sha3_256(b"new-slice"),
                    old_head_maybe: Some(odf::Multihash::from_digest_sha3_256(b"old-slice")),
                })
                .unwrap(),
            }),
        )
        .await;

    let flow_task_id = harness.mimic_flow_scheduled(flow_id, Utc::now()).await;
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), Utc::now())
        .await;
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            Utc::now(),
            ts::TaskOutcome::Success(ts::TaskResult::empty()),
        )
        .await;

    let response = FlowRunsHarness::flow_history_query(&bar_result.dataset_handle.id, flow_id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                            "eventId": "1",
                                            "activationCause": {
                                                "__typename": "FlowActivationCauseManual"
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventScheduledForActivation",
                                            "eventId": "2",
                                        },
                                        {
                                            "__typename": "FlowEventActivationCauseAdded",
                                            "eventId": "3",
                                            "activationCause": {
                                                "__typename": "FlowActivationCauseDatasetUpdate",
                                                "dataset": {
                                                    "id": foo_result.dataset_handle.id.to_string(),
                                                    "name": "foo"
                                                },
                                                "source": {
                                                    "__typename": "FlowActivationCauseDatasetUpdateSourceExternallyDetectedChange"
                                                }
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventActivationCauseAdded",
                                            "eventId": "4",
                                            "activationCause": {
                                                "__typename": "FlowActivationCauseDatasetUpdate",
                                                "dataset": {
                                                    "id": foo_result.dataset_handle.id.to_string(),
                                                    "name": "foo"
                                                },
                                                "source": {
                                                    "__typename": "FlowActivationCauseDatasetUpdateSourceUpstreamFlow",
                                                    "flowId": "5"
                                                }
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventStartConditionUpdated",
                                            "eventId": "5",
                                            "startCondition": {
                                                "__typename" : "FlowStartConditionExecutor"
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "6",
                                            "taskId": "0",
                                            "taskStatus": "QUEUED",
                                            "task": {
                                                "taskId": "0",
                                            },
                                            "nextAttemptAt": null,
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "7",
                                            "taskId": "0",
                                            "taskStatus": "RUNNING",
                                            "task": {
                                                "taskId": "0",
                                            },
                                            "nextAttemptAt": null,
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "8",
                                            "taskId": "0",
                                            "taskStatus": "FINISHED",
                                            "task": {
                                                "taskId": "0",
                                            },
                                            "nextAttemptAt": null,
                                        },
                                        {
                                            "__typename": "FlowEventCompleted",
                                            "eventId": "9",
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
async fn test_execute_transform_flow_error_after_compaction() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let response = FlowRunsHarness::trigger_compaction_flow_mutation_with_config(
        &create_root_result.dataset_handle.id,
        10000,
        1_000_000,
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerCompactionFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]);

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;
    let complete_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let new_head = odf::Multihash::from_digest_sha3_256(b"new-slice");
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            ts::TaskOutcome::Success(
                TaskResultDatasetHardCompact {
                    compaction_result: CompactionResult::Success {
                        old_head: odf::Multihash::from_digest_sha3_256(b"old-slice"),
                        new_head: new_head.clone(),
                        old_num_blocks: 5,
                        new_num_blocks: 4,
                    },
                }
                .into_task_result(),
            ),
        )
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_root_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_root_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
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
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": complete_time.to_rfc3339(),
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": {
                                            "__typename": "FlowConfigRuleCompaction",
                                            "maxSliceRecords": 10000,
                                            "maxSliceSize": 1_000_000,
                                        },
                                        "relatedTrigger": null,
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

    let response =
        FlowRunsHarness::trigger_transform_flow_mutation(&create_derived_result.dataset_handle.id)
            .execute(&schema, &harness.catalog_authorized)
            .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerTransformFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("1", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "1")]);

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;
    let complete_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time,
            ts::TaskOutcome::Failed(
                TaskErrorDatasetUpdate::InputDatasetCompacted(InputDatasetCompactedError {
                    dataset_id: create_root_result.dataset_handle.id.clone(),
                })
                .into_task_error(),
            ),
        )
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_derived_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_derived_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetExecuteTransform",
                                            "transform": {
                                                "inputs": [
                                                    {
                                                        "alias": "foo"
                                                    }
                                                ],
                                                "transform": {
                                                    "__typename": "TransformSql",
                                                    "engine": "some_engine",
                                                }
                                            },
                                            "transformResult": null,
                                        },
                                        "status": "FINISHED",
                                        "outcome": {
                                            "reason": {
                                                "message": "Input dataset was compacted",
                                                "inputDataset": {
                                                    "id": create_root_result.dataset_handle.id.to_string()
                                                }
                                            }
                                        },
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": schedule_time.to_rfc3339(),
                                            "runningSince": running_time.to_rfc3339(),
                                            "lastAttemptFinishedAt": complete_time.to_rfc3339(),
                                        },
                                        "taskIds": [ "1" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": null,
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
        dataset_changes_mock: None,
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let mutation_requests = [
        FlowRunsHarness::trigger_ingest_flow_mutation(&create_root_result.dataset_handle.id),
        FlowRunsHarness::trigger_transform_flow_mutation(&create_derived_result.dataset_handle.id),
        FlowRunsHarness::cancel_scheduled_tasks_mutation(
            &create_root_result.dataset_handle.id,
            "0",
        ),
    ];

    for mutation_request in mutation_requests {
        let response = mutation_request
            .expect_error()
            .execute(&schema, &harness.catalog_anonymous)
            .await;

        expect_anonymous_access_error(response);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_config_snapshot_returned_correctly() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let response = FlowRunsHarness::trigger_compaction_flow_mutation_with_config(
        &create_result.dataset_handle.id,
        10000,
        1_000_000,
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerCompactionFlow": {
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

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetHardCompaction",
                                            "compactionResult": null,
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": {
                                            "__typename": "FlowConfigRuleCompaction",
                                            "maxSliceRecords": 10000,
                                            "maxSliceSize": 1_000_000,
                                        },
                                        "relatedTrigger": null,
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
async fn test_trigger_ingest_root_dataset_with_retry_policy() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
    })
    .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    // Set ingest retry policy for the flow
    let response = harness
        .set_ingest_config(
            &create_result.dataset_handle.id,
            false,
            Some(value!({
                "maxAttempts": 2,
                "minDelay": {
                    "every": 1,
                    "unit": "MINUTES"
                },
                "backoffType": "FIXED"
            })),
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "configs": {
                            "setIngestConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "rule": {
                                        "__typename": "FlowConfigRuleIngest",
                                        "fetchUncacheable": false,
                                    },
                                    "retryPolicy": {
                                        "__typename": "FlowRetryPolicy",
                                        "maxAttempts": 2,
                                        "minDelay": {
                                            "every": 1,
                                            "unit": "MINUTES"
                                        },
                                        "backoffType": "FIXED"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    // Trigger the flow manually

    let response = FlowRunsHarness::trigger_ingest_flow_mutation(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "runs": {
                            "triggerIngestFlow": {
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

    // Main run attempt: failure

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    let flow_task_id = harness.mimic_flow_scheduled("0", schedule_time).await;
    let flow_task_metadata = ts::TaskMetadata::from(vec![(METADATA_TASK_FLOW_ID, "0")]);

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let complete_time_0 = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata.clone(),
            complete_time_0,
            ts::TaskOutcome::Failed(TaskError::empty_recoverable()),
        )
        .await;

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let next_scheduled_at_0 = complete_time_0 + Duration::minutes(1);

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                            "pollingSource": {
                                                "fetch": {
                                                    "__typename": "FetchStepUrl"
                                                }
                                            }
                                        },
                                        "status": "RETRYING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": next_scheduled_at_0.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": complete_time_0.to_rfc3339(),
                                        },
                                        "taskIds": [ "0" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": {
                                            "fetchUncacheable": false,
                                            "__typename": "FlowConfigRuleIngest",
                                        },
                                        "relatedTrigger": null,
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

    // Retry attempt 1 - failure again

    let flow_task_id = harness.mimic_flow_scheduled("0", next_scheduled_at_0).await;

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let complete_time_1 = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata.clone(),
            complete_time_1,
            ts::TaskOutcome::Failed(TaskError::empty_recoverable()),
        )
        .await;

    let next_scheduled_at_1 = complete_time_1 + Duration::minutes(1);

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                            "pollingSource": {
                                                "fetch": {
                                                    "__typename": "FetchStepUrl"
                                                }
                                            }
                                        },
                                        "status": "RETRYING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": next_scheduled_at_1.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": complete_time_1.to_rfc3339(),
                                        },
                                        "taskIds": [ "0", "1" ],
                                        "initiator": {
                                            "id": harness.logged_account_id().to_string(),
                                            "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                        },
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseManual",
                                            "initiator": {
                                                "id": harness.logged_account_id().to_string(),
                                                "accountName": DEFAULT_ACCOUNT_NAME_STR,
                                            }
                                        },
                                        "startCondition": null,
                                        "configSnapshot": {
                                            "fetchUncacheable": false,
                                            "__typename": "FlowConfigRuleIngest",
                                        },
                                        "relatedTrigger": null,
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

    // Retry attempt 2 - success

    let flow_task_id = harness.mimic_flow_scheduled("0", next_scheduled_at_1).await;

    let running_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_running(flow_task_id, flow_task_metadata.clone(), running_time)
        .await;

    let complete_time_2 = Utc::now().duration_round(Duration::seconds(1)).unwrap();
    harness
        .mimic_task_completed(
            flow_task_id,
            flow_task_metadata,
            complete_time_2,
            ts::TaskOutcome::Success(
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

    // Now, let's see flow history with these retries
    let response = FlowRunsHarness::flow_history_query(
        &create_result.dataset_handle.id,
        "0", /* flowId */
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

    pretty_assertions::assert_eq!(
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
                                            "eventId": "1",
                                            "activationCause": {
                                                "__typename": "FlowActivationCauseManual",
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventScheduledForActivation",
                                            "eventId": "2",
                                        },
                                        {
                                            "__typename": "FlowEventStartConditionUpdated",
                                            "eventId": "3",
                                            "startCondition": {
                                                "__typename": "FlowStartConditionExecutor",
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "4",
                                            "taskId": "0",
                                            "taskStatus": "QUEUED",
                                            "task": {
                                                "taskId": "0"
                                            },
                                            "nextAttemptAt": null
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "5",
                                            "taskId": "0",
                                            "taskStatus": "RUNNING",
                                            "task": {
                                                "taskId": "0"
                                            },
                                            "nextAttemptAt": null
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "6",
                                            "taskId": "0",
                                            "taskStatus": "FINISHED",
                                            "task": {
                                                "taskId": "0"
                                            },
                                            "nextAttemptAt": next_scheduled_at_0.to_rfc3339()
                                        },
                                        {
                                            "__typename": "FlowEventStartConditionUpdated",
                                            "eventId": "7",
                                            "startCondition": {
                                                "__typename": "FlowStartConditionExecutor",
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "8",
                                            "taskId": "1",
                                            "taskStatus": "QUEUED",
                                            "task": {
                                                "taskId": "1"
                                            },
                                            "nextAttemptAt": null
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "9",
                                            "taskId": "1",
                                            "taskStatus": "RUNNING",
                                            "task": {
                                                "taskId": "1"
                                            },
                                            "nextAttemptAt": null
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "10",
                                            "taskId": "1",
                                            "taskStatus": "FINISHED",
                                            "task": {
                                                "taskId": "1"
                                            },
                                            "nextAttemptAt": next_scheduled_at_1.to_rfc3339()
                                        },
                                        {
                                            "__typename": "FlowEventStartConditionUpdated",
                                            "eventId": "11",
                                            "startCondition": {
                                                "__typename": "FlowStartConditionExecutor",
                                            }
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "12",
                                            "taskId": "2",
                                            "taskStatus": "QUEUED",
                                            "task": {
                                                "taskId": "2"
                                            },
                                            "nextAttemptAt": null
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "13",
                                            "taskId": "2",
                                            "taskStatus": "RUNNING",
                                            "task": {
                                                "taskId": "2"
                                            },
                                            "nextAttemptAt": null
                                        },
                                        {
                                            "__typename": "FlowEventTaskChanged",
                                            "eventId": "14",
                                            "taskId": "2",
                                            "taskStatus": "FINISHED",
                                            "task": {
                                                "taskId": "2"
                                            },
                                            "nextAttemptAt": null
                                        },
                                        {
                                            "__typename": "FlowEventCompleted",
                                            "eventId": "15",
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

#[test_group::group(flaky)]
#[test_log::test(tokio::test)]
async fn test_trigger_flow_automatically_via_schedule() {
    let harness = FlowRunsHarness::with_overrides(FlowRunsHarnessOverrides {
        dataset_changes_mock: Some(MockDatasetIncrementQueryService::with_increment_between(
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 12,
                updated_watermark: None,
            },
        )),
    })
    .await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let schedule_time = Utc::now().duration_round(Duration::seconds(1)).unwrap();

    let schema = kamu_adapter_graphql::schema_quiet();

    let response = harness
        .set_time_delta_trigger(
            &create_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
        response.data,
        value!({
            "datasets": {
                "byId": {
                    "flows": {
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "TimeDelta",
                                        "every": 1,
                                        "unit": "DAYS"
                                    },
                                    "reactive": null,
                                    "stopPolicy": {
                                        "__typename": "FlowTriggerStopPolicyAfterConsecutiveFailures",
                                        "maxFailures": 1
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let response = FlowRunsHarness::list_flows_query(&create_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

    pretty_assertions::assert_eq!(
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
                                        "datasetId": create_result.dataset_handle.id.to_string(),
                                        "description": {
                                            "__typename": "FlowDescriptionDatasetPollingIngest",
                                            "ingestResult": null,
                                            "pollingSource": {
                                                "fetch": {
                                                    "__typename": "FetchStepUrl"
                                                }
                                            }
                                        },
                                        "status": "WAITING",
                                        "outcome": null,
                                        "timing": {
                                            "firstAttemptScheduledAt": schedule_time.to_rfc3339(),
                                            "scheduledAt": schedule_time.to_rfc3339(),
                                            "awaitingExecutorSince": null,
                                            "runningSince": null,
                                            "lastAttemptFinishedAt": null,
                                        },
                                        "taskIds": [],
                                        "initiator": null,
                                        "primaryActivationCause": {
                                            "__typename": "FlowActivationCauseAutoPolling",
                                        },
                                        "startCondition": null,
                                        "configSnapshot": null,
                                        "relatedTrigger": {
                                            "paused": false,
                                            "schedule": {
                                                "__typename": "TimeDelta"
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

#[oop::extend(BaseGQLFlowHarness, base_gql_flow_harness)]
struct FlowRunsHarness {
    base_gql_flow_harness: BaseGQLFlowHarness,
}

#[derive(Default)]
struct FlowRunsHarnessOverrides {
    dataset_changes_mock: Option<MockDatasetIncrementQueryService>,
}

impl FlowRunsHarness {
    async fn with_overrides(overrides: FlowRunsHarnessOverrides) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(&base_gql_harness);

        let runs_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(&base_gql_flow_catalog);

            let dataset_changes_mock = overrides.dataset_changes_mock.unwrap_or_default();

            b.add_value(dataset_changes_mock)
                .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
                .add_value(FlowAgentConfig::test_default())
                .add::<TaskSchedulerImpl>()
                .add::<InMemoryTaskEventStore>()
                .add::<FakeDependencyGraphIndexer>();

            kamu_flow_system_services::register_dependencies(&mut b);
            kamu_adapter_flow_dataset::register_dependencies(&mut b, Default::default());

            register_message_dispatcher::<ts::TaskProgressMessage>(
                &mut b,
                ts::MESSAGE_PRODUCER_KAMU_TASK_AGENT,
            );

            register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
            );

            register_message_dispatcher::<FlowTriggerUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
            );

            b.build()
        };

        let base_gql_flow_harness = BaseGQLFlowHarness::new(base_gql_harness, runs_catalog).await;

        Self {
            base_gql_flow_harness,
        }
    }

    fn logged_account_id(&self) -> odf::AccountID {
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

    async fn mimic_flow_scheduled(
        &self,
        flow_id: &str,
        schedule_time: DateTime<Utc>,
    ) -> ts::TaskID {
        let flow_service_test_driver = self
            .catalog_authorized
            .get_one::<dyn FlowAgentTestDriver>()
            .unwrap();

        let flow_id = FlowID::new(flow_id.parse::<u64>().unwrap());
        flow_service_test_driver
            .mimic_flow_scheduled(&self.catalog_authorized, flow_id, schedule_time)
            .await
            .unwrap()
    }

    async fn mimic_flow_secondary_activation_cause(
        &self,
        flow_id: &str,
        activation_cause: FlowActivationCause,
    ) {
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

        flow.add_activation_cause_if_unique(Utc::now(), activation_cause)
            .unwrap();
        flow.save(flow_event_store.as_ref()).await.unwrap();
    }

    async fn mimic_task_running(
        &self,
        task_id: ts::TaskID,
        task_metadata: ts::TaskMetadata,
        event_time: DateTime<Utc>,
    ) {
        let task_event_store = self
            .catalog_anonymous
            .get_one::<dyn ts::TaskEventStore>()
            .unwrap();

        let mut task = ts::Task::load(task_id, task_event_store.as_ref())
            .await
            .unwrap();
        task.run(event_time).unwrap();
        task.save(task_event_store.as_ref()).await.unwrap();

        let outbox = self.catalog_authorized.get_one::<dyn Outbox>().unwrap();
        outbox
            .post_message(
                ts::MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                ts::TaskProgressMessage::running(event_time, task_id, task_metadata),
            )
            .await
            .unwrap();
    }

    async fn mimic_task_completed(
        &self,
        task_id: ts::TaskID,
        task_metadata: ts::TaskMetadata,
        event_time: DateTime<Utc>,
        task_outcome: ts::TaskOutcome,
    ) {
        let task_event_store = self
            .catalog_anonymous
            .get_one::<dyn ts::TaskEventStore>()
            .unwrap();

        let mut task = ts::Task::load(task_id, task_event_store.as_ref())
            .await
            .unwrap();
        task.finish(event_time, task_outcome.clone()).unwrap();
        task.save(task_event_store.as_ref()).await.unwrap();

        let outbox = self.catalog_authorized.get_one::<dyn Outbox>().unwrap();
        outbox
            .post_message(
                ts::MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                ts::TaskProgressMessage::finished(event_time, task_id, task_metadata, task_outcome),
            )
            .await
            .unwrap();
    }

    fn extract_flow_id_from_trigger_response<'a>(
        response_json: &'a serde_json::Value,
        trigger_method: &'static str,
    ) -> &'a str {
        response_json["datasets"]["byId"]["flows"]["runs"][trigger_method]["flow"]["flowId"]
            .as_str()
            .unwrap()
    }

    fn list_flows_query(id: &odf::DatasetID) -> GraphQLQueryRequest {
        let query_code = indoc!(
            r#"
            query($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
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
                                            ... on FlowDescriptionDatasetResetToMetadata {
                                                resetToMetadataResult {
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
                                                transform {
                                                    inputs {
                                                        alias
                                                    }
                                                    transform {
                                                        __typename
                                                        ... on TransformSql {
                                                            engine
                                                        }
                                                    }
                                                }
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
                                            ... on FlowDescriptionDatasetReset {
                                                resetResult {
                                                    newHead
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
                                                pollingSource {
                                                    fetch {
                                                        __typename
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
                                            firstAttemptScheduledAt
                                            scheduledAt
                                            awaitingExecutorSince
                                            runningSince
                                            lastAttemptFinishedAt
                                        }
                                        taskIds
                                        initiator {
                                            id
                                            accountName
                                        }
                                        primaryActivationCause {
                                            __typename
                                            ... on FlowActivationCauseDatasetUpdate {
                                                dataset {
                                                    id
                                                    name
                                                }
                                            }
                                            ... on FlowActivationCauseManual {
                                                initiator {
                                                    id
                                                    accountName
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
                                        configSnapshot {
                                            ... on FlowConfigRuleIngest {
                                                fetchUncacheable
                                                __typename
                                            }
                                            ... on FlowConfigRuleReset {
                                                mode {
                                                    ... on FlowConfigResetPropagationModeCustom {
                                                        newHeadHash
                                                    }
                                                    ... on FlowConfigResetPropagationModeToSeed {
                                                        dummy
                                                    }
                                                }
                                                oldHeadHash
                                            }
                                            ... on FlowConfigRuleCompaction {
                                                __typename
                                                maxSliceRecords
                                                maxSliceSize
                                            }
                                        }
                                        relatedTrigger {
                                            paused
                                            schedule {
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
        );

        GraphQLQueryRequest::new(
            query_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    fn flow_history_query(id: &odf::DatasetID, flow_id: &str) -> GraphQLQueryRequest {
        // Note: avoid extracting time-based properties in test
        let query_code = indoc!(
            r#"
            query($datasetId: DatasetID!, $flowId: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                getFlow(flowId: $flowId) {
                                    __typename
                                    ... on GetFlowSuccess {
                                        message
                                        flow {
                                            history {
                                                __typename
                                                eventId
                                                ... on FlowEventInitiated {
                                                    activationCause {
                                                        __typename
                                                    }
                                                }
                                                ... on FlowEventStartConditionUpdated {
                                                    startCondition {
                                                        __typename
                                                    }
                                                }
                                                ... on FlowEventActivationCauseAdded {
                                                    activationCause {
                                                        __typename
                                                        ... on FlowActivationCauseDatasetUpdate {
                                                            dataset {
                                                                id
                                                                name
                                                            }
                                                            source {
                                                                __typename
                                                                ... on FlowActivationCauseDatasetUpdateSourceUpstreamFlow {
                                                                    flowId
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                                ... on FlowEventTaskChanged {
                                                    taskId
                                                    taskStatus
                                                    task {
                                                        taskId
                                                    }
                                                    nextAttemptAt
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
        );

        GraphQLQueryRequest::new(
            query_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string(),
                "flowId": flow_id
            })),
        )
    }

    fn trigger_ingest_flow_mutation(id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
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
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    fn trigger_transform_flow_mutation(id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerTransformFlow {
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
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    fn trigger_reset_flow_mutation(
        id: &odf::DatasetID,
        new_head_hash: &odf::Multihash,
        old_head_hash: &odf::Multihash,
    ) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!, $newHeadHash: String!, $oldHeadHash: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerResetFlow (
                                    resetConfigInput: {
                                        mode: {
                                            custom: {
                                                newHeadHash: $newHeadHash
                                            }
                                        },
                                        oldHeadHash: $oldHeadHash,
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
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string(),
                "newHeadHash": new_head_hash.to_string(),
                "oldHeadHash": old_head_hash.to_string()
            })),
        )
    }

    fn trigger_compaction_flow_mutation(id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
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
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    fn trigger_compaction_flow_mutation_with_config(
        id: &odf::DatasetID,
        max_slice_records: u64,
        max_slice_size: u64,
    ) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!, $maxSliceRecords: Int!, $maxSliceSize: Int!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerCompactionFlow (
                                    compactionConfigInput: {
                                        maxSliceRecords: $maxSliceRecords,
                                        maxSliceSize: $maxSliceSize,
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
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string(),
                "maxSliceRecords": max_slice_records,
                "maxSliceSize": max_slice_size
            })),
        )
    }

    fn trigger_reset_to_metadata_flow_mutation(id: &odf::DatasetID) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                triggerResetToMetadataFlow {
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
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string()
            })),
        )
    }

    fn cancel_scheduled_tasks_mutation(id: &odf::DatasetID, flow_id: &str) -> GraphQLQueryRequest {
        let mutation_code = indoc!(
            r#"
            mutation($datasetId: DatasetID!, $flowId: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            runs {
                                cancelFlowRun (
                                    flowId: $flowId,
                                ) {
                                    __typename,
                                    message
                                    ... on CancelFlowRunSuccess {
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
        );

        GraphQLQueryRequest::new(
            mutation_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": id.to_string(),
                "flowId": flow_id
            })),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
