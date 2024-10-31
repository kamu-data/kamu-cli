// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::Component;
use indoc::indoc;
use kamu::testing::{MetadataFactory, MockPollingIngestService, MockTransformService};
use kamu::{
    CreateDatasetFromSnapshotUseCaseImpl,
    DatasetRepositoryLocalFs,
    DatasetRepositoryWriter,
    DependencyGraphServiceInMemory,
};
use kamu_core::{
    auth,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    DatasetRepository,
    PollingIngestService,
    TransformService,
};
use kamu_flow_system_inmem::InMemoryFlowConfigurationEventStore;
use kamu_flow_system_services::FlowConfigurationServiceImpl;
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_time_delta_root_dataset() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;

    let create_result = harness.create_root_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        configs {
                            byType (datasetFlowType: "INGEST") {
                                __typename
                                paused
                                ingest {
                                    fetchUncacheable
                                    schedule {
                                        __typename
                                        ... on TimeDelta {
                                            every
                                            unit
                                        }
                                    }
                                }
                                transform {
                                    __typename
                                }
                                compaction {
                                    __typename
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

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
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
                        "configs": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowConfigHarness::set_ingest_config_time_delta_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        false,
        1,
        "DAYS",
        false,
    );

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
                        "configs": {
                            "setConfigIngest": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "paused": false,
                                    "ingest": {
                                        "fetchUncacheable": false,
                                        "schedule": {
                                            "__typename": "TimeDelta",
                                            "every": 1,
                                            "unit": "DAYS"
                                        },
                                    },
                                    "transform": null,
                                    "compaction": null
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowConfigHarness::set_ingest_config_time_delta_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        true,
        2,
        "HOURS",
        false,
    );

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
                        "configs": {
                            "setConfigIngest": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "paused": true,
                                    "ingest": {
                                        "fetchUncacheable": false,
                                        "schedule": {
                                            "__typename": "TimeDelta",
                                            "every": 2,
                                            "unit": "HOURS"
                                        },
                                    },
                                    "transform": null,
                                    "compaction": null
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
async fn test_time_delta_validation() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_result = harness.create_root_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // These cases exceed unit boundary, but must return the same "every" & "unit"
    for test_case in [
        (63, "MINUTES"),
        (30, "HOURS"),
        (8, "DAYS"),
        (169, "DAYS"),
        (169, "WEEKS"),
    ] {
        let mutation_code = FlowConfigHarness::set_ingest_config_time_delta_mutation(
            &create_result.dataset_handle.id,
            "INGEST",
            true,
            test_case.0,
            test_case.1,
            false,
        );

        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code.clone())
                    .data(harness.catalog_authorized.clone()),
            )
            .await;
        assert!(response.is_ok(), "{response:?}");

        let response_json = response.data.into_json().unwrap();
        assert_eq!(
            (test_case.0, test_case.1,),
            FlowConfigHarness::extract_time_delta_from_response(&response_json)
        );
    }

    // These cases exceed unit boundary, but can be compacted to higher level unit
    for test_case in [
        (360, "MINUTES", 6, "HOURS"),
        (48, "HOURS", 2, "DAYS"),
        (7, "DAYS", 1, "WEEKS"),
    ] {
        let mutation_code = FlowConfigHarness::set_ingest_config_time_delta_mutation(
            &create_result.dataset_handle.id,
            "INGEST",
            true,
            test_case.0,
            test_case.1,
            false,
        );

        let response = schema
            .execute(
                async_graphql::Request::new(mutation_code.clone())
                    .data(harness.catalog_authorized.clone()),
            )
            .await;
        assert!(response.is_ok(), "{response:?}");

        let response_json = response.data.into_json().unwrap();
        assert_eq!(
            (test_case.2, test_case.3,),
            FlowConfigHarness::extract_time_delta_from_response(&response_json)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_cron_root_dataset() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_result = harness.create_root_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        configs {
                            byType (datasetFlowType: "INGEST") {
                                __typename
                                paused
                                ingest {
                                    fetchUncacheable
                                    schedule {
                                        __typename
                                        ... on Cron5ComponentExpression {
                                            cron5ComponentExpression
                                        }
                                    }
                                }
                                transform {
                                    __typename
                                }
                                compaction {
                                    __typename
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

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
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
                        "configs": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowConfigHarness::set_ingest_config_cron_expression_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        false,
        "*/2 * * * *",
        false,
    );

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
                        "configs": {
                            "setConfigIngest": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "paused": false,
                                    "ingest": {
                                        "fetchUncacheable": false,
                                        "schedule": {
                                            "__typename": "Cron5ComponentExpression",
                                            "cron5ComponentExpression": "*/2 * * * *",
                                        },
                                    },
                                    "transform": null,
                                    "compaction": null
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowConfigHarness::set_ingest_config_cron_expression_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        true,
        "0 */1 * * *",
        false,
    );

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
                        "configs": {
                            "setConfigIngest": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "paused": true,
                                    "ingest": {
                                        "fetchUncacheable": false,
                                        "schedule": {
                                            "__typename": "Cron5ComponentExpression",
                                            "cron5ComponentExpression": "0 */1 * * *",
                                        },
                                    },
                                    "transform": null,
                                    "compaction": null
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    // Try to pass invalid cron expression
    let invalid_cron_expression = "0 0 */1 *";
    let mutation_code = FlowConfigHarness::set_ingest_config_cron_expression_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        true,
        invalid_cron_expression,
        false,
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_err(), "{res:?}");
    assert_eq!(
        res.errors[0].message,
        format!("Cron expression {invalid_cron_expression} is invalid")
    );

    // Try to pass valid cron expression with year (not supported)
    let past_cron_expression = "0 0 1 JAN ? 2024";
    let mutation_code = FlowConfigHarness::set_ingest_config_cron_expression_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        true,
        past_cron_expression,
        false,
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_err(), "{res:?}");
    assert_eq!(
        res.errors[0].message,
        format!("Cron expression {past_cron_expression} is invalid",)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_transform_derived_dataset() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        configs {
                            byType (datasetFlowType: "EXECUTE_TRANSFORM") {
                                __typename
                                paused
                                ingest {
                                    __typename
                                }
                                transform {
                                    __typename
                                    minRecordsToAwait
                                    maxBatchingInterval {
                                        every
                                        unit
                                    }
                                }
                                compaction {
                                    __typename
                                }
                            }
                        }
                    }
                }
            }
        }
        "#
    )
    .replace("<id>", &create_derived_result.dataset_handle.id.to_string());

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
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
                        "configs": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowConfigHarness::set_config_transform_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        false,
        1,
        (30, "MINUTES"),
    );

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
                        "configs": {
                            "setConfigTransform": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "paused": false,
                                    "ingest": null,
                                    "transform": {
                                        "__typename": "FlowConfigurationTransform",
                                        "minRecordsToAwait": 1,
                                        "maxBatchingInterval": {
                                            "every": 30,
                                            "unit": "MINUTES"
                                        }
                                    },
                                    "compaction": null
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
async fn test_crud_compaction_root_dataset() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_result = harness.create_root_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        configs {
                            byType (datasetFlowType: "HARD_COMPACTION") {
                                __typename
                                paused
                                ingest {
                                    __typename
                                }
                                transform {
                                    __typename
                                }
                                compaction {
                                    __typename
                                    ... on CompactionFull {
                                        maxSliceSize
                                        maxSliceRecords
                                    }
                                    ... on CompactionMetadataOnly {
                                        recursive
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
    .replace("<id>", &create_result.dataset_handle.id.to_string());

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(request_code.clone())
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
                        "configs": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowConfigHarness::set_config_compaction_full_mutation(
        &create_result.dataset_handle.id,
        "HARD_COMPACTION",
        1_000_000,
        10000,
        false,
    );

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
                        "configs": {
                            "setConfigCompaction": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "paused": false,
                                    "ingest": null,
                                    "transform": null,
                                    "compaction": {
                                        "__typename": "CompactionFull",
                                        "maxSliceSize": 1_000_000,
                                        "maxSliceRecords": 10000,
                                        "recursive": false
                                    }
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
async fn test_transform_config_validation() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();

    for test_case in [
        (
            0,
            30,
            "MINUTES",
            "Minimum records to await must be a positive number",
        ),
        (
            1,
            0,
            "MINUTES",
            "Minimum interval to await should be positive",
        ),
        (
            1,
            25,
            "HOURS",
            "Maximum interval to await should not exceed 24 hours",
        ),
    ] {
        let mutation_code = FlowConfigHarness::set_config_transform_mutation(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            true,
            test_case.0,
            (test_case.1, test_case.2),
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
                                "configs": {
                                    "setConfigTransform": {
                                        "__typename": "FlowInvalidTransformConfig",
                                        "message": test_case.3,
                                    }
                                }
                            }
                        }
                    }
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_compaction_config_validation() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();

    for test_case in [
        (
            0,
            1_000_000,
            false,
            "Maximum slice size must be a positive number",
        ),
        (
            1_000_000,
            0,
            false,
            "Maximum slice records must be a positive number",
        ),
    ] {
        let mutation_code = FlowConfigHarness::set_config_compaction_full_mutation(
            &create_derived_result.dataset_handle.id,
            "HARD_COMPACTION",
            test_case.0,
            test_case.1,
            test_case.2,
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
                                "configs": {
                                    "setConfigCompaction": {
                                        "__typename": "FlowInvalidCompactionConfig",
                                        "message": test_case.3,
                                    }
                                }
                            }
                        }
                    }
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_dataset_flows() {
    async fn check_flow_config_status(
        harness: &FlowConfigHarness,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &DatasetID,
        dataset_flow_type: &str,
        expect_paused: bool,
    ) {
        let query = FlowConfigHarness::quick_flow_config_query(dataset_id, dataset_flow_type);

        let res = schema
            .execute(async_graphql::Request::new(query).data(harness.catalog_authorized.clone()))
            .await;
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(
            res.data,
            value!({
                "datasets": {
                    "byId": {
                        "flows": {
                            "configs": {
                                "byType": {
                                    "__typename": "FlowConfiguration",
                                    "paused": expect_paused,
                                }
                            }
                        }
                    }
                }
            })
        );
    }

    async fn check_dataset_all_configs_status(
        harness: &FlowConfigHarness,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &DatasetID,
        expect_paused: bool,
    ) {
        let query = FlowConfigHarness::all_paused_config_query(dataset_id);

        let res = schema
            .execute(async_graphql::Request::new(query).data(harness.catalog_authorized.clone()))
            .await;
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(
            res.data,
            value!({
                "datasets": {
                    "byId": {
                        "flows": {
                            "configs": {
                                "allPaused": expect_paused,
                            }
                        }
                    }
                }
            })
        );
    }

    // Setup initial flow configs for datasets

    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let mutation_set_ingest = FlowConfigHarness::set_ingest_config_time_delta_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
        false,
        1,
        "DAYS",
        false,
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_set_ingest)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let mutation_set_transform = FlowConfigHarness::set_config_transform_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        false,
        1,
        (30, "MINUTES"),
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_set_transform)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let flow_cases = [
        (&create_root_result.dataset_handle.id, "INGEST"),
        (
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
        ),
    ];

    let dataset_cases = [
        &create_root_result.dataset_handle.id,
        &create_derived_result.dataset_handle.id,
    ];

    // Ensure all flow configs are not paused
    for ((dataset_id, dataset_flow_type), expect_paused) in
        flow_cases.iter().zip(vec![false, false])
    {
        check_flow_config_status(
            &harness,
            &schema,
            dataset_id,
            dataset_flow_type,
            expect_paused,
        )
        .await;
    }
    for (dataset_id, expect_paused) in dataset_cases.iter().zip(vec![false, false]) {
        check_dataset_all_configs_status(&harness, &schema, dataset_id, expect_paused).await;
    }

    let mutation_pause_root_compaction = FlowConfigHarness::pause_flows_of_type_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_pause_root_compaction)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    // execute transform should be paused

    for ((dataset_id, dataset_flow_type), expect_paused) in flow_cases.iter().zip(vec![false, true])
    {
        check_flow_config_status(
            &harness,
            &schema,
            dataset_id,
            dataset_flow_type,
            expect_paused,
        )
        .await;
    }
    for (dataset_id, expect_paused) in dataset_cases.iter().zip(vec![false, true]) {
        check_dataset_all_configs_status(&harness, &schema, dataset_id, expect_paused).await;
    }

    // Pause all the root
    let mutation_pause_all_root =
        FlowConfigHarness::pause_all_flows_mutation(&create_root_result.dataset_handle.id);

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_pause_all_root)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Root flows should be paused
    for ((dataset_id, dataset_flow_type), expect_paused) in flow_cases.iter().zip(vec![true, true])
    {
        check_flow_config_status(
            &harness,
            &schema,
            dataset_id,
            dataset_flow_type,
            expect_paused,
        )
        .await;
    }
    for (dataset_id, expect_paused) in dataset_cases.iter().zip(vec![true, true]) {
        check_dataset_all_configs_status(&harness, &schema, dataset_id, expect_paused).await;
    }

    // Resume ingestion
    let mutation_resume_ingest = FlowConfigHarness::resume_flows_of_type_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_resume_ingest)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Only transform of deriving should be paused
    for ((dataset_id, dataset_flow_type), expect_paused) in flow_cases.iter().zip(vec![false, true])
    {
        check_flow_config_status(
            &harness,
            &schema,
            dataset_id,
            dataset_flow_type,
            expect_paused,
        )
        .await;
    }
    for (dataset_id, expect_paused) in dataset_cases.iter().zip(vec![false, true]) {
        check_dataset_all_configs_status(&harness, &schema, dataset_id, expect_paused).await;
    }

    // Pause derived transform
    let mutation_pause_derived_transform = FlowConfigHarness::pause_flows_of_type_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_pause_derived_transform)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Observe status change
    for ((dataset_id, dataset_flow_type), expect_paused) in flow_cases.iter().zip(vec![false, true])
    {
        check_flow_config_status(
            &harness,
            &schema,
            dataset_id,
            dataset_flow_type,
            expect_paused,
        )
        .await;
    }
    for (dataset_id, expect_paused) in dataset_cases.iter().zip(vec![false, true]) {
        check_dataset_all_configs_status(&harness, &schema, dataset_id, expect_paused).await;
    }

    // Resume all derived
    let mutation_resume_derived_all =
        FlowConfigHarness::resume_all_flows_mutation(&create_derived_result.dataset_handle.id);

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_resume_derived_all)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    // Observe status change
    for ((dataset_id, dataset_flow_type), expect_paused) in
        flow_cases.iter().zip(vec![false, false])
    {
        check_flow_config_status(
            &harness,
            &schema,
            dataset_id,
            dataset_flow_type,
            expect_paused,
        )
        .await;
    }
    for (dataset_id, expect_paused) in dataset_cases.iter().zip(vec![false, false]) {
        check_dataset_all_configs_status(&harness, &schema, dataset_id, expect_paused).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_conditions_not_met_for_flows() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::without_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::without_active_polling_source()),
    })
    .await;
    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    ////

    let mutation_code = FlowConfigHarness::set_config_transform_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        false,
        1,
        (30, "MINUTES"),
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
                        "configs": {
                            "setConfigTransform": {
                                "__typename": "FlowPreconditionsNotMet",
                                "message": "Flow didn't met preconditions: 'No SetTransform event defined'",
                            }
                        }
                    }
                }
            }
        })
    );

    ////

    let mutation_code = FlowConfigHarness::set_ingest_config_cron_expression_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
        false,
        "0 */2 * * *",
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
                        "configs": {
                            "setConfigIngest": {
                                "__typename": "FlowPreconditionsNotMet",
                                "message": "Flow didn't met preconditions: 'No SetPollingSource event defined'",
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
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    ////

    let mutation_code = FlowConfigHarness::set_config_transform_mutation(
        &create_root_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        false,
        1,
        (30, "MINUTES"),
    );

    let schema = kamu_adapter_graphql::schema_quiet();

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
                        "configs": {
                            "setConfigTransform": {
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

    let mutation_code = FlowConfigHarness::set_ingest_config_cron_expression_mutation(
        &create_derived_result.dataset_handle.id,
        "INGEST",
        false,
        "0 */2 * * *",
        false,
    );

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
                        "configs": {
                            "setConfigIngest": {
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

    let mutation_code = FlowConfigHarness::set_ingest_config_time_delta_mutation(
        &create_derived_result.dataset_handle.id,
        "INGEST",
        false,
        2,
        "HOURS",
        false,
    );

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
                        "configs": {
                            "setConfigIngest": {
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

    let mutation_code = FlowConfigHarness::set_config_compaction_full_mutation(
        &create_derived_result.dataset_handle.id,
        "HARD_COMPACTION",
        1000,
        1000,
        false,
    );

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
                        "configs": {
                            "setConfigCompaction": {
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

#[test_log::test(tokio::test)]
async fn test_set_metadataonly_compaction_config_form_derivative() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code = FlowConfigHarness::set_config_compaction_metadata_only_mutation(
        &create_derived_result.dataset_handle.id,
        "HARD_COMPACTION",
        false,
    );

    let schema = kamu_adapter_graphql::schema_quiet();
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
                        "configs": {
                            "setConfigCompaction": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "paused": false,
                                    "ingest": null,
                                    "transform": null,
                                    "compaction": {
                                        "__typename": "CompactionMetadataOnly",
                                        "recursive": false
                                    }
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
async fn test_set_config_for_hard_compaction_fails() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::without_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::without_active_polling_source()),
    })
    .await;
    let create_root_result = harness.create_root_dataset().await;

    ////

    let mutation_code = FlowConfigHarness::set_config_transform_mutation(
        &create_root_result.dataset_handle.id,
        "HARD_COMPACTION",
        false,
        1,
        (30, "MINUTES"),
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
                        "configs": {
                            "setConfigTransform": {
                                "__typename": "FlowTypeIsNotSupported",
                                "message": "Flow type is not supported",
                            }
                        }
                    }
                }
            }
        })
    );

    ////

    let mutation_code = FlowConfigHarness::set_ingest_config_cron_expression_mutation(
        &create_root_result.dataset_handle.id,
        "HARD_COMPACTION",
        false,
        "0 */2 * * *",
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
                        "configs": {
                            "setConfigIngest": {
                                "__typename": "FlowTypeIsNotSupported",
                                "message": "Flow type is not supported",
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
async fn test_anonymous_setters_fail() {
    let harness = FlowConfigHarness::with_overrides(FlowRunsHarnessOverrides {
        transform_service_mock: Some(MockTransformService::with_set_transform()),
        polling_service_mock: Some(MockPollingIngestService::with_active_polling_source()),
    })
    .await;
    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_codes = [
        FlowConfigHarness::set_ingest_config_time_delta_mutation(
            &create_root_result.dataset_handle.id,
            "INGEST",
            false,
            30,
            "MINUTES",
            false,
        ),
        FlowConfigHarness::set_ingest_config_cron_expression_mutation(
            &create_root_result.dataset_handle.id,
            "INGEST",
            false,
            "* */2 * * *",
            false,
        ),
        FlowConfigHarness::set_config_transform_mutation(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            false,
            1,
            (30, "MINUTES"),
        ),
        FlowConfigHarness::pause_flows_of_type_mutation(
            &create_root_result.dataset_handle.id,
            "INGEST",
        ),
        FlowConfigHarness::resume_flows_of_type_mutation(
            &create_root_result.dataset_handle.id,
            "INGEST",
        ),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct FlowRunsHarnessOverrides {
    transform_service_mock: Option<MockTransformService>,
    polling_service_mock: Option<MockPollingIngestService>,
}

struct FlowConfigHarness {
    _tempdir: tempfile::TempDir,
    _catalog_base: dill::Catalog,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl FlowConfigHarness {
    async fn with_overrides(overrides: FlowRunsHarnessOverrides) -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let transform_service_mock = overrides.transform_service_mock.unwrap_or_default();
        let polling_service_mock = overrides.polling_service_mock.unwrap_or_default();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<DummyOutboxImpl>()
                .add_builder(
                    DatasetRepositoryLocalFs::builder()
                        .with_root(datasets_dir)
                        .with_multi_tenant(false),
                )
                .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
                .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<SystemTimeSourceDefault>()
                .add_value(polling_service_mock)
                .bind::<dyn PollingIngestService, MockPollingIngestService>()
                .add_value(transform_service_mock)
                .bind::<dyn TransformService, MockTransformService>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
                .add::<DependencyGraphServiceInMemory>()
                .add::<FlowConfigurationServiceImpl>()
                .add::<InMemoryFlowConfigurationEventStore>()
                .add::<DatabaseTransactionRunner>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        // Init dataset with no sources
        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&catalog_base).await;

        Self {
            _tempdir: tempdir,
            _catalog_base: catalog_base,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    async fn create_root_dataset(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(DatasetKind::Root)
                    .name("foo")
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    async fn create_derived_dataset(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name("bar")
                    .kind(DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(["foo"])
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    fn extract_time_delta_from_response(response_json: &serde_json::Value) -> (u64, &str) {
        let schedule_json = &response_json["datasets"]["byId"]["flows"]["configs"]
            ["setConfigIngest"]["config"]["ingest"]["schedule"];

        (
            schedule_json["every"].as_u64().unwrap(),
            schedule_json["unit"].as_str().unwrap(),
        )
    }

    fn set_ingest_config_time_delta_mutation(
        id: &DatasetID,
        dataset_flow_type: &str,
        paused: bool,
        every: u64,
        unit: &str,
        fetch_uncacheable: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setConfigIngest (
                                    datasetFlowType: "<dataset_flow_type>",
                                    paused: <paused>,
                                    ingest: {
                                        fetchUncacheable: <fetch_uncacheable>,
                                        schedule: {
                                            timeDelta: { every: <every>, unit: "<unit>" }
                                        }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        config {
                                            __typename
                                            paused
                                            ingest {
                                                fetchUncacheable,
                                                schedule {
                                                    __typename
                                                    ... on TimeDelta {
                                                        every
                                                        unit
                                                    }
                                                }
                                            }
                                            transform {
                                                __typename
                                            }
                                            compaction {
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
        .replace(
            "<fetch_uncacheable>",
            if fetch_uncacheable { "true" } else { "false" },
        )
        .replace("<every>", every.to_string().as_str())
        .replace("<unit>", unit)
    }

    fn set_ingest_config_cron_expression_mutation(
        id: &DatasetID,
        dataset_flow_type: &str,
        paused: bool,
        cron_expression: &str,
        fetch_uncacheable: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setConfigIngest (
                                    datasetFlowType: "<dataset_flow_type>",
                                    paused: <paused>,
                                    ingest: {
                                        fetchUncacheable: <fetch_uncacheable>,
                                        schedule: {
                                            cron5ComponentExpression: "<cron_expression>"
                                        }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        config {
                                            __typename,
                                            paused
                                            ingest {
                                                fetchUncacheable,
                                                schedule {
                                                    __typename
                                                    ... on Cron5ComponentExpression {
                                                        cron5ComponentExpression
                                                    }
                                                }
                                            }
                                            transform {
                                                __typename
                                            }
                                            compaction {
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
        .replace(
            "<fetch_uncacheable>",
            if fetch_uncacheable { "true" } else { "false" },
        )
        .replace("<cron_expression>", cron_expression)
    }

    fn set_config_transform_mutation(
        id: &DatasetID,
        dataset_flow_type: &str,
        paused: bool,
        min_records_to_await: u64,
        max_batching_interval: (u32, &str),
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setConfigTransform (
                                    datasetFlowType: "<dataset_flow_type>",
                                    paused: <paused>,
                                    transform: {
                                        minRecordsToAwait: <minRecordsToAwait>,
                                        maxBatchingInterval: { every: <every>, unit: "<unit>" }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowConfigSuccess {
                                            config {
                                                __typename
                                                paused
                                                ingest {
                                                    __typename
                                                }
                                                transform {
                                                    __typename
                                                    minRecordsToAwait
                                                    maxBatchingInterval {
                                                        every
                                                        unit
                                                    }
                                                }
                                                compaction {
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
            }
            "#
        )
        .replace("<id>", &id.to_string())
        .replace("<dataset_flow_type>", dataset_flow_type)
        .replace("<paused>", if paused { "true" } else { "false" })
        .replace("<every>", &max_batching_interval.0.to_string())
        .replace("<unit>", max_batching_interval.1)
        .replace("<minRecordsToAwait>", &min_records_to_await.to_string())
    }

    fn set_config_compaction_full_mutation(
        id: &DatasetID,
        dataset_flow_type: &str,
        max_slice_size: u64,
        max_slice_records: u64,
        recursive: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setConfigCompaction (
                                    datasetFlowType: "<dataset_flow_type>",
                                    compactionArgs: {
                                        full: {
                                            maxSliceSize: <max_slice_size>,
                                            maxSliceRecords: <max_slice_records>,
                                            recursive: <recursive>
                                        }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowConfigSuccess {
                                            config {
                                                __typename
                                                paused
                                                ingest {
                                                    __typename
                                                }
                                                transform {
                                                    __typename
                                                }
                                                compaction {
                                                    __typename
                                                    ... on CompactionFull {
                                                        maxSliceSize
                                                        maxSliceRecords
                                                        recursive
                                                    }
                                                    ... on CompactionMetadataOnly {
                                                        recursive
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

    fn set_config_compaction_metadata_only_mutation(
        id: &DatasetID,
        dataset_flow_type: &str,
        recursive: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setConfigCompaction (
                                    datasetFlowType: "<dataset_flow_type>",
                                    compactionArgs: {
                                        metadataOnly: {
                                            recursive: <recursive>
                                        }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowConfigSuccess {
                                            config {
                                                __typename
                                                paused
                                                ingest {
                                                    __typename
                                                }
                                                transform {
                                                    __typename
                                                }
                                                compaction {
                                                    __typename
                                                    ... on CompactionFull {
                                                        maxSliceSize
                                                        maxSliceRecords
                                                        recursive
                                                    }
                                                    ... on CompactionMetadataOnly {
                                                        recursive
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
        .replace("<recursive>", if recursive { "true" } else { "false" })
    }

    fn quick_flow_config_query(id: &DatasetID, dataset_flow_type: &str) -> String {
        indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                byType (datasetFlowType: "<dataset_flow_type>") {
                                    __typename
                                    paused
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

    fn pause_flows_of_type_mutation(id: &DatasetID, dataset_flow_type: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                pauseFlows (
                                    datasetFlowType: "<dataset_flow_type>",
                                )
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

    fn resume_flows_of_type_mutation(id: &DatasetID, dataset_flow_type: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                resumeFlows (
                                    datasetFlowType: "<dataset_flow_type>",
                                )
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

    fn pause_all_flows_mutation(id: &DatasetID) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                pauseFlows
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<id>", &id.to_string())
    }

    fn resume_all_flows_mutation(id: &DatasetID) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                resumeFlows
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<id>", &id.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
