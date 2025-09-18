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
use kamu::MetadataQueryServiceImpl;
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::FlowTriggerServiceImpl;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{
    BaseGQLDatasetHarness,
    PredefinedAccountOpts,
    authentication_catalogs,
    expect_anonymous_access_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_time_delta_root_dataset() {
    let harness = FlowTriggerHarness::make().await;

    let create_result = harness.create_root_dataset().await;
    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        triggers {
                            byType (datasetFlowType: "INGEST") {
                                __typename
                                schedule {
                                    __typename
                                    ... on TimeDelta {
                                        every
                                        unit
                                    }
                                }
                                reactive {
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
                        "triggers": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::set_time_delta_trigger_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        1,
        "DAYS",
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
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::set_time_delta_trigger_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        2,
        "HOURS",
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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "TimeDelta",
                                        "every": 2,
                                        "unit": "HOURS"
                                    },
                                    "reactive": null,
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
    let harness = FlowTriggerHarness::make().await;

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
        let mutation_code = FlowTriggerHarness::set_time_delta_trigger_mutation(
            &create_result.dataset_handle.id,
            "INGEST",
            test_case.0,
            test_case.1,
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
            FlowTriggerHarness::extract_time_delta_from_response(&response_json)
        );
    }

    // These cases exceed unit boundary, but can be compacted to higher level unit
    for test_case in [
        (360, "MINUTES", 6, "HOURS"),
        (48, "HOURS", 2, "DAYS"),
        (7, "DAYS", 1, "WEEKS"),
    ] {
        let mutation_code = FlowTriggerHarness::set_time_delta_trigger_mutation(
            &create_result.dataset_handle.id,
            "INGEST",
            test_case.0,
            test_case.1,
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
            FlowTriggerHarness::extract_time_delta_from_response(&response_json)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_cron_root_dataset() {
    let harness = FlowTriggerHarness::make().await;

    let create_result = harness.create_root_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        triggers {
                            byType (datasetFlowType: "INGEST") {
                                __typename
                                paused
                                schedule {
                                    __typename
                                    ... on Cron5ComponentExpression {
                                        cron5ComponentExpression
                                    }
                                }
                                reactive {
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
                        "triggers": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::set_cron_trigger_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        "*/2 * * * *",
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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "Cron5ComponentExpression",
                                        "cron5ComponentExpression": "*/2 * * * *",
                                    },
                                    "reactive": null,
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::set_cron_trigger_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        "0 */1 * * *",
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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "Cron5ComponentExpression",
                                        "cron5ComponentExpression": "0 */1 * * *",
                                    },
                                    "reactive": null,
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
    let mutation_code = FlowTriggerHarness::set_cron_trigger_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        invalid_cron_expression,
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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "FlowInvalidTriggerInputError",
                                "message": format!("Cron expression {invalid_cron_expression} is invalid"),
                            }
                        }
                    }
                }
            }
        })
    );

    // Try to pass valid cron expression with year (not supported)
    let past_cron_expression = "0 0 1 JAN ? 2024";
    let mutation_code = FlowTriggerHarness::set_cron_trigger_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        past_cron_expression,
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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "FlowInvalidTriggerInputError",
                                "message": format!("Cron expression {past_cron_expression} is invalid"),
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
async fn test_crud_reactive_buffering_derived_dataset() {
    let harness = FlowTriggerHarness::make().await;

    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        triggers {
                            byType (datasetFlowType: "EXECUTE_TRANSFORM") {
                                __typename
                                paused
                                schedule {
                                    __typename
                                }
                                reactive {
                                    __typename
                                    forNewData {
                                        __typename
                                        ... on FlowTriggerBatchingRuleBuffering {
                                            minRecordsToAwait
                                            maxBatchingInterval {
                                                every
                                                unit
                                            }
                                        }
                                    }
                                    forBreakingChange
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
                        "triggers": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::set_trigger_reactive_buffering_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        1,
        (30, "MINUTES"),
        "NO_ACTION",
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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "paused": false,
                                    "schedule": null,
                                    "reactive": {
                                        "__typename": "FlowTriggerReactiveRule",
                                        "forNewData": {
                                            "__typename": "FlowTriggerBatchingRuleBuffering",
                                            "minRecordsToAwait": 1,
                                            "maxBatchingInterval": {
                                                "every": 30,
                                                "unit": "MINUTES"
                                            }
                                        },
                                        "forBreakingChange": "NO_ACTION",
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
async fn test_crud_reactive_immediate_derived_dataset() {
    let harness = FlowTriggerHarness::make().await;

    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        triggers {
                            byType (datasetFlowType: "EXECUTE_TRANSFORM") {
                                __typename
                                paused
                                schedule {
                                    __typename
                                }
                                reactive {
                                    __typename
                                    forNewData {
                                        __typename
                                    }
                                    forBreakingChange
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
                        "triggers": {
                            "byType": null
                        }
                    }
                }
            }
        })
    );

    let mutation_code = FlowTriggerHarness::set_trigger_reactive_immediate_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        "NO_ACTION",
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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "paused": false,
                                    "schedule": null,
                                    "reactive": {
                                        "__typename": "FlowTriggerReactiveRule",
                                        "forNewData": {
                                            "__typename": "FlowTriggerBatchingRuleImmediate",
                                        },
                                        "forBreakingChange": "NO_ACTION",
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
async fn test_reactive_buffering_trigger_validation() {
    let harness = FlowTriggerHarness::make().await;

    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();

    for test_case in [
        (
            1,
            25,
            "HOURS",
            "Maximum interval to await should not exceed 24 hours",
        ),
        (
            1,
            0,
            "MINUTES",
            "Minimum interval to await should be positive",
        ),
        (
            0,
            30,
            "MINUTES",
            "Minimum records to await should be positive",
        ),
    ] {
        let mutation_code = FlowTriggerHarness::set_trigger_reactive_buffering_mutation(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            test_case.0,
            (test_case.1, test_case.2),
            "NO_ACTION",
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
                                "triggers": {
                                    "setTrigger": {
                                        "__typename": "FlowInvalidTriggerInputError",
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
        harness: &FlowTriggerHarness,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
        dataset_flow_type: &str,
        expect_paused: bool,
    ) {
        let query = FlowTriggerHarness::quick_flow_trigger_query(dataset_id, dataset_flow_type);

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
                            "triggers": {
                                "byType": {
                                    "__typename": "FlowTrigger",
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
        harness: &FlowTriggerHarness,
        schema: &kamu_adapter_graphql::Schema,
        dataset_id: &odf::DatasetID,
        expect_paused: bool,
    ) {
        let query = FlowTriggerHarness::all_paused_trigger_query(dataset_id);

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
                            "triggers": {
                                "allPaused": expect_paused,
                            }
                        }
                    }
                }
            })
        );
    }

    // Setup initial flow configs for datasets

    let harness = FlowTriggerHarness::make().await;

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();

    let mutation_set_ingest = FlowTriggerHarness::set_time_delta_trigger_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
        1,
        "DAYS",
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_set_ingest)
                .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");

    let mutation_set_transform = FlowTriggerHarness::set_trigger_reactive_buffering_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        1,
        (30, "MINUTES"),
        "RECOVER",
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

    let mutation_pause_root_compaction = FlowTriggerHarness::pause_flow_mutation(
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
        FlowTriggerHarness::pause_all_flows_mutation(&create_root_result.dataset_handle.id);

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
    let mutation_resume_ingest =
        FlowTriggerHarness::resume_flow_mutation(&create_root_result.dataset_handle.id, "INGEST");

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
    let mutation_pause_derived_transform = FlowTriggerHarness::pause_flow_mutation(
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
        FlowTriggerHarness::resume_all_flows_mutation(&create_derived_result.dataset_handle.id);

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
    let harness = FlowTriggerHarness::make().await;

    let create_root_result = harness.create_root_dataset_no_source().await;
    let create_derived_result = harness.create_derived_dataset_no_transform().await;

    ////

    let mutation_code = FlowTriggerHarness::set_trigger_reactive_buffering_mutation(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
        1,
        (30, "MINUTES"),
        "RECOVER",
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
                        "triggers": {
                            "setTrigger": {
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

    let mutation_code = FlowTriggerHarness::set_cron_trigger_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
        "0 */2 * * *",
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
                        "triggers": {
                            "setTrigger": {
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
async fn test_stop_policies() {
    let harness = FlowTriggerHarness::make().await;

    let create_root_result = harness.create_root_dataset().await;

    //// Set ingest trigger stop policy with non-default consecutive failures

    let mutation_code = indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: "INGEST",
                                    triggerRuleInput: {
                                        schedule: {
                                            timeDelta: { every: 1, unit: "DAYS" }
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        afterConsecutiveFailures: { maxFailures: 3 }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        trigger {
                                            __typename
                                            stopPolicy {
                                                __typename
                                                ... on FlowTriggerStopPolicyAfterConsecutiveFailures {
                                                    maxFailures
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
        .replace("<id>", &create_root_result.dataset_handle.id.to_string());

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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "stopPolicy": {
                                        "__typename": "FlowTriggerStopPolicyAfterConsecutiveFailures",
                                        "maxFailures": 3
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    // Set another stop policy - never
    let mutation_code = indoc!(
        r#"
        mutation {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        triggers {
                            setTrigger (
                                datasetFlowType: "INGEST",
                                triggerRuleInput: {
                                    schedule: {
                                        timeDelta: { every: 1, unit: "DAYS" }
                                    }
                                }
                                triggerStopPolicyInput: {
                                    never: { dummy: false }
                                }
                            ) {
                                __typename,
                                message
                                ... on SetFlowTriggerSuccess {
                                    trigger {
                                        __typename
                                        stopPolicy {
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
    .replace("<id>", &create_root_result.dataset_handle.id.to_string());

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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "SetFlowTriggerSuccess",
                                "message": "Success",
                                "trigger": {
                                    "__typename": "FlowTrigger",
                                    "stopPolicy": {
                                        "__typename": "FlowTriggerStopPolicyNever",
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
async fn test_stop_policies_validation() {
    let harness = FlowTriggerHarness::make().await;

    let create_root_result = harness.create_root_dataset().await;

    //// Set ingest trigger stop policy with incorrect consecutive failures

    let mutation_code = indoc!(
        r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
                                setTrigger (
                                    datasetFlowType: "INGEST",
                                    triggerRuleInput: {
                                        schedule: {
                                            timeDelta: { every: 1, unit: "DAYS" }
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        afterConsecutiveFailures: { maxFailures: 0 }
                                    }
                                ) {
                                    __typename,
                                    message
                                }
                            }
                        }
                    }
                }
            }
            "#
    )
    .replace("<id>", &create_root_result.dataset_handle.id.to_string());

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
                        "triggers": {
                            "setTrigger": {
                                "__typename": "FlowInvalidTriggerStopPolicyInputError",
                                "message": "ConsecutiveFailuresCount is too small. The value must be greater or equal to 1.",
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
    let harness = FlowTriggerHarness::make().await;

    let create_root_result = harness.create_root_dataset().await;

    let mutation_codes = [FlowTriggerHarness::set_time_delta_trigger_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
        5,
        "MINUTES",
    )];

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

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct FlowTriggerHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl FlowTriggerHarness {
    async fn make() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<MetadataQueryServiceImpl>()
                .add::<FlowTriggerServiceImpl>()
                .add::<InMemoryFlowEventStore>()
                .add::<InMemoryFlowTriggerEventStore>()
                .add::<InMemoryFlowConfigurationEventStore>()
                .add::<InMemoryFlowSystemEventStore>()
                .add::<InMemoryFlowProcessState>();

            b.build()
        };

        // Init dataset with no sources
        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
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
                    .kind(odf::DatasetKind::Root)
                    .name("foo")
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    async fn create_root_dataset_no_source(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name("foo")
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
                    .kind(odf::DatasetKind::Derivative)
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

    async fn create_derived_dataset_no_transform(&self) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name("bar")
                    .kind(odf::DatasetKind::Derivative)
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    fn extract_time_delta_from_response(response_json: &serde_json::Value) -> (u64, &str) {
        let schedule_json = &response_json["datasets"]["byId"]["flows"]["triggers"]["setTrigger"]
            ["trigger"]["schedule"];

        (
            schedule_json["every"].as_u64().unwrap(),
            schedule_json["unit"].as_str().unwrap(),
        )
    }

    fn set_time_delta_trigger_mutation(
        id: &odf::DatasetID,
        dataset_flow_type: &str,
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
                                    triggerRuleInput: {
                                        schedule: {
                                            timeDelta: { every: <every>, unit: "<unit>" }
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
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
                                            reactive {
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
        .replace("<every>", every.to_string().as_str())
        .replace("<unit>", unit)
    }

    fn set_cron_trigger_mutation(
        id: &odf::DatasetID,
        dataset_flow_type: &str,
        cron_expression: &str,
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
                                    triggerRuleInput: {
                                        schedule: {
                                            cron5ComponentExpression: "<cron_expression>"
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        trigger {
                                            __typename,
                                            paused
                                            schedule {
                                                __typename
                                                ... on Cron5ComponentExpression {
                                                    cron5ComponentExpression
                                                }
                                            }
                                            reactive {
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
        .replace("<cron_expression>", cron_expression)
    }

    fn set_trigger_reactive_buffering_mutation(
        id: &odf::DatasetID,
        dataset_flow_type: &str,
        min_records_to_await: u64,
        max_batching_interval: (u32, &str),
        for_breaking_change: &str,
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
                                    triggerRuleInput: {
                                        reactive: {
                                            forNewData: {
                                                buffering: {
                                                    minRecordsToAwait: <minRecordsToAwait>,
                                                    maxBatchingInterval: { every: <every>, unit: "<unit>" }
                                                }
                                            },
                                            forBreakingChange: "<forBreakingChange>"
                                        }
                                    }
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowTriggerSuccess {
                                            trigger {
                                                __typename
                                                paused
                                                schedule {
                                                    __typename
                                                }
                                                reactive {
                                                    __typename
                                                    forNewData {
                                                        __typename
                                                        ... on FlowTriggerBatchingRuleBuffering {
                                                            minRecordsToAwait
                                                            maxBatchingInterval {
                                                                every
                                                                unit
                                                            }
                                                        }
                                                    }
                                                    forBreakingChange
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
        .replace("<every>", &max_batching_interval.0.to_string())
        .replace("<unit>", max_batching_interval.1)
        .replace("<minRecordsToAwait>", &min_records_to_await.to_string())
        .replace("<forBreakingChange>", for_breaking_change)
    }

    fn set_trigger_reactive_immediate_mutation(
        id: &odf::DatasetID,
        dataset_flow_type: &str,
        for_breaking_change: &str,
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
                                    triggerRuleInput: {
                                        reactive: {
                                            forNewData: {
                                                immediate: {
                                                    dummy: false
                                                }
                                            },
                                            forBreakingChange: "<forBreakingChange>"
                                        }
                                    },
                                    triggerStopPolicyInput: {
                                        never: { dummy: true }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowTriggerSuccess {
                                        __typename,
                                        message
                                        ... on SetFlowTriggerSuccess {
                                            trigger {
                                                __typename
                                                paused
                                                schedule {
                                                    __typename
                                                }
                                                reactive {
                                                    __typename
                                                    forNewData {
                                                        __typename
                                                    }
                                                    forBreakingChange
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
        .replace("<forBreakingChange>", for_breaking_change)
    }

    fn quick_flow_trigger_query(id: &odf::DatasetID, dataset_flow_type: &str) -> String {
        indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
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

    fn pause_flow_mutation(id: &odf::DatasetID, dataset_flow_type: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
                                pauseFlow (
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

    fn resume_flow_mutation(id: &odf::DatasetID, dataset_flow_type: &str) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
                                resumeFlow (
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

    fn pause_all_flows_mutation(id: &odf::DatasetID) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
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

    fn resume_all_flows_mutation(id: &odf::DatasetID) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            triggers {
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
