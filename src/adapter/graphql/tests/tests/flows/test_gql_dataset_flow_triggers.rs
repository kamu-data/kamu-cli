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
use kamu_core::TenancyConfig;
use kamu_flow_system_services::FlowTriggerServiceImpl;
use pretty_assertions::assert_eq;

use crate::utils::{
    BaseGQLDatasetHarness,
    BaseGQLFlowHarness,
    GraphQLQueryRequest,
    expect_anonymous_access_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_time_delta_root_dataset() {
    let harness = FlowTriggerHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = FlowTriggerHarness::query_flow_trigger(&create_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let response = harness
        .set_time_delta_trigger(
            &create_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let response = harness
        .set_time_delta_trigger(
            &create_result.dataset_handle.id,
            "INGEST",
            (2, "HOURS"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "TimeDelta",
                                        "every": 2,
                                        "unit": "HOURS"
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_time_delta_validation() {
    let harness = FlowTriggerHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    // These cases exceed unit boundary, but must return the same "every" & "unit"
    for test_case in [
        (63, "MINUTES"),
        (30, "HOURS"),
        (8, "DAYS"),
        (169, "DAYS"),
        (169, "WEEKS"),
    ] {
        let response = harness
            .set_time_delta_trigger(
                &create_result.dataset_handle.id,
                "INGEST",
                (test_case.0, test_case.1),
                None,
            )
            .execute(&schema, &harness.catalog_authorized)
            .await;

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
        let response = harness
            .set_time_delta_trigger(
                &create_result.dataset_handle.id,
                "INGEST",
                (test_case.0, test_case.1),
                None,
            )
            .execute(&schema, &harness.catalog_authorized)
            .await;

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

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = FlowTriggerHarness::query_flow_trigger(&create_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let response = harness
        .set_cron_trigger(
            &create_result.dataset_handle.id,
            "INGEST",
            "*/2 * * * *",
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "Cron5ComponentExpression",
                                        "cron5ComponentExpression": "*/2 * * * *",
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

    let response = harness
        .set_cron_trigger(
            &create_result.dataset_handle.id,
            "INGEST",
            "0 */1 * * *",
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "Cron5ComponentExpression",
                                        "cron5ComponentExpression": "0 */1 * * *",
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

    // Try to pass invalid cron expression
    let invalid_cron_expression = "0 0 */1 *";

    let response = harness
        .set_cron_trigger(
            &create_result.dataset_handle.id,
            "INGEST",
            invalid_cron_expression,
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    assert_eq!(
        response.data,
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
    let response = harness
        .set_cron_trigger(
            &create_result.dataset_handle.id,
            "INGEST",
            past_cron_expression,
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    assert_eq!(
        response.data,
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

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = FlowTriggerHarness::query_flow_trigger(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

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

    let response = harness
        .set_reactive_trigger_buffering(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            1,
            (30, "MINUTES"),
            false,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = FlowTriggerHarness::query_flow_trigger(
        &create_derived_result.dataset_handle.id,
        "EXECUTE_TRANSFORM",
    )
    .execute(&schema, &harness.catalog_authorized)
    .await;

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

    let response = harness
        .set_reactive_trigger_immediate(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            false,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

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
        let response = harness
            .set_reactive_trigger_buffering(
                &create_derived_result.dataset_handle.id,
                "EXECUTE_TRANSFORM",
                test_case.0,
                (test_case.1, test_case.2),
                false,
            )
            .execute(&schema, &harness.catalog_authorized)
            .await;

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
        let response = FlowTriggerHarness::quick_flow_trigger_query(dataset_id, dataset_flow_type)
            .execute(schema, &harness.catalog_authorized)
            .await;

        assert_eq!(
            response.data,
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

    // Setup initial flow configs for datasets

    let harness = FlowTriggerHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    let schema = kamu_adapter_graphql::schema_quiet();

    harness
        .set_time_delta_trigger(
            &create_root_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    harness
        .set_reactive_trigger_buffering(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            1,
            (30, "MINUTES"),
            true,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

    let flow_cases = [
        (&create_root_result.dataset_handle.id, "INGEST"),
        (
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
        ),
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

    let _response = harness
        .pause_flow_trigger(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    // Pause all the root
    let _response = harness
        .pause_all_flow_triggers(&create_root_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    // Resume ingestion
    let _response = harness
        .resume_flow_trigger(&create_root_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    // Pause derived transform
    let _response = harness
        .pause_flow_trigger(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    // Resume all derived
    let _response = harness
        .resume_all_flow_triggers(&create_derived_result.dataset_handle.id)
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_conditions_not_met_for_flows() {
    let harness = FlowTriggerHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset_no_source(foo_alias).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness.create_derived_dataset_no_transform(bar_alias).await;

    ////

    let schema = kamu_adapter_graphql::schema_quiet();

    let response = harness
        .set_reactive_trigger_buffering(
            &create_derived_result.dataset_handle.id,
            "EXECUTE_TRANSFORM",
            1,
            (30, "MINUTES"),
            true,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let response = harness
        .set_cron_trigger(
            &create_root_result.dataset_handle.id,
            "INGEST",
            "0 */2 * * *",
            None,
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    //// Set ingest trigger stop policy with non-default consecutive failures

    let response = harness
        .set_time_delta_trigger(
            &create_root_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            Some(value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 3
                    }
                }
            )),
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
    let response = harness
        .set_time_delta_trigger(
            &create_root_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            Some(value!( {
                "never": { "dummy": true }
            })),
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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
                                    "paused": false,
                                    "schedule": {
                                        "__typename": "TimeDelta",
                                        "every": 1,
                                        "unit": "DAYS"
                                    },
                                    "reactive": null,
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

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias).await;

    //// Set ingest trigger stop policy with incorrect consecutive failures

    let schema = kamu_adapter_graphql::schema_quiet();

    let response = harness
        .set_time_delta_trigger(
            &create_root_result.dataset_handle.id,
            "INGEST",
            (1, "DAYS"),
            Some(value!(
                {
                    "afterConsecutiveFailures": {
                        "maxFailures": 0
                    }
                }
            )),
        )
        .execute(&schema, &harness.catalog_authorized)
        .await;

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

    let schema = kamu_adapter_graphql::schema_quiet();

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias).await;

    let response = harness
        .set_time_delta_trigger(
            &create_root_result.dataset_handle.id,
            "INGEST",
            (5, "MINUTES"),
            None,
        )
        .expect_error()
        .execute(&schema, &harness.catalog_anonymous)
        .await;

    expect_anonymous_access_error(response);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLFlowHarness, base_gql_flow_harness)]
struct FlowTriggerHarness {
    base_gql_flow_harness: BaseGQLFlowHarness,
}

impl FlowTriggerHarness {
    async fn make() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(base_gql_harness.catalog());

        let triggers_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(&base_gql_flow_catalog);
            b.add::<FlowTriggerServiceImpl>();
            b.build()
        };

        let base_gql_flow_harness =
            BaseGQLFlowHarness::new(base_gql_harness, triggers_catalog).await;

        Self {
            base_gql_flow_harness,
        }
    }

    fn extract_time_delta_from_response(response_json: &serde_json::Value) -> (u64, &str) {
        let schedule_json = &response_json["datasets"]["byId"]["flows"]["triggers"]["setTrigger"]
            ["trigger"]["schedule"];

        (
            schedule_json["every"].as_u64().unwrap(),
            schedule_json["unit"].as_str().unwrap(),
        )
    }

    fn quick_flow_trigger_query(
        id: &odf::DatasetID,
        dataset_flow_type: &str,
    ) -> GraphQLQueryRequest {
        let query_code = indoc!(
            r#"
            query($datasetId: DatasetID!, $flowType: DatasetFlowType!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                byType (datasetFlowType: $flowType) {
                                    __typename
                                    paused
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
                "flowType": dataset_flow_type,
            })),
        )
    }

    fn query_flow_trigger(
        dataset_id: &odf::DatasetID,
        dataset_flow_type: &str,
    ) -> GraphQLQueryRequest {
        let query_code = indoc!(
            r#"
            query($datasetId: DatasetID!, $datasetFlowType: DatasetFlowType!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            triggers {
                                byType (datasetFlowType: $datasetFlowType) {
                                    __typename
                                    paused
                                    schedule {
                                        __typename
                                        ... on TimeDelta {
                                            every
                                            unit
                                        }
                                        ... on Cron5ComponentExpression {
                                            cron5ComponentExpression
                                        }
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
        );

        GraphQLQueryRequest::new(
            query_code,
            async_graphql::Variables::from_value(value!({
                "datasetId": dataset_id.to_string(),
                "datasetFlowType": dataset_flow_type,
            })),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
