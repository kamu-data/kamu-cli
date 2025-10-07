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
use kamu_flow_system_services::FlowConfigurationServiceImpl;

use crate::utils::{BaseGQLDatasetHarness, BaseGQLFlowHarness, expect_anonymous_access_error};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_ingest_root_dataset() {
    let harness = FlowConfigHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        configs {
                            byType (datasetFlowType: "INGEST") {
                                __typename
                                rule {
                                    __typename
                                    ... on FlowConfigRuleIngest {
                                        fetchUncacheable
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

    let mutation_code =
        FlowConfigHarness::set_ingest_config_mutation(&create_result.dataset_handle.id, false);

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
                            "setIngestConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "rule": {
                                        "__typename": "FlowConfigRuleIngest",
                                        "fetchUncacheable": false
                                    },
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let mutation_code =
        FlowConfigHarness::set_ingest_config_mutation(&create_result.dataset_handle.id, true);

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
                            "setIngestConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "rule": {
                                        "__typename": "FlowConfigRuleIngest",
                                        "fetchUncacheable": true
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
async fn test_crud_compaction_root_dataset() {
    let harness = FlowConfigHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    flows {
                        configs {
                            byType (datasetFlowType: "HARD_COMPACTION") {
                                __typename
                                rule {
                                    __typename
                                    ... on FlowConfigRuleCompaction {
                                        __typename
                                        maxSliceSize
                                        maxSliceRecords
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

    let mutation_code = FlowConfigHarness::set_config_compaction_mutation(
        &create_result.dataset_handle.id,
        1_000_000,
        10000,
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
                            "setCompactionConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "rule": {
                                        "__typename": "FlowConfigRuleCompaction",
                                        "maxSliceSize": 1_000_000,
                                        "maxSliceRecords": 10000,
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
async fn test_compaction_config_validation() {
    let harness = FlowConfigHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();

    for test_case in [
        (0, 1_000_000, "Maximum slice size must be a positive number"),
        (
            1_000_000,
            0,
            "Maximum slice records must be a positive number",
        ),
    ] {
        let mutation_code = FlowConfigHarness::set_config_compaction_mutation(
            &create_root_result.dataset_handle.id,
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
        assert_eq!(
            response.data,
            value!({
                    "datasets": {
                        "byId": {
                            "flows": {
                                "configs": {
                                    "setCompactionConfig": {
                                        "__typename": "FlowInvalidConfigInputError",
                                        "message": test_case.2,
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
async fn test_incorrect_dataset_kinds_for_flow_type() {
    let harness = FlowConfigHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    harness.create_root_dataset(foo_alias.clone()).await;

    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let create_derived_result = harness
        .create_derived_dataset(bar_alias, &[foo_alias])
        .await;

    ////

    let schema = kamu_adapter_graphql::schema_quiet();

    let mutation_code = FlowConfigHarness::set_ingest_config_mutation(
        &create_derived_result.dataset_handle.id,
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
                            "setIngestConfig": {
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

    let mutation_code = FlowConfigHarness::set_config_compaction_mutation(
        &create_derived_result.dataset_handle.id,
        1000,
        1000,
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
                            "setCompactionConfig": {
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
async fn test_anonymous_setters_fail() {
    let harness = FlowConfigHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_root_result = harness.create_root_dataset(foo_alias).await;

    let mutation_codes = [FlowConfigHarness::set_ingest_config_mutation(
        &create_root_result.dataset_handle.id,
        false,
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

#[oop::extend(BaseGQLFlowHarness, base_gql_flow_harness)]
struct FlowConfigHarness {
    base_gql_flow_harness: BaseGQLFlowHarness,
}

impl FlowConfigHarness {
    async fn make() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let base_gql_flow_catalog =
            BaseGQLFlowHarness::make_base_gql_flow_catalog(&base_gql_harness);

        let configs_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(&base_gql_flow_catalog);
            b.add::<FlowConfigurationServiceImpl>();
            b.build()
        };

        let base_gql_flow_harness =
            BaseGQLFlowHarness::new(base_gql_harness, configs_catalog).await;

        Self {
            base_gql_flow_harness,
        }
    }

    fn set_ingest_config_mutation(id: &odf::DatasetID, fetch_uncacheable: bool) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setIngestConfig (
                                    ingestConfigInput : {
                                        fetchUncacheable: <fetch_uncacheable>,
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        config {
                                            __typename
                                            rule {
                                                __typename
                                                ... on FlowConfigRuleIngest {
                                                    fetchUncacheable
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
        .replace(
            "<fetch_uncacheable>",
            if fetch_uncacheable { "true" } else { "false" },
        )
    }

    fn set_config_compaction_mutation(
        id: &odf::DatasetID,
        max_slice_size: u64,
        max_slice_records: u64,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setCompactionConfig (
                                    compactionConfigInput: {
                                        maxSliceSize: <max_slice_size>,
                                        maxSliceRecords: <max_slice_records>,
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
                                                rule {
                                                    __typename
                                                    ... on FlowConfigRuleCompaction {
                                                        maxSliceSize
                                                        maxSliceRecords
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
        .replace("<max_slice_records>", &max_slice_records.to_string())
        .replace("<max_slice_size>", &max_slice_size.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
