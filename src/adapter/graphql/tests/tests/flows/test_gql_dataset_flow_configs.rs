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
use pretty_assertions::assert_eq;

use crate::utils::{
    BaseGQLDatasetHarness,
    BaseGQLFlowHarness,
    GraphQLQueryRequest,
    expect_anonymous_access_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_ingest_root_dataset() {
    let harness = FlowConfigHarness::make().await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let create_result = harness.create_root_dataset(foo_alias).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = harness
        .query_flow_config(&create_result.dataset_handle.id, "INGEST")
        .execute(&schema, &harness.catalog_authorized)
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

    let res = harness
        .set_ingest_config(&create_result.dataset_handle.id, false, false, None)
        .execute(&schema, &harness.catalog_authorized)
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
                                        "fetchUncacheable": false,
                                        "fetchNextIteration": false
                                    },
                                    "retryPolicy": null
                                }
                            }
                        }
                    }
                }
            }
        })
    );

    let res = harness
        .set_ingest_config(&create_result.dataset_handle.id, true, true, None)
        .execute(&schema, &harness.catalog_authorized)
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
                                        "fetchUncacheable": true,
                                        "fetchNextIteration": true
                                    },
                                    "retryPolicy": null
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

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = harness
        .query_flow_config(&create_result.dataset_handle.id, "HARD_COMPACTION")
        .execute(&schema, &harness.catalog_authorized)
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

    let res = harness
        .set_compaction_config(&create_result.dataset_handle.id, 1_000_000, 10000)
        .execute(&schema, &harness.catalog_authorized)
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
        let response = harness
            .set_compaction_config(
                &create_root_result.dataset_handle.id,
                test_case.0,
                test_case.1,
            )
            .execute(&schema, &harness.catalog_authorized)
            .await;
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

    let res = harness
        .set_ingest_config(&create_derived_result.dataset_handle.id, false, false, None)
        .execute(&schema, &harness.catalog_authorized)
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

    let res = harness
        .set_compaction_config(&create_derived_result.dataset_handle.id, 1000, 1000)
        .execute(&schema, &harness.catalog_authorized)
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

    let schema = kamu_adapter_graphql::schema_quiet();

    let res = harness
        .set_ingest_config(&create_root_result.dataset_handle.id, false, false, None)
        .expect_error()
        .execute(&schema, &harness.catalog_anonymous)
        .await;

    expect_anonymous_access_error(res);
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
            BaseGQLFlowHarness::make_base_gql_flow_catalog(base_gql_harness.catalog());

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

    fn query_flow_config(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_flow_type: &str,
    ) -> GraphQLQueryRequest {
        let query_code = indoc!(
            r#"
            query($datasetId: DatasetID!, $datasetFlowType: String!) {
                datasets {
                    byId (datasetId: $datasetId) {
                        flows {
                            configs {
                                byType (datasetFlowType: $datasetFlowType) {
                                    __typename
                                    rule {
                                        __typename
                                        ... on FlowConfigRuleIngest {
                                            fetchUncacheable
                                            fetchNextIteration
                                        }
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
