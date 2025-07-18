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
use kamu_flow_system_inmem::InMemoryFlowConfigurationEventStore;
use kamu_flow_system_services::FlowConfigurationServiceImpl;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{
    BaseGQLDatasetHarness,
    PredefinedAccountOpts,
    authentication_catalogs,
    expect_anonymous_access_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_crud_ingest_root_dataset() {
    let harness = FlowConfigHarness::make().await;

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
                                rule {
                                    __typename
                                    ... on FlowConfigRuleCompaction {
                                        compactionMode {
                                            __typename
                                            ... on FlowConfigCompactionModeFull {
                                                maxSliceSize
                                                maxSliceRecords
                                                recursive
                                            }
                                            ... on FlowConfigCompactionModeMetadataOnly {
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
                            "setCompactionConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "rule": {
                                        "__typename": "FlowConfigRuleCompaction",
                                        "compactionMode": {
                                            "__typename": "FlowConfigCompactionModeFull",
                                            "maxSliceSize": 1_000_000,
                                            "maxSliceRecords": 10000,
                                            "recursive": false
                                        }
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
    // ToDo#Separate check compaction for derivative
    let create_root_result = harness.create_root_dataset().await;

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
            &create_root_result.dataset_handle.id,
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
                                    "setCompactionConfig": {
                                        "__typename": "FlowInvalidConfigInputError",
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
async fn test_incorrect_dataset_kinds_for_flow_type() {
    let harness = FlowConfigHarness::make().await;

    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

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

    let mutation_code = FlowConfigHarness::set_config_compaction_full_mutation(
        &create_derived_result.dataset_handle.id,
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
async fn test_set_metadataonly_compaction_config_for_derivative() {
    let harness = FlowConfigHarness::make().await;

    harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let mutation_code = FlowConfigHarness::set_config_compaction_metadata_only_mutation(
        &create_derived_result.dataset_handle.id,
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
                            "setCompactionConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "rule": {
                                        "__typename": "FlowConfigRuleCompaction",
                                        "compactionMode": {
                                            "__typename": "FlowConfigCompactionModeMetadataOnly",
                                            "recursive": false
                                        }
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
async fn test_anonymous_setters_fail() {
    let harness = FlowConfigHarness::make().await;
    let create_root_result = harness.create_root_dataset().await;

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

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct FlowConfigHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl FlowConfigHarness {
    async fn make() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add::<MetadataQueryServiceImpl>()
                .add::<FlowConfigurationServiceImpl>()
                .add::<InMemoryFlowConfigurationEventStore>();

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

    fn set_config_compaction_full_mutation(
        id: &odf::DatasetID,
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
                                setCompactionConfig (
                                    compactionConfigInput: {
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
                                                rule {
                                                    __typename
                                                    ... on FlowConfigRuleCompaction {
                                                        compactionMode {
                                                            __typename
                                                            ... on FlowConfigCompactionModeFull {
                                                                maxSliceSize
                                                                maxSliceRecords
                                                                recursive
                                                            }
                                                            ... on FlowConfigCompactionModeMetadataOnly {
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
                }
            }
            "#
        )
        .replace("<id>", &id.to_string())
        .replace("<max_slice_records>", &max_slice_records.to_string())
        .replace("<max_slice_size>", &max_slice_size.to_string())
        .replace("<recursive>", if recursive { "true" } else { "false" })
    }

    fn set_config_compaction_metadata_only_mutation(
        id: &odf::DatasetID,
        recursive: bool,
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
                                                rule {
                                                    __typename
                                                    ... on FlowConfigRuleCompaction {
                                                        compactionMode {
                                                            __typename
                                                            ... on FlowConfigCompactionModeFull {
                                                                maxSliceSize
                                                                maxSliceRecords
                                                                recursive
                                                            }
                                                            ... on FlowConfigCompactionModeMetadataOnly {
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
                }
            }
            "#
        )
        .replace("<id>", &id.to_string())
        .replace("<recursive>", if recursive { "true" } else { "false" })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
