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
use kamu::{DatasetStorageUnitLocalFs, MetadataQueryServiceImpl};
use kamu_accounts::JwtAuthenticationConfig;
use kamu_accounts_inmem::InMemoryAccessTokenRepository;
use kamu_accounts_services::{AccessTokenServiceImpl, AuthenticationServiceImpl};
use kamu_core::{auth, DidGeneratorDefault, TenancyConfig};
use kamu_datasets::CreateDatasetFromSnapshotUseCase;
use kamu_datasets_inmem::{InMemoryDatasetDependencyRepository, InMemoryDatasetEntryRepository};
use kamu_datasets_services::{
    CreateDatasetFromSnapshotUseCaseImpl,
    CreateDatasetUseCaseImpl,
    DatasetEntryServiceImpl,
    DependencyGraphServiceImpl,
    ViewDatasetUseCaseImpl,
};
use kamu_flow_system_inmem::InMemoryFlowConfigurationEventStore;
use kamu_flow_system_services::FlowConfigurationServiceImpl;
use messaging_outbox::DummyOutboxImpl;
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceDefault;

use crate::utils::{authentication_catalogs, expect_anonymous_access_error};

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
                                ingest {
                                    fetchUncacheable
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

    let mutation_code = FlowConfigHarness::set_ingest_config_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
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
                            "setConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "ingest": {
                                        "fetchUncacheable": false,
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

    let mutation_code = FlowConfigHarness::set_ingest_config_mutation(
        &create_result.dataset_handle.id,
        "INGEST",
        true,
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
                            "setConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "ingest": {
                                        "fetchUncacheable": true,
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
                                ingest {
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
                            "setConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "ingest": null,
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
                                    "setConfig": {
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

    let expected_error = value!({
        "datasets": {
            "byId": {
                "flows": {
                    "configs": {
                        "setConfig": {
                            "__typename": "FlowIncompatibleDatasetKind",
                            "message": "Expected a Root dataset, but a Derivative dataset was provided",
                        }
                    }
                }
            }
        }
    });
    ////

    let schema = kamu_adapter_graphql::schema_quiet();

    let mutation_code = FlowConfigHarness::set_ingest_config_mutation(
        &create_derived_result.dataset_handle.id,
        "INGEST",
        false,
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(res.data, expected_error);

    ////

    let mutation_code = FlowConfigHarness::set_ingest_config_mutation(
        &create_derived_result.dataset_handle.id,
        "INGEST",
        false,
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(res.data, expected_error);

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
    assert_eq!(res.data, expected_error);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_metadataonly_compaction_config_for_derivative() {
    let harness = FlowConfigHarness::make().await;

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
                            "setConfig": {
                                "__typename": "SetFlowConfigSuccess",
                                "message": "Success",
                                "config": {
                                    "__typename": "FlowConfiguration",
                                    "ingest": null,
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
    let harness = FlowConfigHarness::make().await;
    let create_root_result = harness.create_root_dataset().await;
    let expected_error = value!({
        "datasets": {
            "byId": {
                "flows": {
                    "configs": {
                        "setConfig": {
                            "__typename": "FlowTypeIsNotSupported",
                            "message": "Flow type is not supported",
                        }
                    }
                }
            }
        }
    });

    ////

    let mutation_code = FlowConfigHarness::set_ingest_config_mutation(
        &create_root_result.dataset_handle.id,
        "HARD_COMPACTION",
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
    assert_eq!(response.data, expected_error);

    ////

    let mutation_code = FlowConfigHarness::set_config_compaction_full_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
        10,
        20,
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
    assert_eq!(response.data, expected_error);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_anonymous_setters_fail() {
    let harness = FlowConfigHarness::make().await;
    let create_root_result = harness.create_root_dataset().await;

    let mutation_codes = [FlowConfigHarness::set_ingest_config_mutation(
        &create_root_result.dataset_handle.id,
        "INGEST",
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

struct FlowConfigHarness {
    _tempdir: tempfile::TempDir,
    _catalog_base: dill::Catalog,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl FlowConfigHarness {
    async fn make() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<DummyOutboxImpl>()
                .add::<DidGeneratorDefault>()
                .add_value(TenancyConfig::SingleTenant)
                .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
                .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
                .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
                .add::<MetadataQueryServiceImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<ViewDatasetUseCaseImpl>()
                .add::<SystemTimeSourceDefault>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add::<FlowConfigurationServiceImpl>()
                .add::<InMemoryFlowConfigurationEventStore>()
                .add::<DatabaseTransactionRunner>()
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
                .add::<AuthenticationServiceImpl>()
                .add::<AccessTokenServiceImpl>()
                .add::<InMemoryAccessTokenRepository>()
                .add_value(JwtAuthenticationConfig::default());

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

    async fn create_root_dataset(&self) -> odf::CreateDatasetResult {
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

    async fn create_derived_dataset(&self) -> odf::CreateDatasetResult {
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

    fn set_ingest_config_mutation(
        id: &odf::DatasetID,
        dataset_flow_type: &str,
        fetch_uncacheable: bool,
    ) -> String {
        indoc!(
            r#"
            mutation {
                datasets {
                    byId (datasetId: "<id>") {
                        flows {
                            configs {
                                setConfig (
                                    datasetFlowType: "<dataset_flow_type>",
                                    configInput : {
                                        ingest: {
                                            fetchUncacheable: <fetch_uncacheable>,
                                        }
                                    }
                                ) {
                                    __typename,
                                    message
                                    ... on SetFlowConfigSuccess {
                                        config {
                                            __typename
                                            ingest {
                                                fetchUncacheable,
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
        .replace(
            "<fetch_uncacheable>",
            if fetch_uncacheable { "true" } else { "false" },
        )
    }

    fn set_config_compaction_full_mutation(
        id: &odf::DatasetID,
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
                                setConfig (
                                    datasetFlowType: "<dataset_flow_type>",
                                    configInput: {
                                        compaction: {
                                            full: {
                                                maxSliceSize: <max_slice_size>,
                                                maxSliceRecords: <max_slice_records>,
                                                recursive: <recursive>
                                            }
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
                                                ingest {
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
        id: &odf::DatasetID,
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
                                setConfig (
                                    datasetFlowType: "<dataset_flow_type>",
                                    configInput: {
                                        compaction: {
                                            metadataOnly: {
                                                recursive: <recursive>
                                            }
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
                                                ingest {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
