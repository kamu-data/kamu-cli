// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use database_common::NoOpDatabasePlugin;
use dill::*;
use indoc::indoc;
use kamu::*;
use kamu_accounts::testing::MockAuthenticationService;
use kamu_accounts::{AuthenticationService, DEFAULT_ACCOUNT_NAME};
use kamu_core::*;
use kamu_datasets::{CreateDatasetFromSnapshotUseCase, CreateDatasetResult};
use kamu_datasets_inmem::{InMemoryDatasetDependencyRepository, InMemoryDatasetEntryRepository};
use kamu_datasets_services::{
    CreateDatasetFromSnapshotUseCaseImpl,
    CreateDatasetUseCaseImpl,
    DatasetEntryServiceImpl,
    DependencyGraphServiceImpl,
    ViewDatasetUseCaseImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceDefault;

use crate::utils::authentication_catalogs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_push_sources() {
    let harness = DatasetMetadataHarness::new().await;
    let create_result = harness.create_root_dataset().await;

    let request_code = indoc!(
        r#"
        {
            datasets {
                byId (datasetId: "<id>") {
                    metadata {
                        currentPushSources {
                            sourceName
                            read {
                                __typename
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
        .execute(Request::new(request_code.clone()).data(harness.catalog_authorized.clone()))
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentPushSources": []
                    }
                }
            }
        })
    );

    // Add two push sources
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source1")
                .read(odf::metadata::ReadStepCsv {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("source2")
                .read(odf::metadata::ReadStepNdJson {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = schema
        .execute(Request::new(request_code.clone()).data(harness.catalog_authorized))
        .await;
    assert!(res.is_ok(), "{res:?}");

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentPushSources": [{
                            "sourceName": "source1",
                            "read": {
                                "__typename": "ReadStepCsv",
                            }
                        }, {
                            "sourceName": "source2",
                            "read": {
                                "__typename": "ReadStepNdJson",
                            }
                         }]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_set_transform() {
    let harness = DatasetMetadataHarness::new().await;

    let create_root_result = harness.create_root_dataset().await;
    let create_derived_result = harness.create_derived_dataset().await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(
                DatasetMetadataHarness::get_dataset_set_transform_metadata(
                    create_derived_result.dataset_handle.id.to_string().as_str(),
                ),
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentTransform": {
                            "inputs": [{
                                "datasetRef": create_root_result.dataset_handle.id.to_string(),
                                "alias": create_root_result.dataset_handle.alias.to_string(),
                            }],
                        }
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetMetadataHarness {
    _tempdir: tempfile::TempDir,
    _catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
}

impl DatasetMetadataHarness {
    async fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let catalog_base = {
            let mut b = dill::CatalogBuilder::new();

            b.add::<SystemTimeSourceDefault>()
                .add::<DidGeneratorDefault>()
                .add::<DummyOutboxImpl>()
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<ViewDatasetUseCaseImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add::<MetadataQueryServiceImpl>()
                .add::<EngineProvisionerNull>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<DataFormatRegistryImpl>()
                .add_value(TenancyConfig::MultiTenant)
                .add_builder(
                    odf::dataset::DatasetStorageUnitLocalFs::builder().with_root(datasets_dir),
                )
                .bind::<dyn odf::DatasetStorageUnit, odf::dataset::DatasetStorageUnitLocalFs>()
                .bind::<dyn odf::DatasetStorageUnitWriter, odf::dataset::DatasetStorageUnitLocalFs>(
                )
                .add_value(MockAuthenticationService::built_in())
                .bind::<dyn AuthenticationService, MockAuthenticationService>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
                .add::<DependencyGraphServiceImpl>()
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        let (catalog_anonymous, catalog_authorized) = authentication_catalogs(&catalog_base).await;

        Self {
            _tempdir: tempdir,
            _catalog_anonymous: catalog_anonymous,
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
                    .name(odf::DatasetAlias::new(
                        Some(DEFAULT_ACCOUNT_NAME.clone()),
                        odf::DatasetName::new_unchecked("foo"),
                    ))
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
                    .name(odf::DatasetAlias::new(
                        Some(DEFAULT_ACCOUNT_NAME.clone()),
                        odf::DatasetName::new_unchecked("bar"),
                    ))
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs([odf::DatasetAlias::new(
                                Some(DEFAULT_ACCOUNT_NAME.clone()),
                                odf::DatasetName::new_unchecked("foo"),
                            )])
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    fn get_dataset_set_transform_metadata(dataset_id: &str) -> String {
        indoc!(
            r#"
            query Datasets {
                datasets {
                    byId(
                        datasetId: "<dataset_id>"
                    ) {
                        metadata {
                            currentTransform {
                                inputs {
                                    datasetRef
                                    alias
                                }
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<dataset_id>", dataset_id)
    }
}
