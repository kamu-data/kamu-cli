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
use kamu_core::*;
use kamu_datasets::CreateDatasetFromSnapshotUseCase;
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
    let tempdir = tempfile::tempdir().unwrap();
    let datasets_dir = tempdir.path().join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let base_catalog = {
        let mut b = CatalogBuilder::new();

        b.add_value(RunInfoDir::new(tempdir.path().join("run")))
            .add::<DidGeneratorDefault>()
            .add::<DummyOutboxImpl>()
            .add_value(TenancyConfig::SingleTenant)
            .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
            .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
            .add::<MetadataQueryServiceImpl>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<ViewDatasetUseCaseImpl>()
            .add::<SystemTimeSourceDefault>()
            .add::<EngineProvisionerNull>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<DataFormatRegistryImpl>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<DatasetEntryServiceImpl>()
            .add::<InMemoryDatasetEntryRepository>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        b.build()
    };

    // Init dataset with no sources
    let (_, catalog_authorized) = authentication_catalogs(&base_catalog).await;

    let create_dataset_from_snapshot = catalog_authorized
        .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
        .unwrap();
    let create_result = create_dataset_from_snapshot
        .execute(
            MetadataFactory::dataset_snapshot()
                .kind(odf::DatasetKind::Root)
                .name("foo")
                .build(),
            Default::default(),
        )
        .await
        .unwrap();

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
        .execute(Request::new(request_code.clone()).data(catalog_authorized.clone()))
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
        .execute(Request::new(request_code.clone()).data(catalog_authorized))
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
