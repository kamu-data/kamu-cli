// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use dill::*;
use indoc::indoc;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use opendatafabric::*;

use crate::utils::authentication_catalogs;

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_current_push_sources() {
    let tempdir = tempfile::tempdir().unwrap();

    let base_catalog = CatalogBuilder::new()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(tempdir.path().join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add_builder(PushIngestServiceImpl::builder().with_run_info_dir(tempdir.path().join("run")))
        .bind::<dyn PushIngestService, PushIngestServiceImpl>()
        .add::<SystemTimeSourceDefault>()
        .bind::<dyn SystemTimeSource, SystemTimeSourceDefault>()
        .add::<ObjectStoreRegistryImpl>()
        .bind::<dyn ObjectStoreRegistry, ObjectStoreRegistryImpl>()
        .add::<DataFormatRegistryImpl>()
        .bind::<dyn DataFormatRegistry, DataFormatRegistryImpl>()
        .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
        .bind::<dyn auth::DatasetActionAuthorizer, auth::AlwaysHappyDatasetActionAuthorizer>()
        .build();

    // Init dataset with no sources
    let (_, catalog_authorized) = authentication_catalogs(&base_catalog);
    let dataset_repo = catalog_authorized
        .get_one::<dyn DatasetRepository>()
        .unwrap();
    let create_result = dataset_repo
        .create_dataset_from_snapshot(
            None,
            MetadataFactory::dataset_snapshot()
                .kind(DatasetKind::Root)
                .name("foo")
                .build(),
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
        .execute(async_graphql::Request::new(request_code.clone()).data(catalog_authorized.clone()))
        .await;
    assert!(res.is_ok(), "{:?}", res);

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
                .read(ReadStepCsv {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();
    create_result
        .dataset
        .commit_event(
            MetadataFactory::add_push_source()
                .source_name("device1")
                .read(ReadStepNdJson {
                    schema: Some(vec!["foo STRING".to_string()]),
                    ..Default::default()
                })
                .build()
                .into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let res = schema
        .execute(async_graphql::Request::new(request_code.clone()).data(catalog_authorized))
        .await;
    assert!(res.is_ok(), "{:?}", res);

    assert_eq!(
        res.data,
        value!({
            "datasets": {
                "byId": {
                    "metadata": {
                        "currentPushSources": [{
                            "sourceName": "device1",
                            "read": {
                                "__typename": "ReadStepNdJson",
                            }
                         }, {
                            "sourceName": null,
                            "read": {
                                "__typename": "ReadStepCsv",
                            }
                         }]
                    }
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////
