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
use kamu_core::*;
use kamu_datasets::CreateDatasetFromSnapshotUseCase;
use kamu_datasets_inmem::{InMemoryDatasetDependencyRepository, InMemoryDatasetEntryRepository};
use kamu_datasets_services::{
    CreateDatasetFromSnapshotUseCaseImpl,
    CreateDatasetUseCaseImpl,
    DatasetEntryServiceImpl,
    DependencyGraphServiceImpl,
};
use messaging_outbox::DummyOutboxImpl;
use odf::metadata::testing::MetadataFactory;
use time_source::SystemTimeSourceDefault;

use crate::utils::authentication_catalogs;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_search_query() {
    let harness = GqlSearchHarness::new().await;

    use odf::metadata::testing::alias;

    harness.create_datasets([alias(&"kamu", &"foo")]).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            Request::new(
                "
                {
                    search {
                      query(query: \"bar\") {
                        nodes {
                          __typename
                          ... on Dataset {
                            name
                          }
                        }
                        totalCount
                        pageInfo {
                          totalPages
                          hasNextPage
                          hasPreviousPage
                        }
                      }
                    }
                  }
                ",
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "search": {
                "query": {
                    "nodes": [],
                    "totalCount": 0i32,
                    "pageInfo": {
                        "totalPages": 0i32,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );

    let res = schema
        .execute(
            Request::new(
                "
                {
                    search {
                      query(query: \"foo\") {
                        nodes {
                          __typename
                          ... on Dataset {
                            name
                          }
                        }
                        totalCount
                        pageInfo {
                          totalPages
                          hasNextPage
                          hasPreviousPage
                        }
                      }
                    }
                  }
                ",
            )
            .data(harness.catalog_authorized.clone()),
        )
        .await;
    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "__typename": "Dataset",
                        "name": "foo",
                    }],
                    "totalCount": 1i32,
                    "pageInfo": {
                        "totalPages": 1i32,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );

    let res = schema
        .execute(
            Request::new(
                "
                {
                  search {
                    query(query: \"\") {
                      nodes {
                        ... on Dataset {
                          metadata {
                            chain {
                              blocks(page: 0, perPage: 0) {
                                totalCount
                                pageInfo {
                                  totalPages
                                  hasNextPage
                                  hasPreviousPage
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                ",
            )
            .data(harness.catalog_authorized),
        )
        .await;
    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "metadata": {
                          "chain": {
                            "blocks": {
                              "totalCount": 2i32,
                              "pageInfo": {
                                "totalPages": 0i32,
                                "hasNextPage": false,
                                "hasPreviousPage": false
                              }
                            }
                          }
                        },
                    }],
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GqlSearchHarness {
    _temp_dir: tempfile::TempDir,
    pub catalog_authorized: dill::Catalog,
}

impl GqlSearchHarness {
    pub async fn new() -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let datasets_dir = temp_dir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let base_catalog = {
            use dill::Component;

            let mut b = dill::CatalogBuilder::new();
            b.add::<DidGeneratorDefault>()
                .add::<SystemTimeSourceDefault>()
                .add::<DummyOutboxImpl>()
                .add::<DependencyGraphServiceImpl>()
                .add::<InMemoryDatasetDependencyRepository>()
                .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
                .add_value(TenancyConfig::SingleTenant)
                .add_builder(
                    odf::dataset::DatasetStorageUnitLocalFs::builder().with_root(datasets_dir),
                )
                .bind::<dyn odf::DatasetStorageUnit, odf::dataset::DatasetStorageUnitLocalFs>()
                .bind::<dyn odf::DatasetStorageUnitWriter, odf::dataset::DatasetStorageUnitLocalFs>(
                )
                .add::<CreateDatasetFromSnapshotUseCaseImpl>()
                .add::<CreateDatasetUseCaseImpl>()
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        let (_catalog_anonymous, catalog_authorized) = authentication_catalogs(&base_catalog).await;

        Self {
            _temp_dir: temp_dir,
            catalog_authorized,
        }
    }

    pub async fn create_datasets(
        &self,
        dataset_aliases: impl IntoIterator<Item = odf::DatasetAlias>,
    ) {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        for dataset_alias in dataset_aliases {
            create_dataset
                .execute(
                    MetadataFactory::dataset_snapshot()
                        .name(dataset_alias)
                        .kind(odf::DatasetKind::Root)
                        .push_event(MetadataFactory::set_polling_source().build())
                        .build(),
                    Default::default(),
                )
                .await
                .unwrap();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
