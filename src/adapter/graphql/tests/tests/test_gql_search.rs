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
async fn test_single_dataset_search() {
    let harness = GqlSearchHarness::new().await;

    use odf::metadata::testing::alias;

    harness.create_datasets([alias(&"kamu", &"foo")]).await;

    pretty_assertions::assert_eq!(
        harness
            .search_authorized("bar", SearchOptions::default())
            .await
            .data,
        value!({
            "search": {
                "query": {
                    "nodes": [],
                    "totalCount": 0,
                    "pageInfo": {
                        "totalPages": 0,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );
    pretty_assertions::assert_eq!(
        harness
            .search_authorized("foo", SearchOptions::default())
            .await
            .data,
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "__typename": "Dataset",
                        "name": "foo",
                    }],
                    "totalCount": 1,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        })
    );
    pretty_assertions::assert_eq!(
        harness
            .execute_authorized(indoc::indoc!(
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
                "
            ))
            .await
            .data,
        value!({
            "search": {
                "query": {
                    "nodes": [{
                        "metadata": {
                          "chain": {
                            "blocks": {
                              "totalCount": 2,
                              "pageInfo": {
                                "totalPages": 0,
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

#[tokio::test]
async fn test_correct_dataset_order_in_response() {
    let harness = GqlSearchHarness::new().await;

    use odf::metadata::testing::alias;

    harness
        .create_datasets(
            // NOTE: purposely random order
            [
                alias(&"kamu", &"dataset5"),
                alias(&"kamu", &"dataset3"),
                alias(&"kamu", &"dataset4"),
                alias(&"kamu", &"dataset1"),
                alias(&"kamu", &"dataset2"),
            ],
        )
        .await;

    // Run queries multiple times to ensure the search results stability
    for _ in 0..5 {
        // Search without pagination
        pretty_assertions::assert_eq!(
            harness
                .search_authorized("dataset", SearchOptions::default())
                .await
                .data,
            value!({
                "search": {
                    "query": {
                        "nodes": [
                            {
                                "__typename": "Dataset",
                                "name": "dataset1",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset2",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset3",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset4",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset5",
                            },
                        ],
                        "totalCount": 5,
                        "pageInfo": {
                            "totalPages": 1,
                            "hasNextPage": false,
                            "hasPreviousPage": false,
                        }
                    }
                }
            })
        );

        // Checking with pagination
        pretty_assertions::assert_eq!(
            harness
                .search_authorized(
                    "dataset",
                    SearchOptions {
                        page: Some(0),
                        per_page: Some(3),
                    }
                )
                .await
                .data,
            value!({
                "search": {
                    "query": {
                        "nodes": [
                            {
                                "__typename": "Dataset",
                                "name": "dataset1",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset2",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset3",
                            },
                        ],
                        "totalCount": 5,
                        "pageInfo": {
                            "totalPages": 2,
                            "hasNextPage": true,
                            "hasPreviousPage": false,
                        }
                    }
                }
            })
        );
        pretty_assertions::assert_eq!(
            harness
                .search_authorized(
                    "dataset",
                    SearchOptions {
                        page: Some(1),
                        per_page: Some(3),
                    }
                )
                .await
                .data,
            value!({
                "search": {
                    "query": {
                        "nodes": [
                            {
                                "__typename": "Dataset",
                                "name": "dataset4",
                            },
                            {
                                "__typename": "Dataset",
                                "name": "dataset5",
                            },
                        ],
                        "totalCount": 5,
                        "pageInfo": {
                            "totalPages": 2,
                            "hasNextPage": false,
                            "hasPreviousPage": true,
                        }
                    }
                }
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GqlSearchHarness {
    _temp_dir: tempfile::TempDir,
    catalog_authorized: dill::Catalog,
    schema: kamu_adapter_graphql::Schema,
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
        let schema = kamu_adapter_graphql::schema_quiet();

        Self {
            _temp_dir: temp_dir,
            catalog_authorized,
            schema,
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

    pub async fn execute_authorized(&self, request: &str) -> Response {
        let res = self
            .schema
            .execute(Request::new(request).data(self.catalog_authorized.clone()))
            .await;

        assert!(res.is_ok());

        res
    }

    pub async fn search_authorized(&self, query: &str, options: SearchOptions) -> Response {
        let extra_arguments = {
            let mut s = String::new();
            if let Some(value) = options.page {
                s += &format!(", page: {value}",);
            }
            if let Some(value) = options.per_page {
                s += &format!(", perPage: {value}");
            }
            s
        };

        self.execute_authorized(
            &indoc::indoc!(
                "
                {
                  search {
                    query(query: \"<query>\" <extra_arguments>) {
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
                "
            )
            .replace("<query>", query)
            .replace("<extra_arguments>", &extra_arguments),
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct SearchOptions {
    page: Option<usize>,
    per_page: Option<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
