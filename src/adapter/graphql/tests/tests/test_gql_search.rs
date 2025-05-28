// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::*;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

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
async fn test_search_correct_dataset_order_in_response() {
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

#[tokio::test]
async fn test_name_lookup_accounts() {
    let harness = GqlSearchHarness::new().await;

    pretty_assertions::assert_eq!(
        harness
            .name_lookup_anonymous(
                "kA",
                value!({
                    "byAccount": {
                        "excludeAccountsByIds": [],
                    },
                })
            )
            .await
            .data,
        value!({
            "search": {
                "nameLookup": {
                    "nodes": [
                        {
                            "__typename": "Account",
                            "accountName": "kamu",
                        }
                    ],
                    "totalCount": 1,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    }
                }
            }
        }),
    );
}

#[tokio::test]
async fn test_name_lookup_accounts_with_excluding() {
    let harness = GqlSearchHarness::new().await;

    let authorized_current_account_subject = harness
        .catalog_authorized
        .get_one::<CurrentAccountSubject>()
        .unwrap();
    let authorized_account_id = authorized_current_account_subject.account_id().to_string();

    pretty_assertions::assert_eq!(
        harness
            .name_lookup_anonymous(
                "kA",
                value!({
                    "byAccount": {
                        "excludeAccountsByIds": [authorized_account_id]
                    }
                })
            )
            .await
            .data,
        value!({
            "search": {
                "nameLookup": {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct GqlSearchHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
    schema: kamu_adapter_graphql::Schema,
}

impl GqlSearchHarness {
    pub async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::SingleTenant)
            .build();

        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(base_gql_harness.catalog(), PredefinedAccountOpts::default())
                .await;
        let schema = kamu_adapter_graphql::schema_quiet();

        Self {
            base_gql_harness,
            catalog_anonymous,
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

    pub async fn execute_anonymous(&self, request: &str) -> Response {
        let res = self
            .schema
            .execute(Request::new(request).data(self.catalog_anonymous.clone()))
            .await;

        assert!(res.is_ok(), "{res:?}");

        res
    }

    pub async fn execute_authorized(&self, request: &str) -> Response {
        let res = self
            .schema
            .execute(Request::new(request).data(self.catalog_authorized.clone()))
            .await;

        assert!(res.is_ok(), "{res:?}");

        res
    }

    pub async fn search_authorized(&self, query: &str, options: SearchOptions) -> Response {
        use std::fmt::Write;

        let extra_arguments = {
            let mut s = String::new();
            if let Some(value) = options.page {
                write!(&mut s, ", page: {value}").unwrap();
            }
            if let Some(value) = options.per_page {
                write!(&mut s, ", perPage: {value}").unwrap();
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

    pub async fn name_lookup_anonymous(
        &self,
        query: &str,
        filters: async_graphql::Value,
    ) -> Response {
        self.execute_anonymous(
            &indoc::indoc!(
                "
                {
                  search {
                    nameLookup(
                      query: \"<query>\",
                      filters: <filters>
                    ) {
                      nodes {
                        __typename
                        ... on Account {
                          accountName
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
            .replace("<filters>", &format!("{filters}")),
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
