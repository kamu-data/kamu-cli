// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{value, EmptyMutation, EmptySubscription, Object, Schema};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pagination() {
    let harness = PaginationHarness::new();

    harness
        .query(
            GenerateItemOptions {
                count: 0,
                page: None,
                per_page: None,
            },
            value!({
                "generateItems": {
                    "nodes": [],
                    "totalCount": 0,
                    "pageInfo": {
                        "totalPages": 0,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    },
                }
            }),
        )
        .await;

    harness
        .query(
            GenerateItemOptions {
                count: 2,
                page: None,
                per_page: None,
            },
            value!({
                "generateItems": {
                    "nodes": [
                        {
                            "index": 0,
                        },
                        {
                            "index": 1,
                        },
                    ],
                    "totalCount": 2,
                    "pageInfo": {
                        "totalPages": 1,
                        "hasNextPage": false,
                        "hasPreviousPage": false,
                    },
                }
            }),
        )
        .await;

    harness
        .query(
            GenerateItemOptions {
                count: 7,
                page: None,
                per_page: Some(5),
            },
            value!({
                "generateItems": {
                    "nodes": [
                        {
                            "index": 0,
                        },
                        {
                            "index": 1,
                        },
                        {
                            "index": 2,
                        },
                        {
                            "index": 3,
                        },
                        {
                            "index": 4,
                        },
                    ],
                    "totalCount": 7,
                    "pageInfo": {
                        "totalPages": 2,
                        "hasNextPage": true,
                        "hasPreviousPage": false,
                    },
                }
            }),
        )
        .await;

    harness
        .query(
            GenerateItemOptions {
                count: 7,
                page: Some(1),
                per_page: Some(5),
            },
            value!({
                "generateItems": {
                    "nodes": [
                        {
                            "index": 5,
                        },
                        {
                            "index": 6,
                        },
                    ],
                    "totalCount": 7,
                    "pageInfo": {
                        "totalPages": 2,
                        "hasNextPage": false,
                        "hasPreviousPage": true,
                    },
                }
            }),
        )
        .await;

    harness
        .query(
            GenerateItemOptions {
                count: 7,
                page: Some(2),
                per_page: Some(5),
            },
            value!({
                "generateItems": {
                    "nodes": [],
                    "totalCount": 7,
                    "pageInfo": {
                        "totalPages": 2,
                        "hasNextPage": false,
                        "hasPreviousPage": true,
                    },
                }
            }),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PaginationHarness {
    schema: Schema<TestRootQuery, EmptyMutation, EmptySubscription>,
}

impl PaginationHarness {
    fn new() -> Self {
        let schema = Schema::build(TestRootQuery, EmptyMutation, EmptySubscription).finish();

        Self { schema }
    }

    async fn query(
        &self,
        options: GenerateItemOptions,
        expected_response_data: async_graphql::Value,
    ) {
        let arguments = {
            let mut s = format!("count: {}", options.count);
            if let Some(value) = options.page {
                s += &format!(", page: {value}",);
            }
            if let Some(value) = options.per_page {
                s += &format!(", perPage: {value}");
            }
            s
        };

        let res = self
            .schema
            .execute(async_graphql::Request::new(indoc::formatdoc!(
                r#"
                query {{
                    generateItems({arguments}) {{
                        nodes {{
                            index
                        }}
                        totalCount
                        pageInfo {{
                            totalPages
                            hasNextPage
                            hasPreviousPage
                        }}
                    }}
                }}
                "#
            )))
            .await;

        assert!(res.is_ok(), "{res:?}");

        pretty_assertions::assert_eq!(expected_response_data, res.data);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct GenerateItemOptions {
    count: usize,
    page: Option<usize>,
    per_page: Option<usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Query
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct TestRootQuery;

#[Object]
impl TestRootQuery {
    const DEFAULT_PER_PAGE: usize = 15;

    async fn generate_items(
        &self,
        count: usize,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> ItemConnection {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let items = (0..count)
            .map(|i| Item::new(i))
            .skip(page * per_page)
            .take(per_page)
            .collect::<Vec<_>>();
        let total = count;

        ItemConnection::new(items, page, per_page, total)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct Item {
    index: usize,
}

#[Object]
impl Item {
    #[graphql(skip)]
    pub fn new(index: usize) -> Self {
        Self { index }
    }

    async fn index(&self) -> usize {
        self.index
    }
}

kamu_adapter_graphql::page_based_connection!(Item, ItemConnection, ItemEdge);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
