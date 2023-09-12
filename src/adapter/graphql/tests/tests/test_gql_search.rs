// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::*;
use kamu::testing::MetadataFactory;
use kamu::*;
use kamu_core::*;
use opendatafabric::*;

#[tokio::test]
async fn query() {
    let tempdir = tempfile::tempdir().unwrap();
    let dataset_repo = DatasetRepositoryLocalFs::create(
        tempdir.path().join("datasets"),
        Arc::new(CurrentAccountSubject::new_test()),
        Arc::new(auth::AlwaysHappyDatasetActionAuthorizer::new()),
        false,
    )
    .unwrap();

    let cat = dill::CatalogBuilder::new()
        .add_value(dataset_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let dataset_repo = cat.get_one::<dyn DatasetRepository>().unwrap();
    dataset_repo
        .create_dataset_from_snapshot(
            None,
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .await
        .unwrap();

    let schema = kamu_adapter_graphql::schema();
    let res = schema
        .execute(
            async_graphql::Request::new(
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
            .data(cat.clone()),
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
            async_graphql::Request::new(
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
            .data(cat),
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
}
