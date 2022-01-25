// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

use kamu::domain::*;
use kamu::infra;
use kamu::testing::MetadataFactory;
use opendatafabric::*;

#[tokio::test]
async fn query() {
    let tempdir = tempfile::tempdir().unwrap();

    let workspace_layout = infra::WorkspaceLayout::create(tempdir.path()).unwrap();

    let cat = dill::CatalogBuilder::new()
        .add_value(workspace_layout)
        .add::<infra::DatasetRegistryImpl>()
        .bind::<dyn DatasetRegistry, infra::DatasetRegistryImpl>()
        .build();

    let dataset_reg = cat.get_one::<dyn DatasetRegistry>().unwrap();
    dataset_reg
        .add_dataset(
            MetadataFactory::dataset_snapshot()
                .name("foo")
                .kind(DatasetKind::Root)
                .push_event(MetadataFactory::set_polling_source().build())
                .build(),
        )
        .unwrap();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(
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
