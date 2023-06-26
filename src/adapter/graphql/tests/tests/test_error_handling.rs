// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use indoc::indoc;
use kamu::*;
use kamu_core::*;
use opendatafabric::AccountName;

#[test_log::test(tokio::test)]
async fn test_malformed_argument() {
    let schema = kamu_adapter_graphql::schema(dill::CatalogBuilder::new().build());
    let res = schema
        .execute(indoc!(
            r#"
            {
                datasets {
                    byAccountName (accountName: "????") {
                        nodes { id }
                    }
                }
            }
            "#
        ))
        .await;

    let mut json_resp = serde_json::to_value(res).unwrap();

    // Ingore extensions and error locations
    json_resp["extensions"] = serde_json::Value::Null;
    json_resp["errors"][0]["locations"] = serde_json::Value::Array(Vec::new());

    assert_eq!(
        json_resp,
        serde_json::json!({
            "errors":[{
                "locations": [],
                "message": "Failed to parse \"AccountName\": Invalid AccountName: ????",
                "path": ["datasets", "byAccountName"],
            }],
            "data": null,
            "extensions": null,
        })
    );
}

#[test_log::test(tokio::test)]
async fn test_internal_error() {
    let tempdir = tempfile::tempdir().unwrap();

    // Note: Not creating a workspace to cause an error
    let workspace_layout = Arc::new(WorkspaceLayout::new(tempdir.path()));
    let local_repo = DatasetRepositoryLocalFs::new(
        workspace_layout.datasets_dir.clone(),
        AccountName::new_unchecked(DEFAULT_DATASET_OWNER_NAME),
        false,
    );

    let cat = dill::CatalogBuilder::new()
        .add_value(local_repo)
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .build();

    let schema = kamu_adapter_graphql::schema(cat);
    let res = schema
        .execute(indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "did:odf:z4k88e8n8Je6fC9Lz9FHrZ7XGsikEyBwTwtMBzxp4RH9pbWn4UM") {
                        name
                    }
                }
            }
            "#
        ))
        .await;

    let mut json_resp = serde_json::to_value(res).unwrap();

    // Ingore extensions and error locations
    json_resp["extensions"] = serde_json::Value::Null;
    json_resp["errors"][0]["locations"] = serde_json::Value::Array(Vec::new());

    assert_eq!(
        json_resp,
        serde_json::json!({
            "errors":[{
                "locations": [],
                "message": "Internal error",
                "path": ["datasets", "byId"],
            }],
            "data": null,
            "extensions": null,
        })
    );
}

#[test_log::test(tokio::test)]
#[should_panic]
async fn test_handler_panics() {
    // Not expecting panic to be trapped - that's the job of an HTTP server
    let schema = kamu_adapter_graphql::schema(dill::CatalogBuilder::new().build());

    schema
        .execute(indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "did:odf:z4k88e8n8Je6fC9Lz9FHrZ7XGsikEyBwTwtMBzxp4RH9pbWn4UM") {
                        name
                    }
                }
            }
            "#
        ))
        .await;
}
