// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Component;
use indoc::indoc;
use kamu::{DatasetRegistryRepoBridge, DatasetRepositoryLocalFs};
use kamu_accounts::CurrentAccountSubject;
use kamu_core::DatasetRepository;
use time_source::SystemTimeSourceDefault;

#[test_log::test(tokio::test)]
async fn test_malformed_argument() {
    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc!(
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
            .data(dill::CatalogBuilder::new().build()),
        )
        .await;

    let mut json_resp = serde_json::to_value(res).unwrap();

    // Ignore extensions and error locations
    json_resp["extensions"] = serde_json::Value::Null;
    json_resp["errors"][0]["locations"] = serde_json::Value::Array(Vec::new());

    assert_eq!(
        json_resp,
        serde_json::json!({
            "errors":[{
                "locations": [],
                "message": "Failed to parse \"AccountName\": Value '????' is not a valid AccountName",
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

    let cat = dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add_value(CurrentAccountSubject::new_test())
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(tempdir.path().join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<DatasetRegistryRepoBridge>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema.execute(async_graphql::Request::new(indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc") {
                        name
                    }
                }
            }
            "#
        )).data(cat))
        .await;

    let mut json_resp = serde_json::to_value(res).unwrap();

    // Ignore extensions and error locations
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

// TODO: There seems to be an issue with libunwind in a version of XCode used on
// GitHub's M1 runners which results in error:
//   libunwind: stepWithCompactEncoding - invalid compact unwind encoding
#[cfg(not(target_os = "macos"))]
#[test_log::test(tokio::test)]
// We use the substring part because we have a dynamic panic message part.
#[should_panic(expected = "called `Result::unwrap()` on an `Err` value: \
                           Unregistered(UnregisteredTypeError { type_id: TypeId { t: ")]
async fn test_handler_panics() {
    // Not expecting panic to be trapped - that's the job of an HTTP server
    let schema = kamu_adapter_graphql::schema_quiet();
    schema.execute(async_graphql::Request::new(indoc!(
            r#"
            {
                datasets {
                    byId (datasetId: "did:odf:fed012126262ba49e1ba8392c26f7a39e1ba8d756c7469786d3365200c68402ff65dc") {
                        name
                    }
                }
            }
            "#
        )).data(dill::CatalogBuilder::new().build()))
        .await;
}
