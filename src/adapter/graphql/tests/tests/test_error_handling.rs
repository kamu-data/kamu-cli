// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{component, interface, Component};
use indoc::indoc;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu::DatasetRegistrySoloUnitBridge;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::AlwaysHappyDatasetActionAuthorizer;
use kamu_core::TenancyConfig;
use kamu_datasets::{ViewDatasetUseCase, ViewDatasetUseCaseError, ViewMultiResponse};
use thiserror::Error;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_internal_error() {
    #[component]
    #[interface(dyn ViewDatasetUseCase)]
    struct ErrorViewDatasetUseCaseImpl {}

    #[async_trait::async_trait]
    impl ViewDatasetUseCase for ErrorViewDatasetUseCaseImpl {
        async fn execute(
            &self,
            _: &odf::DatasetRef,
        ) -> Result<odf::DatasetHandle, ViewDatasetUseCaseError> {
            #[derive(Debug, Error)]
            #[error("I'm a dummy error that should not propagate through")]
            struct DummyError {}

            Err(ViewDatasetUseCaseError::Internal((DummyError {}).int_err()))
        }

        async fn execute_multi(
            &self,
            _: Vec<odf::DatasetRef>,
        ) -> Result<ViewMultiResponse, InternalError> {
            unimplemented!()
        }
    }

    let tempdir = tempfile::tempdir().unwrap();

    let cat = dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(
            odf::dataset::DatasetStorageUnitLocalFs::builder()
                .with_root(tempdir.path().join("datasets")),
        )
        .bind::<dyn odf::DatasetStorageUnit, odf::dataset::DatasetStorageUnitLocalFs>()
        .add::<DatasetRegistrySoloUnitBridge>()
        .add::<ErrorViewDatasetUseCaseImpl>()
        .add::<AlwaysHappyDatasetActionAuthorizer>()
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
