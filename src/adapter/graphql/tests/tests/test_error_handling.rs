// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use indoc::indoc;
use internal_error::ErrorIntoInternal;
use pretty_assertions::assert_eq;
use thiserror::Error;

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

    // Ignore error locations
    json_resp["errors"][0]["locations"] = serde_json::Value::Array(Vec::new());

    assert_eq!(
        json_resp,
        serde_json::json!({
            "errors":[{
                "locations": [],
                "message": "Failed to parse \"AccountName\": Value '????' is not a valid AccountName",
                "path": ["datasets", "byAccountName"],
            }],
            "data": {
                "datasets": null,
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_internal_error() {
    // NOTE: Service chosen to have the shortest DI catalog.
    #[dill::component]
    #[dill::interface(dyn kamu_accounts::AuthenticationService)]
    struct DummyAuthenticationService;

    #[async_trait::async_trait]
    impl kamu_accounts::AuthenticationService for DummyAuthenticationService {
        fn supported_login_methods(&self) -> Vec<&'static str> {
            unimplemented!()
        }

        async fn login(
            &self,
            _login_method: &str,
            _login_credentials_json: String,
            _device_code: Option<kamu_accounts::DeviceCode>,
        ) -> Result<kamu_accounts::LoginResponse, kamu_accounts::LoginError> {
            unimplemented!()
        }

        async fn account_by_token(
            &self,
            _access_token: String,
        ) -> Result<kamu_accounts::Account, kamu_accounts::GetAccountInfoError> {
            #[derive(Debug, Error)]
            #[error("I'm a dummy error that should not propagate through")]
            struct DummyError;

            Err(DummyError.int_err().into())
        }
    }

    let catalog = dill::CatalogBuilder::new()
        .add::<DummyAuthenticationService>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc!(
                r#"
                mutation ($accessToken: String!) {
                  auth {
                    accountDetails(accessToken: $accessToken) {
                      __typename
                    }
                  }
                }
                "#
            ))
            .variables(async_graphql::Variables::from_value(value!({
                "accessToken": "foobar",
            })))
            .data(catalog),
        )
        .await;

    let mut json_resp = serde_json::to_value(res).unwrap();

    // Ignore error locations
    json_resp["errors"][0]["locations"] = serde_json::Value::Array(Vec::new());

    assert_eq!(
        json_resp,
        serde_json::json!({
            "errors":[{
                "locations": [],
                "message": "Internal error",
                "path": ["auth", "accountDetails"],
            }],
            "data": {
                "auth": null,
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
