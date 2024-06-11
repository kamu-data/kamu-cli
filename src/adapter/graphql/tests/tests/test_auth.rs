// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use kamu_accounts::{
    AuthenticationService,
    MockAuthenticationService,
    DEFAULT_ACCOUNT_NAME_STR,
    DUMMY_LOGIN_METHOD,
};

////////////////////////////////////////////////////////////////////////////////////////

fn make_login_request() -> async_graphql::Request {
    async_graphql::Request::new(format!(
        r#"
        mutation {{
            auth {{
                login (loginMethod: "{DUMMY_LOGIN_METHOD}", loginCredentialsJson: "dummy") {{
                    accessToken
                    account {{
                        accountName
                    }}
                }}
            }}
        }}
        "#,
    ))
}

////////////////////////////////////////////////////////////////////////////////////////

fn make_account_details_request() -> async_graphql::Request {
    async_graphql::Request::new(format!(
        r#"
        mutation {{
            auth {{
                accountDetails (accessToken: "{}") {{
                    accountName
                }}
            }}
        }}
        "#,
        kamu_accounts::DUMMY_ACCESS_TOKEN,
    ))
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_enabled_login_methods() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_supported_login_methods()
        .return_once(|| vec![DUMMY_LOGIN_METHOD]);

    let cat = dill::CatalogBuilder::new()
        .add_value(mock_authentication_service)
        .bind::<dyn AuthenticationService, MockAuthenticationService>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(
                r#"
                query {
                    auth {
                        enabledLoginMethods
                    }
                }
                "#,
            )
            .data(cat),
        )
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "auth": {
                "enabledLoginMethods": [ DUMMY_LOGIN_METHOD ]
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login() {
    let cat = dill::CatalogBuilder::new()
        .add_value(MockAuthenticationService::built_in())
        .bind::<dyn AuthenticationService, MockAuthenticationService>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema.execute(make_login_request().data(cat)).await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "auth": {
                "login": {
                    "accessToken": kamu_accounts::DUMMY_ACCESS_TOKEN,
                    "account": {
                        "accountName": DEFAULT_ACCOUNT_NAME_STR
                    }
                },
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login_bad_method() {
    let cat = dill::CatalogBuilder::new()
        .add_value(MockAuthenticationService::unsupported_login_method())
        .bind::<dyn AuthenticationService, MockAuthenticationService>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema.execute(make_login_request().data(cat)).await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(
        res.errors[0].message,
        format!("Unsupported login method '{DUMMY_LOGIN_METHOD}'")
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_details() {
    let cat = dill::CatalogBuilder::new()
        .add_value(MockAuthenticationService::built_in())
        .bind::<dyn AuthenticationService, MockAuthenticationService>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(make_account_details_request().data(cat))
        .await;

    assert!(res.is_ok(), "{res:?}");
    assert_eq!(
        res.data,
        value!({
            "auth": {
                "accountDetails": {
                    "accountName": DEFAULT_ACCOUNT_NAME_STR
                },
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_details_expired_token() {
    let cat = dill::CatalogBuilder::new()
        .add_value(MockAuthenticationService::expired_token())
        .bind::<dyn AuthenticationService, MockAuthenticationService>()
        .build();

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(make_account_details_request().data(cat))
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Access token error".to_string());
}

////////////////////////////////////////////////////////////////////////////////////////
