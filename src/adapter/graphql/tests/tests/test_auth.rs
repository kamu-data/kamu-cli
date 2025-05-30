// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::value;
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::Component;
use indoc::indoc;
use kamu_accounts::testing::{DUMMY_LOGIN_METHOD, MockAuthenticationService};
use kamu_accounts::{
    AccessTokenLifecycleMessage,
    AuthenticationService,
    DEFAULT_ACCOUNT_ID,
    DEFAULT_ACCOUNT_NAME_STR,
    MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE,
};
use kamu_accounts_inmem::InMemoryAccessTokenRepository;
use kamu_accounts_services::AccessTokenServiceImpl;
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};
use time_source::SystemTimeSourceDefault;

use crate::utils::{PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_enabled_login_providers() {
    let mut mock_authentication_service = MockAuthenticationService::new();
    mock_authentication_service
        .expect_supported_login_methods()
        .return_once(|| vec![DUMMY_LOGIN_METHOD]);

    let harness = AuthGQLHarness::new(mock_authentication_service).await;

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
            .data(harness.catalog_base),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login() {
    let harness = AuthGQLHarness::new(MockAuthenticationService::built_in()).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(make_login_request().data(harness.catalog_base))
        .await;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login_bad_method() {
    let harness = AuthGQLHarness::new(MockAuthenticationService::unsupported_login_method()).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(make_login_request().data(harness.catalog_base))
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(
        res.errors[0].message,
        format!("Unsupported login method '{DUMMY_LOGIN_METHOD}'")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_details() {
    let harness = AuthGQLHarness::new(MockAuthenticationService::built_in()).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(make_account_details_request().data(harness.catalog_base))
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_details_expired_token() {
    let harness = AuthGQLHarness::new(MockAuthenticationService::expired_token()).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(make_account_details_request().data(harness.catalog_base))
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Access token error".to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_and_get_access_token() {
    let harness = AuthGQLHarness::new(MockAuthenticationService::expired_token()).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let mutation_code = AuthGQLHarness::create_access_token(&DEFAULT_ACCOUNT_ID.to_string(), "foo");

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok());

    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();

    let created_token_id = json["auth"]["createAccessToken"]["token"]["id"].clone();

    let query_code = AuthGQLHarness::get_access_tokens(&DEFAULT_ACCOUNT_ID.to_string());
    let res = schema
        .execute(
            async_graphql::Request::new(query_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert_eq!(
        res.data,
        value!({
            "auth": {
                "listAccessTokens": {
                    "nodes": [{
                        "id": created_token_id,
                        "name": "foo",
                        "revokedAt": null
                    }]
                }
            }
        })
    );

    let mutation_code = AuthGQLHarness::create_access_token(&DEFAULT_ACCOUNT_ID.to_string(), "bar");

    let res = schema
        .execute(async_graphql::Request::new(mutation_code.clone()).data(harness.catalog_anonymous))
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Account access error".to_string());

    let mutation_code = AuthGQLHarness::create_access_token(&DEFAULT_ACCOUNT_ID.to_string(), "foo");

    let res = schema
        .execute(async_graphql::Request::new(mutation_code).data(harness.catalog_authorized))
        .await;

    assert_eq!(
        res.data,
        value!({
            "auth": {
                "createAccessToken": {
                    "__typename": "CreateAccessTokenResultDuplicate",
                    "message": "Access token with foo name already exists"
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_revoke_access_token() {
    let harness = AuthGQLHarness::new(MockAuthenticationService::expired_token()).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let mutation_code = AuthGQLHarness::create_access_token(&DEFAULT_ACCOUNT_ID.to_string(), "foo");

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok());

    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();

    let created_token_id = json["auth"]["createAccessToken"]["token"]["id"].clone();

    let mutation_code = AuthGQLHarness::revoke_access_token(&created_token_id.to_string());

    let res = schema
        .execute(async_graphql::Request::new(mutation_code.clone()).data(harness.catalog_anonymous))
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(
        res.errors[0].message,
        "Access token access error".to_string()
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "auth": {
                "revokeAccessToken": {
                    "__typename": "RevokeResultSuccess",
                    "message": "Access token revoked successfully"
                }
            }
        })
    );

    let res = schema
        .execute(
            async_graphql::Request::new(mutation_code.clone())
                .data(harness.catalog_authorized.clone()),
        )
        .await;

    assert!(res.is_ok());
    assert_eq!(
        res.data,
        value!({
            "auth": {
                "revokeAccessToken": {
                    "__typename": "RevokeResultAlreadyRevoked",
                }
            }
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AuthGQLHarness {
    catalog_anonymous: dill::Catalog,
    catalog_authorized: dill::Catalog,
    catalog_base: dill::Catalog,
}

impl AuthGQLHarness {
    async fn new(mock_authentication_service: MockAuthenticationService) -> Self {
        let catalog_base = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(mock_authentication_service)
                .bind::<dyn AuthenticationService, MockAuthenticationService>()
                .add::<SystemTimeSourceDefault>()
                .add::<AccessTokenServiceImpl>()
                .add::<InMemoryAccessTokenRepository>()
                .add_builder(
                    messaging_outbox::OutboxImmediateImpl::builder()
                        .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add::<DatabaseTransactionRunner>();

            NoOpDatabasePlugin::init_database_components(&mut b);
            register_message_dispatcher::<AccessTokenLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE,
            );

            b.build()
        };

        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&catalog_base, PredefinedAccountOpts::default()).await;

        Self {
            catalog_anonymous,
            catalog_authorized,
            catalog_base,
        }
    }

    fn create_access_token(account_id: &str, token_name: &str) -> String {
        indoc!(
            r#"
            mutation {
                auth {
                    createAccessToken (accountId: "<account_id>", tokenName: "<token_name>") {
                        __typename
                        message
                        ... on CreateAccessTokenResultSuccess {
                            token {
                                id,
                                name,
                                composed
                            }
                        }
                    }
                }
            }
            "#
        )
        .replace("<account_id>", account_id)
        .replace("<token_name>", token_name)
    }

    fn revoke_access_token(token_id: &str) -> String {
        indoc!(
            r#"
            mutation {
                auth {
                    revokeAccessToken (tokenId: <token_id>) {
                        __typename
                        ... on RevokeResultSuccess {
                            message
                        }
                    }
                }
            }
            "#
        )
        .replace("<token_id>", token_id)
    }

    fn get_access_tokens(account_id: &str) -> String {
        indoc!(
            r#"
            query {
                auth {
                    listAccessTokens (accountId: "<account_id>", perPage: 10, page: 0) {
                        nodes {
                            id,
                            name,
                            revokedAt
                        }
                    }
                }
            }
            "#
        )
        .replace("<account_id>", account_id)
    }
}
