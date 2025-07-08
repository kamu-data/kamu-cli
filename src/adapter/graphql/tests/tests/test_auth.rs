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
use kamu_accounts::testing::{DUMMY_LOGIN_METHOD, MockAuthenticationService};
use kamu_accounts::{AccountProvider, AuthenticationService, DEFAULT_ACCOUNT_NAME_STR};
use kamu_accounts_inmem::InMemoryDidSecretKeyRepository;
use messaging_outbox::{Outbox, OutboxImmediateImpl};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_login_request() -> async_graphql::Request {
    async_graphql::Request::new(indoc::indoc!(
        r#"
        mutation {
            auth {
                login (loginMethod: OAUTH_GITHUB, loginCredentialsJson: "dummy") {
                    accessToken
                    account {
                        accountName
                    }
                }
            }
        }
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
        .return_once(|| vec![AccountProvider::OAuthGitHub.into()]);

    let harness = AuthGQLHarness::new(mock_authentication_service);
    let schema = kamu_adapter_graphql::schema_quiet();

    let res = schema
        .execute(
            async_graphql::Request::new(
                r#"
                query {
                    auth {
                        enabledProviders
                    }
                }
                "#,
            )
            .data(harness.catalog_base),
        )
        .await;

    pretty_assertions::assert_eq!(
        value!({
            "auth": {
                "enabledProviders": [
                    async_graphql::Name::new("OAUTH_GITHUB")
                ]
            }
        }),
        res.data,
        "{res:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login() {
    let harness = AuthGQLHarness::new(MockAuthenticationService::built_in());

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
    let harness = AuthGQLHarness::new(MockAuthenticationService::unsupported_login_method());

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
    let harness = AuthGQLHarness::new(MockAuthenticationService::built_in());

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
    let harness = AuthGQLHarness::new(MockAuthenticationService::expired_token());

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(make_account_details_request().data(harness.catalog_base))
        .await;

    assert!(res.is_err());
    assert_eq!(res.errors.len(), 1);
    assert_eq!(res.errors[0].message, "Access token error".to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AuthGQLHarness {
    catalog_base: dill::Catalog,
}

impl AuthGQLHarness {
    fn new(mock_authentication_service: MockAuthenticationService) -> Self {
        let catalog_base = {
            let mut b = dill::CatalogBuilder::new();

            b.add_value(mock_authentication_service)
                .bind::<dyn AuthenticationService, MockAuthenticationService>()
                .add::<SystemTimeSourceDefault>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add_builder(
                    messaging_outbox::OutboxImmediateImpl::builder()
                        .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add::<DatabaseTransactionRunner>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        Self { catalog_base }
    }
}
