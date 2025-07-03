// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use kamu_accounts::*;
use kamu_accounts_inmem::{
    InMemoryAccessTokenRepository,
    InMemoryAccountRepository,
    InMemoryDidSecretKeyRepository,
    InMemoryOAuthDeviceCodeRepository,
};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AccountServiceImpl,
    AuthenticationServiceImpl,
    OAuthDeviceCodeGeneratorDefault,
    OAuthDeviceCodeServiceImpl,
};
use messaging_outbox::{MockOutbox, Outbox};
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_enabled_login_providers() {
    let catalog = make_catalog(MockOutbox::new());
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    let mut supported_login_methods = authentication_service.supported_login_methods();
    supported_login_methods.sort_unstable();
    assert_eq!(supported_login_methods, vec!["method-A", "method-B"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login() {
    let mut mock_outbox = MockOutbox::new();
    expect_outbox_account_created(&mut mock_outbox);

    let catalog = make_catalog(mock_outbox);
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    let response_a = authentication_service
        .login("method-A", "dummy".to_string(), None)
        .await;

    let response_b = authentication_service
        .login("method-B", "dummy".to_string(), None)
        .await;

    let response_bad = authentication_service
        .login("method-bad", "dummy".to_string(), None)
        .await;

    assert_matches!(response_a, Ok(_));
    assert_matches!(response_b, Ok(_));
    assert_matches!(response_bad, Err(LoginError::UnsupportedMethod(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_use_good_access_token() {
    let mut mock_outbox = MockOutbox::new();
    expect_outbox_account_created(&mut mock_outbox);

    let catalog = make_catalog(mock_outbox);
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    let login_response = authentication_service
        .login("method-A", "dummy".to_string(), None)
        .await
        .unwrap();

    let resolved_account_info = authentication_service
        .account_by_token(login_response.access_token)
        .await
        .unwrap();
    assert_eq!(login_response.account_id, resolved_account_info.id);
    assert_eq!(
        login_response.account_name,
        resolved_account_info.account_name
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_use_bad_access_token() {
    let catalog = make_catalog(MockOutbox::new());
    let authentication_service = catalog.get_one::<dyn AuthenticationService>().unwrap();

    assert_matches!(
        authentication_service
            .account_by_token("bad-token".to_string())
            .await,
        Err(GetAccountInfoError::AccessToken(AccessTokenError::Invalid(
            _
        )))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_catalog(mock_outbox: MockOutbox) -> dill::Catalog {
    let mut b = dill::CatalogBuilder::new();

    b.add::<DummyAuthenticationProviderA>()
        .add::<DummyAuthenticationProviderB>()
        .add::<AuthenticationServiceImpl>()
        .add::<InMemoryAccountRepository>()
        .add::<AccountServiceImpl>()
        .add::<InMemoryDidSecretKeyRepository>()
        .add::<AccessTokenServiceImpl>()
        .add::<InMemoryAccessTokenRepository>()
        .add_value(DidSecretEncryptionConfig::default())
        .add_value(PredefinedAccountsConfig::single_tenant())
        .add_value(SystemTimeSourceStub::new())
        .bind::<dyn SystemTimeSource, SystemTimeSourceStub>()
        .add_value(JwtAuthenticationConfig::default())
        .add::<DatabaseTransactionRunner>()
        .add_value(mock_outbox)
        .bind::<dyn Outbox, MockOutbox>()
        .add::<OAuthDeviceCodeServiceImpl>()
        .add::<OAuthDeviceCodeGeneratorDefault>()
        .add_value(AuthConfig::sample())
        .add::<InMemoryOAuthDeviceCodeRepository>();

    NoOpDatabasePlugin::init_database_components(&mut b);

    b.build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn expect_outbox_account_created(mock_outbox: &mut MockOutbox) {
    use mockall::predicate::{always, eq, function};
    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE),
            function(|message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<AccountLifecycleMessage>(message_as_json.clone()),
                    Ok(AccountLifecycleMessage::Created(_))
                )
            }),
            always(),
        )
        .returning(|_, _, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DummyAuthenticationProviderA {}
struct DummyAuthenticationProviderB {}

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
impl DummyAuthenticationProviderA {
    fn new() -> Self {
        Self {}
    }
}

#[dill::component(pub)]
#[dill::interface(dyn AuthenticationProvider)]
impl DummyAuthenticationProviderB {
    fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl AuthenticationProvider for DummyAuthenticationProviderA {
    fn provider_name(&self) -> &'static str {
        "method-A"
    }

    async fn login(
        &self,
        _login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        Ok(ProviderLoginResponse {
            account_id: DEFAULT_ACCOUNT_ID.clone(),
            account_name: DEFAULT_ACCOUNT_NAME.clone(),
            email: DUMMY_EMAIL_ADDRESS.clone(),
            display_name: String::from(DEFAULT_ACCOUNT_NAME_STR),
            account_type: AccountType::User,
            avatar_url: None,
            provider_identity_key: String::from(DEFAULT_ACCOUNT_NAME_STR),
        })
    }
}

#[async_trait::async_trait]
impl AuthenticationProvider for DummyAuthenticationProviderB {
    fn provider_name(&self) -> &'static str {
        "method-B"
    }

    async fn login(
        &self,
        _login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        Ok(ProviderLoginResponse {
            account_id: DEFAULT_ACCOUNT_ID.clone(),
            account_name: DEFAULT_ACCOUNT_NAME.clone(),
            email: DUMMY_EMAIL_ADDRESS.clone(),
            display_name: String::from(DEFAULT_ACCOUNT_NAME_STR),
            account_type: AccountType::User,
            avatar_url: None,
            provider_identity_key: String::from(DEFAULT_ACCOUNT_NAME_STR),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
