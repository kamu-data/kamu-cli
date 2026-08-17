// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use email_utils::Email;
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
    CreateAccountUseCaseImpl,
    OAuthDeviceCodeGeneratorDefault,
    OAuthDeviceCodeServiceImpl,
};
use messaging_outbox::{MockOutbox, Outbox};
use pretty_assertions::{assert_eq, assert_matches};
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! dummy_auth_provider {
    ($letter:ident, $name:literal) => {
        paste::paste! {
            #[dill::component(pub)]
            #[dill::interface(dyn AuthenticationProvider)]
            struct [<DummyAuthenticationProvider $letter>] {}

            #[async_trait::async_trait]
            impl AuthenticationProvider for [<DummyAuthenticationProvider $letter>] {
                fn provider_name(&self) -> &'static str {
                    concat!("method_", stringify!([<$letter:lower>]))
                }

                async fn login(
                    &self,
                    _login_credentials_json: String,
                ) -> Result<ProviderLoginResponse, ProviderLoginError> {
                    let letter = stringify!([<$letter:lower>]);
                    Ok(ProviderLoginResponse {
                        account_id: None,
                        account_name: odf::AccountName::new_unchecked($name),
                        email: Email::parse(&format!("method-{letter}@example.com")).unwrap(),
                        display_name: String::from($name),
                        avatar_url: None,
                        provider_identity_key: format!("method-{letter}-identity"),
                    })
                }
            }
        }
    };
}

dummy_auth_provider!(A, "kamu");
dummy_auth_provider!(B, "kamu");
dummy_auth_provider!(C, "kamu-method-d");
dummy_auth_provider!(D, "kamu");

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_enabled_login_providers() {
    let harness = AuthenticationServiceHarness::builder().build();

    let mut supported_login_methods = harness.authentication_service().supported_login_methods();
    supported_login_methods.sort_unstable();

    assert_eq!(
        [
            DummyAuthenticationProviderA {}.provider_name(),
            DummyAuthenticationProviderB {}.provider_name(),
            DummyAuthenticationProviderC {}.provider_name(),
            DummyAuthenticationProviderD {}.provider_name(),
        ],
        *supported_login_methods
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_login_generate_busy_account_name() {
    let mut mock_outbox = MockOutbox::new();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut mock_outbox)
        .expected_times(4)
        .call();

    let harness = AuthenticationServiceHarness::builder()
        .mock_outbox(mock_outbox)
        .build();

    // 1. Used vacant account name
    assert_matches!(
         harness.authentication_service
            .login(DummyAuthenticationProviderA {}.provider_name(), "dummy".to_string(), None)
            .await,
        Ok(LoginResponse {
            ref account_name,
            ..
        }) if *account_name == odf::AccountName::new_unchecked("kamu")
    );
    assert_matches!(
        harness
            .authentication_service
            .login(
                DummyAuthenticationProviderA {}.provider_name(),
                "dummy".to_string(),
                None
            )
            .await,
        Err(LoginError::DuplicateCredentials)
    );

    // 2. Used account name + provider
    assert_matches!(
        harness.authentication_service
            .login(
                DummyAuthenticationProviderB {}.provider_name(),
                "dummy".to_string(),
                None
            )
            .await,
        Ok(LoginResponse {
            ref account_name,
            ..
        }) if *account_name == odf::AccountName::new_unchecked("kamu-method-b") );
    assert_matches!(
        harness
            .authentication_service
            .login(
                DummyAuthenticationProviderB {}.provider_name(),
                "dummy".to_string(),
                None
            )
            .await,
        Err(LoginError::DuplicateCredentials)
    );

    // 3. Used account name + provider + random letters

    // DummyAuthenticationProviderC takes the name (/w the provider one)
    // of DummyAuthenticationProviderD:
    assert_matches!(
        harness.authentication_service
            .login(
                DummyAuthenticationProviderC {}.provider_name(),
                "dummy".to_string(),
                None
            )
            .await,
        Ok(LoginResponse {
            ref account_name,
            ..
        }) if *account_name == odf::AccountName::new_unchecked("kamu-method-d")
    );
    assert_matches!(
        harness.authentication_service
            .login(
                DummyAuthenticationProviderD {}.provider_name(),
                "dummy".to_string(),
                None
            )
            .await,
        Ok(LoginResponse {
            ref account_name,
            ..
        }) if {
            // Account name isn't known yet, but the format is it
            let re = regex::Regex::new(r"^kamu-method-d-[A-Za-z0-9]{4}$").unwrap();
            re.is_match(account_name.as_str())
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_try_login_with_wrong_provider() {
    let harness = AuthenticationServiceHarness::builder().build();

    assert_matches!(
        harness
            .authentication_service
            .login("method-bad", "dummy".to_string(), None)
            .await,
        Err(LoginError::UnsupportedMethod(_))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_account_not_created_in_restrict_anonymous_mode() {
    let harness = AuthenticationServiceHarness::builder()
        .auth_config(AuthConfig {
            allow_anonymous: false,
            ..AuthConfig::default()
        })
        .build();

    assert_matches!(
        harness
            .authentication_service
            .login(
                DummyAuthenticationProviderA {}.provider_name(),
                "dummy".to_string(),
                None,
            )
            .await,
        Err(LoginError::RestrictedLogin)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_use_good_access_token() {
    let mut mock_outbox = MockOutbox::new();
    kamu_accounts::testing::expect_outbox_account_created()
        .mock_outbox(&mut mock_outbox)
        .expected_times(1)
        .call();

    let harness = AuthenticationServiceHarness::builder()
        .mock_outbox(mock_outbox)
        .build();

    let login_response = harness
        .authentication_service
        .login(
            DummyAuthenticationProviderA {}.provider_name(),
            "dummy".to_string(),
            None,
        )
        .await
        .unwrap();

    let resolved_account_info = harness
        .authentication_service
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
    let harness = AuthenticationServiceHarness::builder().build();

    assert_matches!(
        harness
            .authentication_service
            .account_by_token("bad-token".to_string())
            .await,
        Err(GetAccountInfoError::AccessToken(AccessTokenError::Invalid(
            _
        )))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AuthenticationServiceHarness {
    _catalog: dill::Catalog,
    authentication_service: Arc<dyn AuthenticationService>,
}

#[bon::bon]
impl AuthenticationServiceHarness {
    #[builder]
    pub fn new(mock_outbox: Option<MockOutbox>, auth_config: Option<AuthConfig>) -> Self {
        let mock_outbox = mock_outbox.unwrap_or_default();
        let auth_config = auth_config.unwrap_or_else(AuthConfig::sample);

        let mut b = dill::CatalogBuilder::new();

        b.add::<DummyAuthenticationProviderA>();
        b.add::<DummyAuthenticationProviderB>();
        b.add::<DummyAuthenticationProviderC>();
        b.add::<DummyAuthenticationProviderD>();
        b.add::<AuthenticationServiceImpl>();
        b.add::<CreateAccountUseCaseImpl>();
        b.add::<InMemoryAccountRepository>();
        b.add::<AccountServiceImpl>();
        b.add::<InMemoryDidSecretKeyRepository>();
        b.add::<AccessTokenServiceImpl>();
        b.add::<InMemoryAccessTokenRepository>();
        b.add_value(DidSecretEncryptionConfig::default());
        b.add_value(PredefinedAccountsConfig::single_tenant());
        b.add_value(SystemTimeSourceStub::new())
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
        b.add_value(JwtAuthenticationConfig::default());
        b.add::<DatabaseTransactionRunner>();
        b.add_value(mock_outbox).bind::<dyn Outbox, MockOutbox>();
        b.add::<OAuthDeviceCodeServiceImpl>();
        b.add::<OAuthDeviceCodeGeneratorDefault>();
        b.add_value(auth_config);
        b.add::<InMemoryOAuthDeviceCodeRepository>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        let catalog = b.build();

        Self {
            authentication_service: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }

    pub fn authentication_service(&self) -> &dyn AuthenticationService {
        self.authentication_service.as_ref()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
