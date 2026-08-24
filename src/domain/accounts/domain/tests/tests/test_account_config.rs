// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;
use kamu_accounts::{
    AccountConfig,
    AccountConfigValidationError,
    AccountProvider,
    AccountType,
    Password,
};
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_password_without_id_or_key_ok() {
    let config = test_config();

    assert_matches!(config.validate(), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_matching_id_and_private_key_ok() {
    let (key, id) = odf::AccountID::new_generated_ed25519();

    let mut config = test_config();
    config.id = Some(id);
    config.private_key = Some(key.into());

    assert_matches!(config.validate(), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_mismatched_id_and_private_key_err() {
    let (key, _) = odf::AccountID::new_generated_ed25519();
    let (_, other_id) = odf::AccountID::new_generated_ed25519();

    let mut config = test_config();
    config.id = Some(other_id);
    config.private_key = Some(key.into());

    assert_matches!(
        config.validate(),
        Err(AccountConfigValidationError::IdMismatch { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Perhaps what we need isn't tricky hack tests,
//       but a more strict type
#[test]
fn test_validate_invalid_email_err() {
    let mut config = test_config();
    config.email = serde_json::from_str("\"not-an-email\"").unwrap();

    assert_matches!(
        config.validate(),
        Err(AccountConfigValidationError::InvalidEmail { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_invalid_provider_err() {
    let mut config = test_config();
    config.provider = "unknown".to_string();

    assert_matches!(
        config.validate(),
        Err(AccountConfigValidationError::InvalidProvider { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_oauth_github_with_private_key_err() {
    let (key, _) = odf::AccountID::new_generated_ed25519();

    let mut config = test_config();
    config.provider = AccountProvider::OAuthGitHub.to_string();
    config.private_key = Some(key.into());

    assert_matches!(
        config.validate(),
        Err(AccountConfigValidationError::PrivateKeyNotAllowed { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_web3_wallet_with_private_key_err() {
    let (key, _) = odf::AccountID::new_generated_ed25519();

    let mut config = test_config();
    config.provider = AccountProvider::Web3Wallet.to_string();
    config.private_key = Some(key.into());

    assert_matches!(
        config.validate(),
        Err(AccountConfigValidationError::PrivateKeyNotAllowed { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_oauth_github_with_pkh_id_err() {
    let mut config = test_config();
    config.provider = AccountProvider::OAuthGitHub.to_string();
    config.id = Some(sample_pkh_id());

    assert_matches!(
        config.validate(),
        Err(AccountConfigValidationError::ExpectedDidOdf { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_oauth_github_with_odf_id_ok() {
    let (_, id) = odf::AccountID::new_generated_ed25519();

    let mut config = test_config();
    config.provider = AccountProvider::OAuthGitHub.to_string();
    config.id = Some(id);

    assert_matches!(config.validate(), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_web3_wallet_with_odf_id_err() {
    let (_, id) = odf::AccountID::new_generated_ed25519();

    let mut config = test_config();
    config.provider = AccountProvider::Web3Wallet.to_string();
    config.id = Some(id);

    assert_matches!(
        config.validate(),
        Err(AccountConfigValidationError::ExpectedDidPkh { .. })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_validate_web3_wallet_with_pkh_id_ok() {
    let mut config = test_config();
    config.provider = AccountProvider::Web3Wallet.to_string();
    config.id = Some(sample_pkh_id());

    assert_matches!(config.validate(), Ok(_));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn test_config() -> AccountConfig {
    let account_name = odf::AccountName::new_unchecked("alice");
    let password = Password::try_new("test-password-0123".to_string()).unwrap();
    let email = Email::parse(&format!("{account_name}@example.com")).unwrap();

    AccountConfig {
        id: None,
        private_key: None,
        account_name,
        password,
        email,
        display_name: None,
        account_type: AccountType::User,
        provider: AccountProvider::Password.to_string(),
        provider_identity_key: None,
        avatar_url: None,
        registered_at: None,
        properties: Vec::new(),
        treat_datasets_as_public: false,
    }
}

fn sample_pkh_id() -> odf::AccountID {
    odf::AccountID::parse_caip10_account_id("eip155:1:0xb9c5714089478a327f09197987f16f9e5d936e8a")
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
